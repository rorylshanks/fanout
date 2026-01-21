package parquet

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"
)

func TestParquetMetadataSizeTuning(t *testing.T) {
	sourcePath := filepath.Join("..", "..", "fanout.parquet")
	if _, err := os.Stat(sourcePath); err != nil {
		t.Skipf("source parquet file not found: %v", err)
	}

	baseOptions := []goparquet.WriterOption{
		goparquet.MaxRowsPerRowGroup(1000),
		goparquet.DataPageStatistics(false),
	}
	columnIndexLimit := goparquet.ColumnIndexSizeLimit(func(path []string) int { return 1 })
	skipBounds := columnsToSkipBounds(t, sourcePath)
	sizeBasedSkip, sizeBasedNames := sizeBasedSkipBoundsOptions(t, sourcePath, 1024)
	t.Logf("size-based skip columns=%d threshold=%d", len(sizeBasedNames), 1024)
	for _, name := range sizeBasedNames {
		t.Logf("size-based skip column=%s", name)
	}

	type combo struct {
		name    string
		options []goparquet.WriterOption
	}
	combos := []combo{
		{name: "base", options: baseOptions},
		{name: "base+column-index-limit", options: append(append([]goparquet.WriterOption{}, baseOptions...), columnIndexLimit)},
		{name: "base+skip-bounds", options: append(append([]goparquet.WriterOption{}, baseOptions...), skipBounds...)},
		{name: "base+size-based-skip", options: append(append([]goparquet.WriterOption{}, baseOptions...), sizeBasedSkip...)},
		{name: "base+column-index-limit+skip-bounds", options: append(append(append([]goparquet.WriterOption{}, baseOptions...), columnIndexLimit), skipBounds...)},
	}

	footerSizes := make(map[string]int, len(combos))
	for _, c := range combos {
		bytes := rewriteParquetWithOptions(t, sourcePath, c.options...)
		footerSizes[c.name] = parquetFooterSize(t, bytes)
		t.Logf("%s footer=%d", c.name, footerSizes[c.name])
	}

	baseFooter := footerSizes["base"]
	if baseFooter == 0 {
		t.Fatalf("base footer size missing")
	}
	for name, size := range footerSizes {
		if size > baseFooter {
			t.Fatalf("expected %s metadata <= base, got base=%d %s=%d", name, baseFooter, name, size)
		}
	}
}

func rewriteParquetWithOptions(t *testing.T, path string, options ...goparquet.WriterOption) []byte {
	t.Helper()

	src, err := os.Open(path)
	if err != nil {
		t.Fatalf("open source parquet: %v", err)
	}
	defer src.Close()

	reader := goparquet.NewReader(src)
	schema := reader.Schema()

	var buf bytes.Buffer
	opts := append([]goparquet.WriterOption{schema}, options...)
	writer := goparquet.NewWriter(&buf, opts...)

	rowBuf := make([]goparquet.Row, 256)
	for {
		n, err := reader.ReadRows(rowBuf)
		if n > 0 {
			if _, werr := writer.WriteRows(rowBuf[:n]); werr != nil {
				t.Fatalf("write rows: %v", werr)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read rows: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	return buf.Bytes()
}

type boundsSize struct {
	minLen int
	maxLen int
}

func columnIndexBoundsSizes(t *testing.T, data []byte) map[string]boundsSize {
	t.Helper()

	reader := bytes.NewReader(data)
	f, err := goparquet.OpenFile(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	rowGroups := f.RowGroups()
	if len(rowGroups) == 0 {
		t.Fatalf("expected at least one row group")
	}

	schemaCols := f.Schema().Columns()
	sizes := make(map[string]boundsSize, len(schemaCols))
	for _, chunk := range rowGroups[0].ColumnChunks() {
		idx := chunk.Column()
		if idx < 0 || idx >= len(schemaCols) {
			t.Fatalf("column index out of range: %d", idx)
		}
		name := strings.Join(schemaCols[idx], ".")
		ci, err := chunk.ColumnIndex()
		if err != nil {
			if err == goparquet.ErrMissingColumnIndex {
				sizes[name] = boundsSize{}
				continue
			}
			t.Fatalf("column index error: %v", err)
		}
		if ci.NumPages() == 0 {
			sizes[name] = boundsSize{}
			continue
		}
		minBytes := ci.MinValue(0).ByteArray()
		maxBytes := ci.MaxValue(0).ByteArray()
		sizes[name] = boundsSize{
			minLen: len(minBytes),
			maxLen: len(maxBytes),
		}
	}
	return sizes
}

func columnsToSkipBounds(t *testing.T, path string) []goparquet.WriterOption {
	t.Helper()

	src, err := os.Open(path)
	if err != nil {
		t.Fatalf("open source parquet: %v", err)
	}
	defer src.Close()

	reader := goparquet.NewReader(src)
	columns := reader.Schema().Columns()
	options := make([]goparquet.WriterOption, 0, len(columns))
	for _, column := range columns {
		options = append(options, goparquet.SkipPageBounds(column...))
	}
	return options
}

func sizeBasedSkipBoundsOptions(t *testing.T, path string, threshold int) ([]goparquet.WriterOption, []string) {
	t.Helper()

	if threshold <= 0 {
		return nil, nil
	}

	src, err := os.Open(path)
	if err != nil {
		t.Fatalf("open source parquet: %v", err)
	}
	defer src.Close()

	reader := goparquet.NewReader(src)
	defer reader.Close()

	schemaCols := reader.Schema().Columns()
	columnNames := make([]string, len(schemaCols))
	for i, path := range schemaCols {
		columnNames[i] = strings.Join(path, ".")
	}

	maxLen := make([]int, len(schemaCols))
	buf := make([]goparquet.Row, 64)
	for {
		n, err := reader.ReadRows(buf)
		for i := 0; i < n; i++ {
			row := buf[i]
			for colIdx := range columnNames {
				val := row[colIdx]
				if val.IsNull() {
					continue
				}
				switch val.Kind() {
				case goparquet.ByteArray, goparquet.FixedLenByteArray:
					if l := len(val.ByteArray()); l > maxLen[colIdx] {
						maxLen[colIdx] = l
					}
				default:
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("failed to read parquet rows: %v", err)
		}
	}

	options := make([]goparquet.WriterOption, 0)
	skippedNames := make([]string, 0)
	for i, max := range maxLen {
		if max > threshold {
			options = append(options, goparquet.SkipPageBounds(schemaCols[i]...))
			skippedNames = append(skippedNames, strings.Join(schemaCols[i], "."))
		}
	}

	return options, skippedNames
}

func parquetFooterSize(t *testing.T, data []byte) int {
	t.Helper()

	if len(data) < 8 {
		t.Fatalf("parquet data too small: %d bytes", len(data))
	}
	if string(data[len(data)-4:]) != "PAR1" {
		t.Fatalf("invalid parquet magic trailer")
	}
	return int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
}
