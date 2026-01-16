package parquet

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"
	"fanout/internal/config"
)

func TestStreamingProcessorNoSort(t *testing.T) {
	tmpDir := t.TempDir()
	bufferPath := writeRawEventsToBuffer(t, tmpDir, "partition", [][]byte{
		[]byte(`{"event":"a","value":1}`),
		[]byte(`{"event":"b","value":2}`),
		[]byte(`{"event":"c","value":3}`),
	})

	cfg := config.ParquetConfig{
		RowsPerFile:         2,
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"event": {Type: "utf8"},
			"value": {Type: "int64"},
		},
	}

	processor := NewStreamingProcessor(cfg, tmpDir, tmpDir)
	outputDir := filepath.Join(tmpDir, "out")
	files, err := processor.ProcessBufferToFiles(bufferPath, outputDir)
	if err != nil {
		t.Fatalf("process buffer: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}

	totalRows := 0
	for _, info := range files {
		totalRows += info.RowCount
		if _, err := os.Stat(info.Path); err != nil {
			t.Fatalf("expected parquet file %s to exist: %v", info.Path, err)
		}
	}
	if totalRows != 3 {
		t.Fatalf("expected 3 rows total, got %d", totalRows)
	}
}

func TestStreamingProcessorWithSort(t *testing.T) {
	tmpDir := t.TempDir()
	events := []map[string]interface{}{
		{"event": "b", "value": 3},
		{"event": "a", "value": 1},
		{"event": "c", "value": 2},
	}
	raw := make([][]byte, 0, len(events))
	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		raw = append(raw, data)
	}
	bufferPath := writeRawEventsToBuffer(t, tmpDir, "partition", raw)

	cfg := config.ParquetConfig{
		RowsPerFile:         2,
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"event": {Type: "utf8"},
			"value": {Type: "int64"},
		},
		SortingColumns: []config.SortingColumn{
			{Column: "value", Descending: false},
		},
	}

	processor := NewStreamingProcessor(cfg, tmpDir, tmpDir)
	outputDir := filepath.Join(tmpDir, "out")
	files, err := processor.ProcessBufferToFiles(bufferPath, outputDir)
	if err != nil {
		t.Fatalf("process buffer: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected parquet files")
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	var values []int64
	for _, info := range files {
		vals, err := readParquetIntColumnFromFile(info.Path, "value")
		if err != nil {
			t.Fatalf("read parquet %s: %v", info.Path, err)
		}
		values = append(values, vals...)
	}

	if len(values) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(values))
	}
	if values[0] != 1 || values[1] != 2 || values[2] != 3 {
		t.Fatalf("unexpected sort order: %v", values)
	}
}

func readParquetIntColumnFromFile(path string, column string) ([]int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := goparquet.NewReader(file)
	defer reader.Close()

	columnIdx := -1
	for idx, path := range reader.Schema().Columns() {
		if strings.Join(path, ".") == column {
			columnIdx = idx
			break
		}
	}
	if columnIdx == -1 {
		return nil, nil
	}

	var values []int64
	rowBuf := make([]goparquet.Row, 64)
	for {
		n, err := reader.ReadRows(rowBuf)
		for i := 0; i < n; i++ {
			val := rowBuf[i][columnIdx]
			if val.IsNull() {
				continue
			}
			values = append(values, val.Int64())
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}
