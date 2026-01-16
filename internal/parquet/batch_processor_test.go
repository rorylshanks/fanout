package parquet

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"
	"fanout/internal/buffer"
	"fanout/internal/config"
)

func TestBatchProcessorProcessBufferSorted(t *testing.T) {
	tmpDir := t.TempDir()
	bufferPath := writeRawEventsToBuffer(t, tmpDir, "partition", [][]byte{
		[]byte(`{"event":"b","value":2}`),
		[]byte(`{"event":"a","value":3}`),
		[]byte(`{"event":"c","value":1}`),
	})

	cfg := config.ParquetConfig{
		RowsPerFile:         100,
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"event": {Type: "utf8"},
			"value": {Type: "int64"},
		},
		SortingColumns: []config.SortingColumn{
			{Column: "value", Descending: false},
		},
	}

	processor := NewBatchProcessor(cfg, tmpDir)
	var out bytes.Buffer
	if err := processor.ProcessBufferToWriter(bufferPath, &out); err != nil {
		t.Fatalf("process buffer to writer: %v", err)
	}

	values := readParquetIntColumn(t, out.Bytes(), "value")
	if len(values) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(values))
	}
	if values[0] != 1 || values[1] != 2 || values[2] != 3 {
		t.Fatalf("unexpected sort order: %v", values)
	}
}

func TestBatchProcessorProcessBufferSkipsInvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	bufferPath := writeRawEventsToBuffer(t, tmpDir, "partition", [][]byte{
		[]byte(`{"event":"a","value":1}`),
		[]byte(`not-json`),
	})

	cfg := config.ParquetConfig{
		RowsPerFile:         100,
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"event": {Type: "utf8"},
			"value": {Type: "int64"},
		},
	}

	processor := NewBatchProcessor(cfg, tmpDir)
	var out bytes.Buffer
	if err := processor.ProcessBufferToWriter(bufferPath, &out); err != nil {
		t.Fatalf("process buffer to writer: %v", err)
	}

	values := readParquetIntColumn(t, out.Bytes(), "value")
	if len(values) != 1 {
		t.Fatalf("expected 1 row, got %d", len(values))
	}
	if values[0] != 1 {
		t.Fatalf("unexpected value: %v", values)
	}
}

func TestBatchProcessorWriteToBuffer(t *testing.T) {
	cfg := config.ParquetConfig{
		RowsPerFile:         100,
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"event": {Type: "utf8"},
			"value": {Type: "int64"},
		},
	}

	processor := NewBatchProcessor(cfg, "")
	events := []map[string]interface{}{
		{"event": "a", "value": float64(1)},
		{"event": "b", "value": float64(2)},
	}
	data, err := processor.WriteToBuffer(events)
	if err != nil {
		t.Fatalf("write to buffer: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty parquet data")
	}
}

func writeRawEventsToBuffer(t *testing.T, baseDir, partition string, events [][]byte) string {
	t.Helper()

	buf, err := buffer.NewDiskBuffer(baseDir, partition)
	if err != nil {
		t.Fatalf("create buffer: %v", err)
	}
	for _, ev := range events {
		if err := buf.Write(ev); err != nil {
			t.Fatalf("write buffer: %v", err)
		}
	}
	if err := buf.CloseForFlush(); err != nil {
		t.Fatalf("close buffer: %v", err)
	}
	return buf.Path()
}

func readParquetIntColumn(t *testing.T, data []byte, column string) []int64 {
	t.Helper()

	reader := bytes.NewReader(data)
	file, err := goparquet.OpenFile(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	var columnIdx int = -1
	for idx, path := range file.Schema().Columns() {
		if strings.Join(path, ".") == column {
			columnIdx = idx
			break
		}
	}
	if columnIdx == -1 {
		t.Fatalf("column %q not found", column)
	}

	var values []int64
	rowGroup := file.RowGroups()[0]
	rows := rowGroup.Rows()
	defer rows.Close()
	rowBuf := make([]goparquet.Row, 64)
	for {
		n, err := rows.ReadRows(rowBuf)
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
			t.Fatalf("read rows: %v", err)
		}
	}
	return values
}

func TestBatchProcessorProcessBufferWritesFiles(t *testing.T) {
	tmpDir := t.TempDir()
	events := []map[string]interface{}{
		{"event": "a", "value": 1},
		{"event": "b", "value": 2},
		{"event": "c", "value": 3},
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
	}

	processor := NewBatchProcessor(cfg, tmpDir)
	outputDir := filepath.Join(tmpDir, "out")
	paths, err := processor.ProcessBuffer(bufferPath, outputDir)
	if err != nil {
		t.Fatalf("process buffer: %v", err)
	}
	if len(paths) != 2 {
		t.Fatalf("expected 2 parquet files, got %d", len(paths))
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected parquet file %s to exist: %v", p, err)
		}
	}
}
