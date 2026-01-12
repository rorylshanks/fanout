package parquet

import (
	"bytes"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	goparquet "github.com/parquet-go/parquet-go"
	"fanout/internal/config"
)

func TestWriteEvents(t *testing.T) {
	cfg := config.ParquetConfig{
		Compression:         "zstd",
		CompressionLevel:    5,
		RowsPerFile:         10000,
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"uuid":        {Type: "utf8"},
			"event":       {Type: "utf8"},
			"properties":  {Type: "utf8"},
			"timestamp":   {Type: "timestamp_ms"},
			"team_id":     {Type: "int64"},
			"distinct_id": {Type: "utf8"},
		},
		JsonColumns: []config.JsonColumnConfig{
			{
				Column:             "properties",
				MaxSubcolumns:      10,
				BucketCount:        4,
				MaxDepth:           5,
				KeepOriginalColumn: true,
			},
		},
		SortingColumns: []config.SortingColumn{
			{Column: "event", Descending: false},
		},
	}

	writer := NewWriter(cfg, "")

	events := []map[string]interface{}{
		{
			"uuid":        "uuid-1",
			"event":       "pageview",
			"properties":  `{"url": "https://example.com", "referrer": "google.com"}`,
			"timestamp":   "2024-01-15T10:30:00Z",
			"team_id":     float64(123),
			"distinct_id": "user-1",
		},
		{
			"uuid":        "uuid-2",
			"event":       "click",
			"properties":  `{"element": "button", "page": "/home"}`,
			"timestamp":   "2024-01-15T10:31:00Z",
			"team_id":     float64(123),
			"distinct_id": "user-1",
		},
		{
			"uuid":        "uuid-3",
			"event":       "pageview",
			"properties":  `{"url": "https://example.com/about", "referrer": "internal"}`,
			"timestamp":   "2024-01-15T10:32:00Z",
			"team_id":     float64(456),
			"distinct_id": "user-2",
		},
	}

	var buf bytes.Buffer
	err := writer.WriteEvents(&buf, events)
	if err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	if buf.Len() == 0 {
		t.Fatal("Expected non-empty parquet output")
	}

	t.Logf("Wrote %d bytes of parquet data", buf.Len())
}

func TestWriteEventsWithSorting(t *testing.T) {
	cfg := config.ParquetConfig{
		Compression:         "zstd",
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"event":  {Type: "utf8"},
			"value":  {Type: "int64"},
		},
		SortingColumns: []config.SortingColumn{
			{Column: "event", Descending: false},
			{Column: "value", Descending: true},
		},
	}

	writer := NewWriter(cfg, "")

	events := []map[string]interface{}{
		{"event": "b", "value": float64(1)},
		{"event": "a", "value": float64(3)},
		{"event": "a", "value": float64(1)},
		{"event": "c", "value": float64(2)},
		{"event": "a", "value": float64(2)},
	}

	var buf bytes.Buffer
	err := writer.WriteEvents(&buf, events)
	if err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	if buf.Len() == 0 {
		t.Fatal("Expected non-empty parquet output")
	}

	t.Logf("Wrote %d bytes of sorted parquet data", buf.Len())
}

func TestWriteEventsEmpty(t *testing.T) {
	cfg := config.ParquetConfig{
		Schema: map[string]config.SchemaField{
			"event": {Type: "utf8"},
		},
	}

	writer := NewWriter(cfg, "")

	var buf bytes.Buffer
	err := writer.WriteEvents(&buf, nil)
	if err != nil {
		t.Fatalf("WriteEvents failed on empty: %v", err)
	}

	if buf.Len() != 0 {
		t.Fatal("Expected empty output for no events")
	}
}

func TestWriteEventsRoundTrip(t *testing.T) {
	cfg := config.ParquetConfig{
		Compression:         "zstd",
		CompressionLevel:    5,
		RowsPerFile:         10000,
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"uuid":       {Type: "utf8"},
			"event":      {Type: "utf8"},
			"properties": {Type: "utf8"},
			"timestamp":  {Type: "timestamp_ms"},
			"team_id":    {Type: "int64"},
		},
		JsonColumns: []config.JsonColumnConfig{
			{
				Column:             "properties",
				MaxSubcolumns:      8,
				BucketCount:        4,
				MaxDepth:           5,
				KeepOriginalColumn: true,
			},
		},
	}

	writer := NewWriter(cfg, "")

	events := []map[string]interface{}{
		{
			"uuid":       "uuid-1",
			"event":      "pageview",
			"properties": `{"url":"https://example.com","referrer":"google.com"}`,
			"timestamp":  "2024-01-15T10:30:00Z",
			"team_id":    float64(123),
		},
		{
			"uuid":       "uuid-2",
			"event":      "click",
			"properties": `{"element":"button"}`,
			"timestamp":  "2024-01-15T10:31:00Z",
			"team_id":    float64(456),
		},
	}

	var buf bytes.Buffer
	if err := writer.WriteEvents(&buf, events); err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("Expected non-empty parquet output")
	}

	rows, columns := readParquetRows(t, buf.Bytes())
	if len(rows) != len(events) {
		t.Fatalf("expected %d rows, got %d", len(events), len(rows))
	}

	urlColumn := "properties.url"
	referrerColumn := "properties.referrer"
	if !columns[urlColumn] || !columns[referrerColumn] {
		t.Fatalf("expected JSON subcolumns %q and %q, got columns: %s", urlColumn, referrerColumn, strings.Join(sortedKeys(columns), ", "))
	}

	row0 := rows[0]
	assertStringValue(t, row0, "uuid", "uuid-1")
	assertStringValue(t, row0, "event", "pageview")
	assertStringValue(t, row0, urlColumn, "https://example.com")
	assertStringValue(t, row0, referrerColumn, "google.com")

	ts0, err := time.Parse(time.RFC3339, "2024-01-15T10:30:00Z")
	if err != nil {
		t.Fatalf("failed to parse timestamp: %v", err)
	}
	assertInt64Value(t, row0, "timestamp", ts0.UnixMilli())
	assertInt64Value(t, row0, "team_id", 123)
}

func readParquetRows(t *testing.T, data []byte) ([]map[string]goparquet.Value, map[string]bool) {
	t.Helper()

	reader := goparquet.NewReader(bytes.NewReader(data))
	defer reader.Close()

	schema := reader.Schema()
	columnPaths := schema.Columns()
	columnNames := make([]string, len(columnPaths))
	columnSet := make(map[string]bool, len(columnPaths))
	for i, path := range columnPaths {
		name := strings.Join(path, ".")
		columnNames[i] = name
		columnSet[name] = true
	}

	var rows []map[string]goparquet.Value
	buf := make([]goparquet.Row, 64)
	for {
		n, err := reader.ReadRows(buf)
		for i := 0; i < n; i++ {
			row := make(map[string]goparquet.Value, len(columnNames))
			for colIdx, name := range columnNames {
				row[name] = buf[i][colIdx]
			}
			rows = append(rows, row)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("failed to read parquet rows: %v", err)
		}
	}

	return rows, columnSet
}

func assertStringValue(t *testing.T, row map[string]goparquet.Value, column string, expected string) {
	t.Helper()
	val, ok := row[column]
	if !ok {
		t.Fatalf("missing column %q", column)
	}
	if val.IsNull() {
		t.Fatalf("column %q is null, expected %q", column, expected)
	}
	got := string(val.ByteArray())
	if got != expected {
		t.Fatalf("column %q: expected %q, got %q", column, expected, got)
	}
}

func assertInt64Value(t *testing.T, row map[string]goparquet.Value, column string, expected int64) {
	t.Helper()
	val, ok := row[column]
	if !ok {
		t.Fatalf("missing column %q", column)
	}
	if val.IsNull() {
		t.Fatalf("column %q is null, expected %d", column, expected)
	}
	if val.Kind() != goparquet.Int64 {
		t.Fatalf("column %q: expected int64, got %s", column, val.Kind().String())
	}
	got := int64(val.Uint64())
	if got != expected {
		t.Fatalf("column %q: expected %d, got %d", column, expected, got)
	}
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
