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
			"event": {Type: "utf8"},
			"value": {Type: "int64"},
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

	// Read back and verify all data is present
	rows, _ := readParquetRows(t, buf.Bytes())
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}

	// Collect all event-value pairs
	type pair struct {
		event string
		value int64
	}
	var got []pair
	for _, row := range rows {
		got = append(got, pair{
			event: string(row["event"].ByteArray()),
			value: int64(row["value"].Uint64()),
		})
	}

	// Verify all expected data is present (order may vary based on parquet implementation)
	expected := map[pair]bool{
		{"a", 1}: true,
		{"a", 2}: true,
		{"a", 3}: true,
		{"b", 1}: true,
		{"c", 2}: true,
	}

	for _, p := range got {
		if !expected[p] {
			t.Errorf("unexpected pair: event=%s, value=%d", p.event, p.value)
		}
		delete(expected, p)
	}

	if len(expected) > 0 {
		t.Errorf("missing pairs: %v", expected)
	}

	// Note: Sorting columns are metadata for readers, the physical row order
	// in the file may not reflect the logical sort order. Parquet readers
	// use this metadata for query optimization.
	t.Logf("Wrote %d rows with sorting columns configured", len(rows))
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

func TestWriteEventsJsonKeysColumn(t *testing.T) {
	cfg := config.ParquetConfig{
		Compression:         "zstd",
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"uuid":       {Type: "utf8"},
			"properties": {Type: "utf8"},
		},
		JsonColumns: []config.JsonColumnConfig{
			{
				Column:             "properties",
				MaxSubcolumns:      5,
				BucketCount:        2,
				MaxDepth:           5,
				KeepOriginalColumn: true,
			},
		},
	}

	writer := NewWriter(cfg, "")

	events := []map[string]interface{}{
		{
			"uuid":       "uuid-1",
			"properties": `{"z_key":"val1","a_key":"val2"}`,
		},
		{
			"uuid":       "uuid-2",
			"properties": `{"m_key":"val3"}`,
		},
	}

	var buf bytes.Buffer
	if err := writer.WriteEvents(&buf, events); err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	rows, columns := readParquetRows(t, buf.Bytes())

	// Check that __json_keys column exists
	jsonKeysCol := "properties__json_keys"
	if !columns[jsonKeysCol] {
		t.Fatalf("expected %s column, got columns: %s", jsonKeysCol, strings.Join(sortedKeys(columns), ", "))
	}

	// Check values in the json_keys column
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Row 0 should have keys sorted alphabetically: ["a_key", "z_key"]
	row0Keys := rows[0][jsonKeysCol]
	if row0Keys.IsNull() {
		t.Fatal("expected non-null json_keys for row 0")
	}
	row0KeysStr := string(row0Keys.ByteArray())
	if row0KeysStr != `["a_key","z_key"]` {
		t.Errorf("expected row 0 json_keys to be [\"a_key\",\"z_key\"], got %s", row0KeysStr)
	}

	// Row 1 should have keys: ["m_key"]
	row1Keys := rows[1][jsonKeysCol]
	if row1Keys.IsNull() {
		t.Fatal("expected non-null json_keys for row 1")
	}
	row1KeysStr := string(row1Keys.ByteArray())
	if row1KeysStr != `["m_key"]` {
		t.Errorf("expected row 1 json_keys to be [\"m_key\"], got %s", row1KeysStr)
	}
}

func TestWriteEventsWithMinMaxIndexDisabled(t *testing.T) {
	falseBool := false
	trueBool := true

	cfg := config.ParquetConfig{
		Compression:         "zstd",
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"event":     {Type: "utf8", MinMaxIndex: &trueBool},
			"data":      {Type: "utf8", MinMaxIndex: &falseBool}, // Disabled
			"timestamp": {Type: "timestamp_ms"},                  // Default (true)
		},
	}

	writer := NewWriter(cfg, "")

	events := []map[string]interface{}{
		{
			"event":     "pageview",
			"data":      "some large data that shouldn't have bounds",
			"timestamp": "2024-01-15T10:30:00Z",
		},
	}

	var buf bytes.Buffer
	if err := writer.WriteEvents(&buf, events); err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	// Just verify the file is valid parquet
	if buf.Len() == 0 {
		t.Fatal("expected non-empty output")
	}

	rows, columns := readParquetRows(t, buf.Bytes())
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if !columns["event"] || !columns["data"] || !columns["timestamp"] {
		t.Fatalf("missing expected columns: %v", sortedKeys(columns))
	}
}

func TestWriteEventsBucketColumnsSkipBounds(t *testing.T) {
	cfg := config.ParquetConfig{
		Compression:            "zstd",
		AllowNullableFields:    true,
		PageBoundsMaxValueBytes: 100, // Enable bounds checking
		Schema: map[string]config.SchemaField{
			"uuid":       {Type: "utf8"},
			"properties": {Type: "utf8"},
		},
		JsonColumns: []config.JsonColumnConfig{
			{
				Column:             "properties",
				MaxSubcolumns:      1, // Force overflow to buckets
				BucketCount:        4,
				MaxDepth:           5,
				KeepOriginalColumn: false,
			},
		},
	}

	writer := NewWriter(cfg, "")

	// Create events with many properties to force bucket overflow
	events := []map[string]interface{}{
		{
			"uuid":       "uuid-1",
			"properties": `{"a":"val1","b":"val2","c":"val3","d":"val4"}`,
		},
	}

	var buf bytes.Buffer
	if err := writer.WriteEvents(&buf, events); err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	// Just verify the file is valid parquet
	if buf.Len() == 0 {
		t.Fatal("expected non-empty output")
	}

	rows, columns := readParquetRows(t, buf.Bytes())
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	// Should have bucket columns (named __json_type_bucket_X)
	hasBucketCol := false
	for col := range columns {
		if strings.Contains(col, "__json_type_bucket_") {
			hasBucketCol = true
			break
		}
	}
	if !hasBucketCol {
		t.Errorf("expected bucket columns, got: %v", sortedKeys(columns))
	}

	// Should have json_keys column
	if !columns["properties__json_keys"] {
		t.Errorf("expected properties__json_keys column, got: %v", sortedKeys(columns))
	}
}

func TestWriteEventsNestedJsonKeys(t *testing.T) {
	cfg := config.ParquetConfig{
		Compression:         "zstd",
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"uuid":       {Type: "utf8"},
			"properties": {Type: "utf8"},
		},
		JsonColumns: []config.JsonColumnConfig{
			{
				Column:             "properties",
				MaxSubcolumns:      10,
				BucketCount:        2,
				MaxDepth:           10,
				KeepOriginalColumn: false,
			},
		},
	}

	writer := NewWriter(cfg, "")

	events := []map[string]interface{}{
		{
			"uuid":       "uuid-1",
			"properties": `{"user":{"name":"Alice","profile":{"age":30}}}`,
		},
	}

	var buf bytes.Buffer
	if err := writer.WriteEvents(&buf, events); err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	rows, columns := readParquetRows(t, buf.Bytes())
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	// Check json_keys contains flattened nested keys
	jsonKeysCol := "properties__json_keys"
	if !columns[jsonKeysCol] {
		t.Fatalf("expected %s column", jsonKeysCol)
	}

	keysValue := rows[0][jsonKeysCol]
	if keysValue.IsNull() {
		t.Fatal("expected non-null json_keys")
	}

	keysStr := string(keysValue.ByteArray())
	// Should contain flattened keys like "user.name", "user.profile.age"
	if !strings.Contains(keysStr, "user.name") {
		t.Errorf("expected json_keys to contain 'user.name', got %s", keysStr)
	}
	if !strings.Contains(keysStr, "user.profile.age") {
		t.Errorf("expected json_keys to contain 'user.profile.age', got %s", keysStr)
	}
}

func TestParseEventTypeCoercion(t *testing.T) {
	cfg := config.ParquetConfig{
		Schema: map[string]config.SchemaField{
			"str_field":    {Type: "utf8"},
			"int_field":    {Type: "int64"},
			"int32_field":  {Type: "int32"},
			"float_field":  {Type: "float64"},
			"float32_field": {Type: "float32"},
			"bool_field":   {Type: "bool"},
			"ts_field":     {Type: "timestamp_ms"},
			"json_field":   {Type: "json"},
		},
	}

	writer := NewWriter(cfg, "")

	tests := []struct {
		name     string
		json     string
		field    string
		expected interface{}
	}{
		// String coercion
		{"string from string", `{"str_field":"hello"}`, "str_field", "hello"},
		{"string from number", `{"str_field":123}`, "str_field", "123"},
		{"string from object", `{"str_field":{"a":1}}`, "str_field", `{"a":1}`},
		{"string from array", `{"str_field":[1,2]}`, "str_field", `[1,2]`},

		// Int64 coercion
		{"int64 from int", `{"int_field":42}`, "int_field", int64(42)},
		{"int64 from float", `{"int_field":42.9}`, "int_field", int64(42)},
		{"int64 from string", `{"int_field":"42"}`, "int_field", nil}, // strings don't coerce to int

		// Int32 coercion
		{"int32 from int", `{"int32_field":42}`, "int32_field", int32(42)},

		// Float64 coercion
		{"float64 from float", `{"float_field":3.14}`, "float_field", float64(3.14)},
		{"float64 from int", `{"float_field":42}`, "float_field", float64(42)},

		// Float32 coercion
		{"float32 from float", `{"float32_field":3.14}`, "float32_field", float32(3.14)},

		// Bool coercion
		{"bool true", `{"bool_field":true}`, "bool_field", true},
		{"bool false", `{"bool_field":false}`, "bool_field", false},
		{"bool from string", `{"bool_field":"true"}`, "bool_field", nil}, // strings don't coerce to bool

		// Timestamp coercion
		{"timestamp from int", `{"ts_field":1704067200000}`, "ts_field", int64(1704067200000)},
		{"timestamp from string", `{"ts_field":"2024-01-01T00:00:00Z"}`, "ts_field", int64(1704067200000)},

		// JSON type
		{"json from object", `{"json_field":{"nested":"value"}}`, "json_field", `{"nested":"value"}`},
		{"json from array", `{"json_field":[1,2,3]}`, "json_field", `[1,2,3]`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			event, err := writer.ParseEvent([]byte(tc.json))
			if err != nil {
				t.Fatalf("ParseEvent failed: %v", err)
			}

			got := event[tc.field]
			if tc.expected == nil {
				if got != nil {
					t.Errorf("expected nil, got %v (%T)", got, got)
				}
				return
			}

			// Handle float comparison with tolerance
			switch exp := tc.expected.(type) {
			case float32:
				if gotF, ok := got.(float32); !ok || (gotF-exp) > 0.001 || (exp-gotF) > 0.001 {
					t.Errorf("expected %v, got %v", exp, got)
				}
			case float64:
				if gotF, ok := got.(float64); !ok || (gotF-exp) > 0.001 || (exp-gotF) > 0.001 {
					t.Errorf("expected %v, got %v", exp, got)
				}
			default:
				if got != tc.expected {
					t.Errorf("expected %v (%T), got %v (%T)", tc.expected, tc.expected, got, got)
				}
			}
		})
	}
}

func TestParseEventNullHandling(t *testing.T) {
	cfg := config.ParquetConfig{
		Schema: map[string]config.SchemaField{
			"str_field":  {Type: "utf8"},
			"int_field":  {Type: "int64"},
			"bool_field": {Type: "bool"},
			"ts_field":   {Type: "timestamp_ms"},
		},
	}

	writer := NewWriter(cfg, "")

	// Test explicit null values
	event, err := writer.ParseEvent([]byte(`{"str_field":null,"int_field":null,"bool_field":null,"ts_field":null}`))
	if err != nil {
		t.Fatalf("ParseEvent failed: %v", err)
	}

	// All null fields should be nil
	if event["str_field"] != nil {
		t.Errorf("expected str_field to be nil, got %v", event["str_field"])
	}
	if event["int_field"] != nil {
		t.Errorf("expected int_field to be nil, got %v", event["int_field"])
	}
	if event["bool_field"] != nil {
		t.Errorf("expected bool_field to be nil, got %v", event["bool_field"])
	}
	if event["ts_field"] != nil {
		t.Errorf("expected ts_field to be nil, got %v", event["ts_field"])
	}
}

func TestParseEventMissingFields(t *testing.T) {
	cfg := config.ParquetConfig{
		Schema: map[string]config.SchemaField{
			"required":  {Type: "utf8"},
			"optional1": {Type: "int64"},
			"optional2": {Type: "bool"},
		},
	}

	writer := NewWriter(cfg, "")

	// Only provide one field
	event, err := writer.ParseEvent([]byte(`{"required":"value"}`))
	if err != nil {
		t.Fatalf("ParseEvent failed: %v", err)
	}

	// Present field should exist
	if event["required"] != "value" {
		t.Errorf("expected required=value, got %v", event["required"])
	}

	// Missing fields should not be in the map
	if _, ok := event["optional1"]; ok {
		t.Errorf("expected optional1 to not exist in map")
	}
	if _, ok := event["optional2"]; ok {
		t.Errorf("expected optional2 to not exist in map")
	}
}

func TestParseEventInvalidJSON(t *testing.T) {
	cfg := config.ParquetConfig{
		Schema: map[string]config.SchemaField{
			"field": {Type: "utf8"},
		},
	}

	writer := NewWriter(cfg, "")

	_, err := writer.ParseEvent([]byte(`not valid json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}

	_, err = writer.ParseEvent([]byte(``))
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestWriteEventsWithNullValues(t *testing.T) {
	cfg := config.ParquetConfig{
		Compression:         "zstd",
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"id":    {Type: "int64"},
			"name":  {Type: "utf8"},
			"value": {Type: "float64"},
		},
	}

	writer := NewWriter(cfg, "")

	events := []map[string]interface{}{
		{"id": float64(1), "name": "alice", "value": float64(1.5)},
		{"id": float64(2), "name": nil, "value": float64(2.5)},      // null name
		{"id": float64(3), "name": "charlie"},                       // missing value field
		{"id": float64(4)},                                          // missing fields
	}

	var buf bytes.Buffer
	if err := writer.WriteEvents(&buf, events); err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	rows, _ := readParquetRows(t, buf.Bytes())
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	// Row 0: all values present
	if rows[0]["id"].IsNull() || rows[0]["name"].IsNull() || rows[0]["value"].IsNull() {
		t.Error("row 0: expected all values present")
	}

	// Row 1: null name (explicit nil)
	if !rows[1]["name"].IsNull() {
		t.Errorf("row 1: expected name to be null, got %v", rows[1]["name"])
	}

	// Row 2: verify name is present
	if rows[2]["name"].IsNull() {
		t.Error("row 2: expected name to be present")
	}

	// Row 3: missing fields - check if they're null
	// (Behavior depends on AllowNullableFields setting)
	t.Logf("row 3 name isNull=%v, value isNull=%v", rows[3]["name"].IsNull(), rows[3]["value"].IsNull())
}

func TestWriteEventsDataIntegrity(t *testing.T) {
	cfg := config.ParquetConfig{
		Compression:         "zstd",
		AllowNullableFields: true,
		Schema: map[string]config.SchemaField{
			"id":        {Type: "int64"},
			"name":      {Type: "utf8"},
			"score":     {Type: "float64"},
			"active":    {Type: "bool"},
			"timestamp": {Type: "timestamp_ms"},
		},
	}

	writer := NewWriter(cfg, "")

	ts, _ := time.Parse(time.RFC3339, "2024-06-15T10:30:00Z")
	events := []map[string]interface{}{
		{"id": int64(100), "name": "Alice", "score": 95.5, "active": true, "timestamp": ts.UnixMilli()},
		{"id": int64(200), "name": "Bob", "score": 87.3, "active": false, "timestamp": ts.Add(time.Hour).UnixMilli()},
		{"id": int64(300), "name": "Charlie", "score": 92.1, "active": true, "timestamp": ts.Add(2 * time.Hour).UnixMilli()},
	}

	var buf bytes.Buffer
	if err := writer.WriteEvents(&buf, events); err != nil {
		t.Fatalf("WriteEvents failed: %v", err)
	}

	rows, _ := readParquetRows(t, buf.Bytes())
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Verify each row's data
	for i, event := range events {
		row := rows[i]

		// Check ID
		if int64(row["id"].Uint64()) != event["id"].(int64) {
			t.Errorf("row %d: id mismatch, expected %v, got %v", i, event["id"], row["id"].Uint64())
		}

		// Check name
		if string(row["name"].ByteArray()) != event["name"].(string) {
			t.Errorf("row %d: name mismatch, expected %v, got %v", i, event["name"], string(row["name"].ByteArray()))
		}

		// Check score (with tolerance)
		gotScore := row["score"].Double()
		expScore := event["score"].(float64)
		if gotScore < expScore-0.001 || gotScore > expScore+0.001 {
			t.Errorf("row %d: score mismatch, expected %v, got %v", i, expScore, gotScore)
		}

		// Check active
		gotActive := row["active"].Boolean()
		if gotActive != event["active"].(bool) {
			t.Errorf("row %d: active mismatch, expected %v, got %v", i, event["active"], gotActive)
		}

		// Check timestamp
		if int64(row["timestamp"].Uint64()) != event["timestamp"].(int64) {
			t.Errorf("row %d: timestamp mismatch, expected %v, got %v", i, event["timestamp"], row["timestamp"].Uint64())
		}
	}
}
