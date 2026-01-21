package jsonexpand

import (
	"testing"

	"fanout/internal/config"
)

func TestProcessBatch(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:             "properties",
		MaxSubcolumns:      5,
		BucketCount:        4,
		MaxDepth:           3,
		KeepOriginalColumn: true,
	}

	processor := NewJsonColumnProcessor(cfg)

	jsonValues := []string{
		`{"url": "https://example.com", "referrer": "google.com", "ip": "1.2.3.4"}`,
		`{"url": "https://example.com/about", "page": "/about"}`,
		`{"url": "https://example.com/home", "referrer": "internal", "browser": "Chrome"}`,
	}

	result := processor.ProcessBatch(jsonValues)

	if result.NumRows != 3 {
		t.Errorf("Expected 3 rows, got %d", result.NumRows)
	}

	if result.ColumnName != "properties" {
		t.Errorf("Expected column name 'properties', got '%s'", result.ColumnName)
	}

	// Check that we have subcolumns
	if len(result.Subcolumns) == 0 {
		t.Error("Expected at least one subcolumn")
	}

	// Check that url is a subcolumn (it appears in all 3 rows)
	if _, ok := result.Subcolumns["properties.url"]; !ok {
		t.Error("Expected 'properties.url' to be a subcolumn")
	}

	// Verify original values are kept
	if result.OriginalValues == nil {
		t.Error("Expected original values to be kept")
	} else if len(result.OriginalValues) != 3 {
		t.Errorf("Expected 3 original values, got %d", len(result.OriginalValues))
	}
}

func TestProcessBatchNested(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:             "data",
		MaxSubcolumns:      10,
		BucketCount:        2,
		MaxDepth:           3,
		KeepOriginalColumn: false,
	}

	processor := NewJsonColumnProcessor(cfg)

	jsonValues := []string{
		`{"user": {"name": "Alice", "age": 30}, "action": "click"}`,
		`{"user": {"name": "Bob", "age": 25}, "action": "view"}`,
	}

	result := processor.ProcessBatch(jsonValues)

	// Check nested keys are flattened
	if _, ok := result.Subcolumns["data.user.name"]; !ok {
		t.Error("Expected 'data.user.name' to be a subcolumn")
	}

	if _, ok := result.Subcolumns["data.user.age"]; !ok {
		t.Error("Expected 'data.user.age' to be a subcolumn")
	}

	if _, ok := result.Subcolumns["data.action"]; !ok {
		t.Error("Expected 'data.action' to be a subcolumn")
	}
}

func TestProcessBatchOverflow(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:             "props",
		MaxSubcolumns:      2, // Only 2 subcolumns
		BucketCount:        4,
		MaxDepth:           2,
		KeepOriginalColumn: false,
	}

	processor := NewJsonColumnProcessor(cfg)

	// Create JSON with many keys
	jsonValues := []string{
		`{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}`,
	}

	result := processor.ProcessBatch(jsonValues)

	// Only 2 should be subcolumns
	if len(result.Subcolumns) > 2 {
		t.Errorf("Expected at most 2 subcolumns, got %d", len(result.Subcolumns))
	}

	// The rest should be in bucket maps
	totalOverflow := 0
	for _, bucket := range result.BucketMaps {
		for _, rowMap := range bucket {
			if rowMap != nil {
				totalOverflow += len(rowMap)
			}
		}
	}

	// We had 5 keys, 2 are subcolumns, 3 should overflow
	if totalOverflow != 3 {
		t.Errorf("Expected 3 overflow keys, got %d", totalOverflow)
	}
}

func TestMurmurHashDistribution(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:        "test",
		MaxSubcolumns: 0, // All keys go to buckets
		BucketCount:   4,
		MaxDepth:      1,
	}

	processor := NewJsonColumnProcessor(cfg)

	// Generate 100 different keys and check distribution
	bucketCounts := make(map[int]int)
	for i := 0; i < 100; i++ {
		key := string(rune('a'+i%26)) + string(rune('0'+i/26))
		bucket := processor.hashKeyToBucket(key)
		bucketCounts[bucket]++
	}

	// All buckets should have at least some keys
	for i := 0; i < 4; i++ {
		if bucketCounts[i] == 0 {
			t.Errorf("Bucket %d has no keys - poor distribution", i)
		}
	}
}

func TestProcessBatchLargeIntegers(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:             "data",
		MaxSubcolumns:      10,
		BucketCount:        2,
		MaxDepth:           2,
		KeepOriginalColumn: false,
	}

	processor := NewJsonColumnProcessor(cfg)

	// Test with integers larger than max int64 (9223372036854775807)
	jsonValues := []string{
		`{"small_int": 123, "max_int64": 9223372036854775807, "large_uint64": 18446744073709551615, "float": 3.14}`,
		`{"small_int": 456, "max_int64": -9223372036854775808, "large_uint64": 9223372036854775808}`,
	}

	result := processor.ProcessBatch(jsonValues)

	// small_int should be int64 type
	if subCol, ok := result.Subcolumns["data.small_int"]; ok {
		if subCol.Type != TypeInt64 {
			t.Errorf("Expected small_int to be TypeInt64, got %v", subCol.Type)
		}
	} else {
		t.Error("Expected data.small_int subcolumn")
	}

	// large_uint64 should be string type (too large for int64)
	if subCol, ok := result.Subcolumns["data.large_uint64"]; ok {
		if subCol.Type != TypeString {
			t.Errorf("Expected large_uint64 to be TypeString (quoted), got %v", subCol.Type)
		}
		// Check the actual value is the string representation
		if len(subCol.Values) > 0 && subCol.Values[0] != nil {
			val := subCol.Values[0].(string)
			if val != "18446744073709551615" {
				t.Errorf("Expected large_uint64 value to be '18446744073709551615', got '%s'", val)
			}
		}
	} else {
		t.Error("Expected data.large_uint64 subcolumn")
	}

	// float should be float64
	if subCol, ok := result.Subcolumns["data.float"]; ok {
		if subCol.Type != TypeFloat64 {
			t.Errorf("Expected float to be TypeFloat64, got %v", subCol.Type)
		}
	}
}

func TestProcessBatchEmptyAndNull(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:             "data",
		MaxSubcolumns:      10,
		BucketCount:        2,
		MaxDepth:           2,
		KeepOriginalColumn: true,
	}

	processor := NewJsonColumnProcessor(cfg)

	jsonValues := []string{
		`{"key": "value"}`,
		"",                // Empty string
		`{}`,              // Empty object
		`{"key": null}`,   // Null value
		`{"key": "val2"}`,
	}

	result := processor.ProcessBatch(jsonValues)

	if result.NumRows != 5 {
		t.Errorf("Expected 5 rows, got %d", result.NumRows)
	}

	// Check that key column exists and has correct values
	if subCol, ok := result.Subcolumns["data.key"]; ok {
		if len(subCol.Values) != 5 {
			t.Errorf("Expected 5 values, got %d", len(subCol.Values))
		}
	}
}

func TestProcessBatchFlattensAllDepths(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:        "data",
		MaxSubcolumns: 10,
		BucketCount:   2,
		MaxDepth:      1,
	}

	processor := NewJsonColumnProcessor(cfg)

	jsonValues := []string{
		`{"flat": "ok", "nested": {"value": "skip"}}`,
	}

	result := processor.ProcessBatch(jsonValues)

	if _, ok := result.Subcolumns["data.flat"]; !ok {
		t.Error("expected data.flat subcolumn")
	}
	if _, ok := result.Subcolumns["data.nested.value"]; !ok {
		t.Error("expected nested keys to be flattened regardless of depth")
	}
}

func TestProcessBatchDepthBuckets(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:        "data",
		MaxSubcolumns: 10,
		BucketCount:   2,
		MaxDepth:      10,
	}

	processor := NewJsonColumnProcessor(cfg)

	jsonValues := []string{
		`{"a":{"b":{"c":{"d":{"e":{"f":{"g":1}}}}}}}`,
	}

	result := processor.ProcessBatch(jsonValues)
	deepKey := "data.a.b.c.d.e.f.g"

	if _, ok := result.Subcolumns[deepKey]; ok {
		t.Error("expected deep key to be bucketed, not a subcolumn")
	}

	found := false
	for _, bucket := range result.BucketMaps {
		if len(bucket) == 0 || bucket[0] == nil {
			continue
		}
		if _, ok := bucket[0]["a.b.c.d.e.f.g"]; ok {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected deep key to be present in bucketed columns")
	}
}

func TestProcessBatchJsonKeys(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:        "data",
		MaxSubcolumns: 10,
		BucketCount:   2,
		MaxDepth:      3,
	}

	processor := NewJsonColumnProcessor(cfg)

	jsonValues := []string{
		`{"b":{"c":2},"a":1}`,
	}

	result := processor.ProcessBatch(jsonValues)
	if result.JsonKeys == nil || len(result.JsonKeys) != 1 {
		t.Fatalf("expected 1 json_keys row, got %#v", result.JsonKeys)
	}
	expected := []string{"a", "b.c"}
	got := result.JsonKeys[0]
	if len(got) != len(expected) {
		t.Fatalf("expected %d json keys, got %d", len(expected), len(got))
	}
	for i, key := range expected {
		if got[i] != key {
			t.Fatalf("expected json key %q at index %d, got %q", key, i, got[i])
		}
	}
}

func TestProcessBatchArrayEncoding(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:        "data",
		MaxSubcolumns: 10,
		BucketCount:   2,
		MaxDepth:      2,
	}

	processor := NewJsonColumnProcessor(cfg)

	jsonValues := []string{
		`{"arr":[1,2,3]}`,
	}

	result := processor.ProcessBatch(jsonValues)
	subCol, ok := result.Subcolumns["data.arr"]
	if !ok {
		t.Fatal("expected data.arr subcolumn")
	}
	if subCol.Type != TypeString {
		t.Fatalf("expected array to be encoded as string, got %v", subCol.Type)
	}
	if len(subCol.Values) != 1 {
		t.Fatalf("expected 1 value, got %d", len(subCol.Values))
	}
	if val, ok := subCol.Values[0].(string); !ok || val != "[1,2,3]" {
		t.Fatalf("unexpected array encoding: %#v", subCol.Values[0])
	}
}

func TestExpandRowJsonKeys(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:        "props",
		MaxSubcolumns: 5,
		BucketCount:   4,
		MaxDepth:      10,
	}

	processor := NewJsonColumnProcessor(cfg)

	// Build a plan first
	jsonValues := []string{
		`{"z":1,"a":2,"m":{"nested":3}}`,
	}
	plan := processor.BuildPlan(jsonValues)

	// Expand a single row
	result := processor.ExpandRow(plan, `{"z":1,"a":2,"m":{"nested":3}}`)
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// JsonKeys should be sorted alphabetically
	expected := []string{"a", "m.nested", "z"}
	if len(result.JsonKeys) != len(expected) {
		t.Fatalf("expected %d json keys, got %d: %v", len(expected), len(result.JsonKeys), result.JsonKeys)
	}
	for i, key := range expected {
		if result.JsonKeys[i] != key {
			t.Errorf("expected json key %q at index %d, got %q", key, i, result.JsonKeys[i])
		}
	}
}

func TestExpandRowSubcolumnsAndBuckets(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:        "data",
		MaxSubcolumns: 2, // Only 2 subcolumns
		BucketCount:   4,
		MaxDepth:      10,
	}

	processor := NewJsonColumnProcessor(cfg)

	// Build plan with multiple keys
	jsonValues := []string{
		`{"a":1,"b":2,"c":3,"d":4}`,
	}
	plan := processor.BuildPlan(jsonValues)

	// Expand a row
	result := processor.ExpandRow(plan, `{"a":1,"b":2,"c":3,"d":4}`)
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Should have at most 2 subcolumns
	if len(result.SubcolumnValues) > 2 {
		t.Errorf("expected at most 2 subcolumn values, got %d", len(result.SubcolumnValues))
	}

	// Overflow keys should be in buckets
	totalBucketed := 0
	for _, bucketMap := range result.BucketValues {
		totalBucketed += len(bucketMap)
	}
	if totalBucketed != 2 {
		t.Errorf("expected 2 bucketed values, got %d", totalBucketed)
	}
}

func TestExpandRowDeepKeysBucketed(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:        "data",
		MaxSubcolumns: 10,
		BucketCount:   4,
		MaxDepth:      10,
	}

	processor := NewJsonColumnProcessor(cfg)

	// Deep nested structure (depth > maxSubcolumnDepth=5)
	jsonValues := []string{
		`{"a":{"b":{"c":{"d":{"e":{"f":{"g":1}}}}}}}`,
	}
	plan := processor.BuildPlan(jsonValues)

	result := processor.ExpandRow(plan, `{"a":{"b":{"c":{"d":{"e":{"f":{"g":1}}}}}}}`)
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Deep key should NOT be in subcolumns
	deepKey := "data.a.b.c.d.e.f.g"
	if _, ok := result.SubcolumnValues[deepKey]; ok {
		t.Error("expected deep key to be bucketed, not in subcolumns")
	}

	// Deep key should be in a bucket
	found := false
	for _, bucketMap := range result.BucketValues {
		if _, ok := bucketMap["a.b.c.d.e.f.g"]; ok {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected deep key to be in bucket values")
	}

	// JsonKeys should still include the deep key
	hasDeepKey := false
	for _, key := range result.JsonKeys {
		if key == "a.b.c.d.e.f.g" {
			hasDeepKey = true
			break
		}
	}
	if !hasDeepKey {
		t.Errorf("expected JsonKeys to include deep key, got: %v", result.JsonKeys)
	}
}

func TestExpandRowEmptyAndInvalid(t *testing.T) {
	cfg := config.JsonColumnConfig{
		Column:        "data",
		MaxSubcolumns: 5,
		BucketCount:   4,
		MaxDepth:      10,
	}

	processor := NewJsonColumnProcessor(cfg)
	plan := processor.BuildPlan([]string{`{"a":1}`})

	// Empty string
	result := processor.ExpandRow(plan, "")
	if result != nil {
		t.Error("expected nil for empty string")
	}

	// Invalid JSON
	result = processor.ExpandRow(plan, "not json")
	if result != nil {
		t.Error("expected nil for invalid JSON")
	}

	// Non-object JSON
	result = processor.ExpandRow(plan, `[1,2,3]`)
	if result != nil {
		t.Error("expected nil for non-object JSON")
	}
}
