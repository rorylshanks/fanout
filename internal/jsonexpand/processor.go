package jsonexpand

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/spaolacci/murmur3"
	"fanout/internal/config"
)

const maxInt64 = int64(math.MaxInt64)

// InferredType represents the inferred type of a JSON value
type InferredType int

const (
	TypeNull InferredType = iota
	TypeBool
	TypeInt64
	TypeFloat64
	TypeString
)

// resolveTypeConflict finds a common type when types conflict
func resolveTypeConflict(a, b InferredType) InferredType {
	if a == TypeNull {
		return b
	}
	if b == TypeNull {
		return a
	}
	if a == b {
		return a
	}
	// Int64 and Float64 resolve to Float64
	if (a == TypeInt64 && b == TypeFloat64) || (a == TypeFloat64 && b == TypeInt64) {
		return TypeFloat64
	}
	// Everything else resolves to String
	return TypeString
}

// KeyStats tracks statistics for a key
type KeyStats struct {
	Count        int
	InferredType InferredType
}

// SubcolumnData holds data for a single subcolumn
type SubcolumnData struct {
	Name   string
	Type   InferredType
	Values []interface{} // nil for null, actual value otherwise
}

// BucketMapEntry holds a single key-value pair for bucketed maps
type BucketMapEntry struct {
	Key   string
	Value string
}

// ProcessedJsonColumns is the result of processing JSON columns for a batch
type ProcessedJsonColumns struct {
	ColumnName     string
	OriginalValues []string // if keep_original_column is true
	Subcolumns     map[string]*SubcolumnData
	BucketMaps     [][]map[string]interface{} // bucket_index -> row_index -> key -> value
	BucketCount    int
	NumRows        int
}

// JsonColumnPlan defines subcolumns and buckets without storing per-row values.
type JsonColumnPlan struct {
	ColumnName        string
	SubcolumnTypes    map[string]InferredType // full column name -> inferred type
	SubcolumnKeyTypes map[string]InferredType // raw key -> inferred type
	OverflowKeys      map[string]bool         // raw key -> overflow bucket
	BucketCount       int
}

// RowExpansion contains JSON subcolumn and bucket values for a single row.
type RowExpansion struct {
	SubcolumnValues map[string]interface{} // full column name -> value
	BucketValues    map[int]map[string]interface{}
}

// JsonColumnProcessor handles JSON column expansion
type JsonColumnProcessor struct {
	config config.JsonColumnConfig
}

// NewJsonColumnProcessor creates a new processor
func NewJsonColumnProcessor(cfg config.JsonColumnConfig) *JsonColumnProcessor {
	return &JsonColumnProcessor{config: cfg}
}

// ColumnName returns the column name this processor handles
func (p *JsonColumnProcessor) ColumnName() string {
	return p.config.Column
}

// BuildPlan determines subcolumns/buckets without storing per-row values.
func (p *JsonColumnProcessor) BuildPlan(jsonValues []string) *JsonColumnPlan {
	// Pass 1: Scan keys to determine schema
	keyStats := make(map[string]*KeyStats)

	for _, jsonStr := range jsonValues {
		if jsonStr == "" {
			continue
		}

		// Use json.Number to preserve large integers
		decoder := json.NewDecoder(strings.NewReader(jsonStr))
		decoder.UseNumber()
		var obj map[string]interface{}
		if err := decoder.Decode(&obj); err != nil {
			continue
		}

		flattened := make(map[string]interface{})
		p.flattenObject(obj, "", 0, flattened)

		for key, value := range flattened {
			inferredType := p.inferType(value)
			if stats, ok := keyStats[key]; ok {
				stats.Count++
				stats.InferredType = resolveTypeConflict(stats.InferredType, inferredType)
			} else {
				keyStats[key] = &KeyStats{
					Count:        1,
					InferredType: inferredType,
				}
			}
		}
	}

	// Sort keys by frequency to determine subcolumns vs overflow
	type keyFreq struct {
		key   string
		stats *KeyStats
	}
	sortedKeys := make([]keyFreq, 0, len(keyStats))
	for key, stats := range keyStats {
		sortedKeys = append(sortedKeys, keyFreq{key, stats})
	}
	sort.Slice(sortedKeys, func(i, j int) bool {
		if sortedKeys[i].stats.Count != sortedKeys[j].stats.Count {
			return sortedKeys[i].stats.Count > sortedKeys[j].stats.Count
		}
		return sortedKeys[i].key < sortedKeys[j].key
	})

	subcolumnKeyTypes := make(map[string]InferredType)
	overflowKeys := make(map[string]bool)
	subcolumnTypes := make(map[string]InferredType)

	for i, kf := range sortedKeys {
		if i < p.config.MaxSubcolumns {
			subcolumnKeyTypes[kf.key] = kf.stats.InferredType
			fullKey := p.config.Column + "." + kf.key
			subcolumnTypes[fullKey] = kf.stats.InferredType
		} else {
			overflowKeys[kf.key] = true
		}
	}

	return &JsonColumnPlan{
		ColumnName:        p.config.Column,
		SubcolumnTypes:    subcolumnTypes,
		SubcolumnKeyTypes: subcolumnKeyTypes,
		OverflowKeys:      overflowKeys,
		BucketCount:       p.config.BucketCount,
	}
}

// ExpandRow expands a JSON string into subcolumn and bucket values for one row.
func (p *JsonColumnProcessor) ExpandRow(plan *JsonColumnPlan, jsonStr string) *RowExpansion {
	if jsonStr == "" || plan == nil {
		return nil
	}

	decoder := json.NewDecoder(strings.NewReader(jsonStr))
	decoder.UseNumber()
	var obj map[string]interface{}
	if err := decoder.Decode(&obj); err != nil {
		return nil
	}

	flattened := make(map[string]interface{})
	p.flattenObject(obj, "", 0, flattened)

	subcolumnValues := make(map[string]interface{})
	bucketValues := make(map[int]map[string]interface{})

	for key, value := range flattened {
		if inferredType, ok := plan.SubcolumnKeyTypes[key]; ok {
			fullKey := plan.ColumnName + "." + key
			if converted := p.convertValue(value, inferredType); converted != nil {
				subcolumnValues[fullKey] = converted
			}
			continue
		}
		if plan.OverflowKeys[key] {
			bucketIdx := p.hashKeyToBucket(key)
			if bucketValues[bucketIdx] == nil {
				bucketValues[bucketIdx] = make(map[string]interface{})
			}
			bucketValues[bucketIdx][key] = value
		}
	}

	return &RowExpansion{
		SubcolumnValues: subcolumnValues,
		BucketValues:    bucketValues,
	}
}

// ProcessBatch processes a batch of JSON string values
func (p *JsonColumnProcessor) ProcessBatch(jsonValues []string) *ProcessedJsonColumns {
	numRows := len(jsonValues)

	// Pass 1: Scan keys to determine schema
	keyStats := make(map[string]*KeyStats)
	parsedRows := make([]map[string]interface{}, numRows)

	for rowIdx, jsonStr := range jsonValues {
		if jsonStr == "" {
			continue
		}

		// Use json.Number to preserve large integers
		decoder := json.NewDecoder(strings.NewReader(jsonStr))
		decoder.UseNumber()
		var obj map[string]interface{}
		if err := decoder.Decode(&obj); err != nil {
			continue
		}

		flattened := make(map[string]interface{})
		p.flattenObject(obj, "", 0, flattened)
		parsedRows[rowIdx] = flattened

		for key, value := range flattened {
			inferredType := p.inferType(value)
			if stats, ok := keyStats[key]; ok {
				stats.Count++
				stats.InferredType = resolveTypeConflict(stats.InferredType, inferredType)
			} else {
				keyStats[key] = &KeyStats{
					Count:        1,
					InferredType: inferredType,
				}
			}
		}
	}

	// Sort keys by frequency to determine subcolumns vs overflow
	type keyFreq struct {
		key   string
		stats *KeyStats
	}
	sortedKeys := make([]keyFreq, 0, len(keyStats))
	for key, stats := range keyStats {
		sortedKeys = append(sortedKeys, keyFreq{key, stats})
	}
	sort.Slice(sortedKeys, func(i, j int) bool {
		if sortedKeys[i].stats.Count != sortedKeys[j].stats.Count {
			return sortedKeys[i].stats.Count > sortedKeys[j].stats.Count
		}
		return sortedKeys[i].key < sortedKeys[j].key
	})

	// Determine which keys become subcolumns
	subcolumnKeys := make(map[string]bool)
	overflowKeys := make(map[string]bool)

	for i, kf := range sortedKeys {
		if i < p.config.MaxSubcolumns {
			subcolumnKeys[kf.key] = true
		} else {
			overflowKeys[kf.key] = true
		}
	}

	// Pass 2: Build subcolumns and bucket maps
	subcolumns := make(map[string]*SubcolumnData)
	for key := range subcolumnKeys {
		fullKey := p.config.Column + "." + key
		subcolumns[fullKey] = &SubcolumnData{
			Name:   fullKey,
			Type:   keyStats[key].InferredType,
			Values: make([]interface{}, numRows),
		}
	}

	// Initialize bucket maps
	bucketMaps := make([][]map[string]interface{}, p.config.BucketCount)
	for i := range bucketMaps {
		bucketMaps[i] = make([]map[string]interface{}, numRows)
	}

	// Original values
	var originalValues []string
	if p.config.KeepOriginalColumn {
		originalValues = make([]string, numRows)
		copy(originalValues, jsonValues)
	}

	// Populate subcolumns and bucket maps
	for rowIdx, flattened := range parsedRows {
		if flattened == nil {
			continue
		}

		for key, value := range flattened {
			if subcolumnKeys[key] {
				fullKey := p.config.Column + "." + key
				subcolumns[fullKey].Values[rowIdx] = p.convertValue(value, keyStats[key].InferredType)
			} else if overflowKeys[key] {
				bucketIdx := p.hashKeyToBucket(key)
				if bucketMaps[bucketIdx][rowIdx] == nil {
					bucketMaps[bucketIdx][rowIdx] = make(map[string]interface{})
				}
				bucketMaps[bucketIdx][rowIdx][key] = value
			}
		}
	}

	return &ProcessedJsonColumns{
		ColumnName:     p.config.Column,
		OriginalValues: originalValues,
		Subcolumns:     subcolumns,
		BucketMaps:     bucketMaps,
		BucketCount:    p.config.BucketCount,
		NumRows:        numRows,
	}
}

// flattenObject flattens a nested JSON object into dot-separated keys
func (p *JsonColumnProcessor) flattenObject(obj map[string]interface{}, prefix string, depth int, result map[string]interface{}) {
	if depth >= p.config.MaxDepth {
		return
	}

	for key, value := range obj {
		fullKey := key
		if prefix != "" {
			fullKey = prefix + "." + key
		}

		switch v := value.(type) {
		case map[string]interface{}:
			p.flattenObject(v, fullKey, depth+1, result)
		case []interface{}:
			// Arrays are serialized as JSON strings
			if jsonBytes, err := json.Marshal(v); err == nil {
				result[fullKey] = string(jsonBytes)
			}
		default:
			result[fullKey] = value
		}
	}
}

// inferType determines the type of a JSON value
func (p *JsonColumnProcessor) inferType(value interface{}) InferredType {
	if value == nil {
		return TypeNull
	}

	switch v := value.(type) {
	case bool:
		return TypeBool
	case json.Number:
		// Try to parse as int64 first
		if i, err := v.Int64(); err == nil {
			_ = i // valid int64
			return TypeInt64
		}
		// Check if it's a large integer (> max int64) - these become strings
		str := v.String()
		if !strings.Contains(str, ".") && !strings.Contains(str, "e") && !strings.Contains(str, "E") {
			// It's an integer but too large for int64, treat as string
			return TypeString
		}
		// It's a float
		return TypeFloat64
	case float64:
		// Check if it's actually an integer
		if v == float64(int64(v)) {
			return TypeInt64
		}
		return TypeFloat64
	case string:
		if v == "" {
			return TypeNull
		}
		return TypeString
	default:
		return TypeString
	}
}

// convertValue converts a value to the target type
func (p *JsonColumnProcessor) convertValue(value interface{}, targetType InferredType) interface{} {
	if value == nil {
		return nil
	}

	switch targetType {
	case TypeBool:
		if b, ok := value.(bool); ok {
			return b
		}
		return nil
	case TypeInt64:
		switch v := value.(type) {
		case json.Number:
			if i, err := v.Int64(); err == nil {
				return i
			}
			return nil
		case float64:
			return int64(v)
		case int64:
			return v
		case string:
			return nil // Can't convert string to int in this context
		}
		return nil
	case TypeFloat64:
		switch v := value.(type) {
		case json.Number:
			if f, err := v.Float64(); err == nil {
				return f
			}
			return nil
		case float64:
			return v
		case int64:
			return float64(v)
		}
		return nil
	case TypeString:
		return p.valueToString(value)
	default:
		return nil
	}
}

// valueToString converts any value to a string
func (p *JsonColumnProcessor) valueToString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case json.Number:
		// For large integers (> max int64), just return the string representation
		// This handles the case where uint64 values > int64 max need to be quoted
		str := v.String()
		if !strings.Contains(str, ".") && !strings.Contains(str, "e") && !strings.Contains(str, "E") {
			// It's an integer - check if it fits in int64
			if _, err := strconv.ParseInt(str, 10, 64); err != nil {
				// Doesn't fit in int64, return as string (this quotes large uint64s)
				return str
			}
		}
		return str
	case float64:
		if v == float64(int64(v)) {
			// It's actually an integer, format without decimal
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%v", v)
	default:
		return string(mustMarshal(v))
	}
}

func mustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

// hashKeyToBucket uses MurmurHash3 to assign a key to a bucket
func (p *JsonColumnProcessor) hashKeyToBucket(key string) int {
	h1, _ := murmur3.Sum128([]byte(key))
	return int(h1 % uint64(p.config.BucketCount))
}

// GetBucketColumnName returns the column name for a bucket index
func (p *JsonColumnProcessor) GetBucketColumnName(bucketIdx int) string {
	return p.config.Column + "__json_type_bucket_" + strings.Replace(
		strings.Replace(
			string(mustMarshal(bucketIdx)), "\"", "", -1),
		" ", "", -1)
}

// ProcessedJsonColumns methods

// GetSubcolumnNames returns sorted subcolumn names
func (pjc *ProcessedJsonColumns) GetSubcolumnNames() []string {
	names := make([]string, 0, len(pjc.Subcolumns))
	for name := range pjc.Subcolumns {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// GetBucketColumnName returns the bucket column name for a given bucket index
func (pjc *ProcessedJsonColumns) GetBucketColumnName(bucketIdx int) string {
	return pjc.ColumnName + "__json_type_bucket_" + string(rune('0'+bucketIdx/100)) +
		string(rune('0'+bucketIdx%100/10)) + string(rune('0'+bucketIdx%10))
}
