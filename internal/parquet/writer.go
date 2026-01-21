package parquet

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"

	goparquet "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/valyala/fastjson"
	"fanout/internal/config"
	"fanout/internal/jsonexpand"
	"fanout/internal/logging"
)

// Writer writes events to Parquet format
type Writer struct {
	config         config.ParquetConfig
	jsonProcessors []*jsonexpand.JsonColumnProcessor
	schemaFields   []config.SchemaFieldWithName
	bufferDir      string
	stageRecorder  ParquetStageRecorder
}

// ParquetStageRecorder records wall/CPU time by parquet encode stage.
type ParquetStageRecorder func(stage string, wallNs int64, cpuNs int64)

// NewWriter creates a new Parquet writer
func NewWriter(cfg config.ParquetConfig, bufferDir string) *Writer {
	// Create JSON processors
	var processors []*jsonexpand.JsonColumnProcessor
	for _, jc := range cfg.JsonColumns {
		processors = append(processors, jsonexpand.NewJsonColumnProcessor(jc))
	}

	// Convert schema map to ordered list
	var schemaFields []config.SchemaFieldWithName
	for name, field := range cfg.Schema {
		schemaFields = append(schemaFields, config.SchemaFieldWithName{Name: name, Field: field})
	}
	sort.Slice(schemaFields, func(i, j int) bool {
		return schemaFields[i].Name < schemaFields[j].Name
	})

	return &Writer{
		config:         cfg,
		jsonProcessors: processors,
		schemaFields:   schemaFields,
		bufferDir:      bufferDir,
	}
}

// SetStageRecorder sets the optional parquet stage recorder.
func (w *Writer) SetStageRecorder(recorder ParquetStageRecorder) {
	w.stageRecorder = recorder
}

// DynamicRow is used for writing dynamic data to parquet
type DynamicRow map[string]interface{}

// columnInfo holds precomputed column information for fast access
type columnInfo struct {
	name         string
	fieldType    string
	isJsonSubcol bool   // true if this is an expanded JSON subcolumn
	isJsonKeys   bool   // true if this is the json_keys column for a JSON column
	jsonColName  string // parent JSON column name (for subcolumns)
	subcolName   string // subcolumn name within processed data
	bucketIdx    int    // bucket index (-1 if not a bucket column)
	skipBounds   bool   // true if min/max indexes should be skipped
}

// WriteEvents writes events to the given writer
func (w *Writer) WriteEvents(out io.Writer, events []map[string]interface{}) error {
	if len(events) == 0 {
		return nil
	}

	totalStart := time.Now()
	w.recordStage(ParquetStageFile, 0, 0)

	// Process JSON columns to determine subcolumns (only if configured)
	jsonStart := time.Now()
	jsonCPUStart := cpuTimeNs()
	var jsonPlans map[string]*jsonexpand.JsonColumnPlan
	jsonProcessors := make(map[string]*jsonexpand.JsonColumnProcessor, len(w.jsonProcessors))
	if len(w.jsonProcessors) > 0 {
		jsonPlans = make(map[string]*jsonexpand.JsonColumnPlan)
		for _, proc := range w.jsonProcessors {
			colName := proc.ColumnName()
			jsonProcessors[colName] = proc
			values := make([]string, len(events))
			for i, event := range events {
				if v, ok := event[colName]; ok {
					if s, ok := v.(string); ok {
						values[i] = s
					}
				}
			}
			jsonPlans[colName] = proc.BuildPlan(values)
		}
	}

	// Build column info list (pre-computed for fast access)
	columns := make([]columnInfo, 0, len(w.schemaFields)+32)

	for _, sf := range w.schemaFields {
		skipBounds := false
		if sf.Field.MinMaxIndex != nil && !*sf.Field.MinMaxIndex {
			skipBounds = true
		}
		columns = append(columns, columnInfo{
			name:      sf.Name,
			fieldType: sf.Field.Type,
			bucketIdx: -1,
			skipBounds: skipBounds,
		})
	}

	// Add JSON subcolumn info with pre-computed lookup info
	for _, plan := range jsonPlans {
		for subcolName := range plan.SubcolumnTypes {
			columns = append(columns, columnInfo{
				name:         subcolName,
				fieldType:    "utf8",
				isJsonSubcol: true,
				jsonColName:  plan.ColumnName,
				subcolName:   subcolName,
				bucketIdx:    -1,
			})
		}
		for bucketIdx := 0; bucketIdx < plan.BucketCount; bucketIdx++ {
			bucketColName := plan.ColumnName + fmt.Sprintf("__json_type_bucket_%d", bucketIdx)
			columns = append(columns, columnInfo{
				name:         bucketColName,
				fieldType:    "json",
				isJsonSubcol: true,
				jsonColName:  plan.ColumnName,
				bucketIdx:    bucketIdx,
			})
		}
		columns = append(columns, columnInfo{
			name:         plan.ColumnName + "__json_keys",
			fieldType:    "json",
			isJsonSubcol: true,
			isJsonKeys:   true,
			jsonColName:  plan.ColumnName,
			bucketIdx:    -1,
		})
	}

	// Sort columns by name
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].name < columns[j].name
	})
	jsonDuration := time.Since(jsonStart)
	jsonCPUDuration := cpuTimeNs() - jsonCPUStart
	w.recordStage(ParquetStageJSON, jsonDuration.Nanoseconds(), jsonCPUDuration)

	// Build schema and write directly - skip intermediate row map
	parquetStart := time.Now()
	err := w.writeParquetDirect(out, events, columns, jsonPlans, jsonProcessors)
	parquetDuration := time.Since(parquetStart)

	totalDuration := time.Since(totalStart)
	if len(events) >= 10000 && totalDuration > 100*time.Millisecond {
		logging.TraceLog("perf_write_events", map[string]interface{}{
			"rows":       len(events),
			"json_ms":    jsonDuration.Milliseconds(),
			"parquet_ms": parquetDuration.Milliseconds(),
			"total_ms":   totalDuration.Milliseconds(),
		})
	}

	return err
}

// ParseEvent decodes a JSON event and extracts only configured schema fields.
func (w *Writer) ParseEvent(raw []byte) (map[string]interface{}, error) {
	var p fastjson.Parser
	v, err := p.ParseBytes(raw)
	if err != nil {
		return nil, err
	}

	event := make(map[string]interface{}, len(w.schemaFields))
	for _, sf := range w.schemaFields {
		val := v.Get(sf.Name)
		if val == nil {
			continue
		}
		converted := w.valueFromFastjson(val, sf.Field.Type)
		if converted != nil {
			event[sf.Name] = converted
		}
	}

	return event, nil
}

func (w *Writer) valueFromFastjson(val *fastjson.Value, fieldType string) interface{} {
	switch fieldType {
	case "utf8", "string", "json":
		switch val.Type() {
		case fastjson.TypeString:
			return string(val.GetStringBytes())
		case fastjson.TypeNull:
			return nil
		default:
			return string(val.MarshalTo(nil))
		}
	case "int64":
		if val.Type() == fastjson.TypeNumber {
			if v, err := val.Int64(); err == nil {
				return v
			}
			if v, err := val.Float64(); err == nil {
				return int64(v)
			}
		}
		return nil
	case "int32":
		if val.Type() == fastjson.TypeNumber {
			if v, err := val.Int64(); err == nil {
				return int32(v)
			}
			if v, err := val.Float64(); err == nil {
				return int32(v)
			}
		}
		return nil
	case "float64", "double":
		if val.Type() == fastjson.TypeNumber {
			if v, err := val.Float64(); err == nil {
				return v
			}
		}
		return nil
	case "float32", "float":
		if val.Type() == fastjson.TypeNumber {
			if v, err := val.Float64(); err == nil {
				return float32(v)
			}
		}
		return nil
	case "bool", "boolean":
		if val.Type() == fastjson.TypeTrue {
			return true
		}
		if val.Type() == fastjson.TypeFalse {
			return false
		}
		return nil
	case "timestamp_ms":
		switch val.Type() {
		case fastjson.TypeNumber:
			if v, err := val.Int64(); err == nil {
				return v
			}
			if v, err := val.Float64(); err == nil {
				return int64(v)
			}
		case fastjson.TypeString:
			if t := w.parseTimestampString(string(val.GetStringBytes())); t != nil {
				return t.UnixMilli()
			}
		}
		return nil
	default:
		return nil
	}
}


// writeParquetDirect writes events directly to parquet without intermediate row map
func (w *Writer) writeParquetDirect(out io.Writer, events []map[string]interface{}, columns []columnInfo, jsonPlans map[string]*jsonexpand.JsonColumnPlan, jsonProcessors map[string]*jsonexpand.JsonColumnProcessor) error {
	totalStart := time.Now()

	// Build parquet schema
	schemaStart := time.Now()
	schemaCPUStart := cpuTimeNs()
	group := make(goparquet.Group)
	for _, col := range columns {
		group[col.name] = w.createNode(col.name, col.fieldType)
	}
	schema := goparquet.NewSchema("event", group)

	// Get schema column order (parquet-go may reorder)
	schemaColumnOrder := make([]columnInfo, 0, len(columns))
	colInfoMap := make(map[string]columnInfo, len(columns))
	for _, col := range columns {
		colInfoMap[col.name] = col
	}
	schemaCols := schema.Columns()
	for _, path := range schemaCols {
		if len(path) > 0 {
			colName := path[0]
			if col, ok := colInfoMap[colName]; ok {
				schemaColumnOrder = append(schemaColumnOrder, col)
			} else {
				// This should never happen - log error if it does
				logging.ErrorLog("schema_column_missing", map[string]interface{}{"path": path})
			}
		}
	}

	// Validate that we have the expected number of columns
	if len(schemaColumnOrder) != len(columns) {
		return fmt.Errorf("column count mismatch: schemaColumnOrder=%d, columns=%d, schemaCols=%d",
			len(schemaColumnOrder), len(columns), len(schemaCols))
	}
	schemaDuration := time.Since(schemaStart)
	schemaCPUDuration := cpuTimeNs() - schemaCPUStart
	w.recordStage(ParquetStageSchema, schemaDuration.Nanoseconds(), schemaCPUDuration)

	// Create writer with compression - write directly to output (parquet-go has its own buffering)
	writerStart := time.Now()
	writerCPUStart := cpuTimeNs()
	compressionCodec := w.getCompressionCodec()
	writerOptions := []goparquet.WriterOption{
		schema,
		goparquet.Compression(compressionCodec),
		// Disable data page statistics - saves significant metadata space (247KB for 993 columns)
		// and the stats are not useful for columns like bucket columns or JSON data
		goparquet.DataPageStatistics(false),
		// Use RLE dictionary encoding for much better compression of string columns
		// This is especially effective for columns with repeated values (e.g., feature flags)
		// Benchmark: 4.45MB -> 1.14MB (3.9x smaller, even smaller than ClickHouse output)
		goparquet.DefaultEncoding(&goparquet.RLEDictionary),
	}
	for _, col := range columns {
		if col.bucketIdx >= 0 || col.isJsonKeys || col.skipBounds {
			writerOptions = append(writerOptions, goparquet.SkipPageBounds(col.name))
		}
	}
	if w.config.MaxRowsPerRowGroup > 0 {
		writerOptions = append(writerOptions, goparquet.MaxRowsPerRowGroup(int64(w.config.MaxRowsPerRowGroup)))
	}
	if w.config.PageBufferBytes > 0 {
		writerOptions = append(writerOptions, goparquet.PageBufferSize(w.config.PageBufferBytes))
	}
	if w.config.UseFileBufferPool != nil && *w.config.UseFileBufferPool && len(events) > 1000 {
		bufferDir := w.bufferDir
		if bufferDir == "" {
			bufferDir = os.TempDir()
		}
		if err := os.MkdirAll(bufferDir, 0755); err != nil {
			return fmt.Errorf("failed to create parquet buffer dir: %w", err)
		}
		writerOptions = append(writerOptions,
			goparquet.ColumnPageBuffers(goparquet.NewFileBufferPool(bufferDir, "fanout-parquet-*.buf")),
		)
	}

	pw := goparquet.NewWriter(out, writerOptions...)
	writerCreateDuration := time.Since(writerStart)
	writerCPUCreateDuration := cpuTimeNs() - writerCPUStart
	w.recordStage(ParquetStageWriterInit, writerCreateDuration.Nanoseconds(), writerCPUCreateDuration)

	// Convert events directly to parquet rows (skip intermediate map)
	var convertDuration time.Duration
	var writeDuration time.Duration
	var convertCPUNs int64
	var writeCPUNs int64
	const rowChunkSize = 2048
	for start := 0; start < len(events); start += rowChunkSize {
		end := start + rowChunkSize
		if end > len(events) {
			end = len(events)
		}

		convertStart := time.Now()
		convertCPUStart := cpuTimeNs()
		parquetRows := make([]goparquet.Row, end-start)

		for rowOffset, event := range events[start:end] {
			var rowJson map[string]*jsonexpand.RowExpansion
			if len(jsonPlans) > 0 {
				rowJson = make(map[string]*jsonexpand.RowExpansion, len(jsonPlans))
				for colName, plan := range jsonPlans {
					if proc := jsonProcessors[colName]; proc != nil {
						if v, ok := event[colName]; ok {
							if s, ok := v.(string); ok && s != "" {
								rowJson[colName] = proc.ExpandRow(plan, s)
							}
						}
					}
				}
			}

			parquetRow := make([]goparquet.Value, len(schemaColumnOrder))
			for idx, col := range schemaColumnOrder {
				var val interface{}
				if !col.isJsonSubcol {
					// Regular column - direct lookup
					val = event[col.name]
				} else if col.isJsonKeys {
					if expanded := rowJson[col.jsonColName]; expanded != nil {
						val = expanded.JsonKeys
					}
				} else if col.bucketIdx >= 0 {
					// JSON bucket column
					if expanded := rowJson[col.jsonColName]; expanded != nil {
						if bucketData := expanded.BucketValues[col.bucketIdx]; bucketData != nil {
							if jsonBytes, err := json.Marshal(bucketData); err == nil {
								val = string(jsonBytes)
							}
						}
					}
				} else {
					// JSON subcolumn
					if expanded := rowJson[col.jsonColName]; expanded != nil {
						if subcolValue, ok := expanded.SubcolumnValues[col.subcolName]; ok && subcolValue != nil {
							val = fmt.Sprintf("%v", subcolValue)
						}
					}
				}
				parquetRow[idx] = w.toParquetValueFast(val, col.fieldType, idx)
			}
			parquetRows[rowOffset] = parquetRow
		}
		convertDuration += time.Since(convertStart)
		convertCPUNs += cpuTimeNs() - convertCPUStart

		writeStart := time.Now()
		writeCPUStart := cpuTimeNs()
		if _, err := pw.WriteRows(parquetRows); err != nil {
			pw.Close()
			return fmt.Errorf("failed to write rows: %w", err)
		}
		writeDuration += time.Since(writeStart)
		writeCPUNs += cpuTimeNs() - writeCPUStart
	}
	w.recordStage(ParquetStageRowConvert, convertDuration.Nanoseconds(), convertCPUNs)
	w.recordStage(ParquetStageRowWrite, writeDuration.Nanoseconds(), writeCPUNs)

	closeStart := time.Now()
	closeCPUStart := cpuTimeNs()
	if err := pw.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}
	closeDuration := time.Since(closeStart)
	closeCPUDuration := cpuTimeNs() - closeCPUStart
	w.recordStage(ParquetStageWriterClose, closeDuration.Nanoseconds(), closeCPUDuration)

	totalDuration := time.Since(totalStart)
	if len(events) >= 10000 && totalDuration > 100*time.Millisecond {
		logging.TraceLog("perf_write_parquet", map[string]interface{}{
			"rows":       len(events),
			"schema_ms":  schemaDuration.Milliseconds(),
			"writer_ms":  writerCreateDuration.Milliseconds(),
			"convert_ms": convertDuration.Milliseconds(),
			"write_ms":   writeDuration.Milliseconds(),
			"close_ms":   closeDuration.Milliseconds(),
			"total_ms":   totalDuration.Milliseconds(),
		})
	}

	return nil
}

func (w *Writer) recordStage(stage string, wallNs int64, cpuNs int64) {
	if w.stageRecorder == nil {
		return
	}
	w.stageRecorder(stage, wallNs, cpuNs)
}

func cpuTimeNs() int64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return timevalToNs(usage.Utime) + timevalToNs(usage.Stime)
}

func timevalToNs(tv syscall.Timeval) int64 {
	return int64(tv.Sec)*1e9 + int64(tv.Usec)*1e3
}

// toParquetValueFast converts a Go value to a parquet value with minimal overhead
// String columns use Optional schema, so empty strings are converted to NULL
// to avoid ClickHouse parquet reading bugs with empty strings.
// For Optional columns, non-null values need Level(0, 1, columnIndex) set.
func (w *Writer) toParquetValueFast(val interface{}, fieldType string, columnIndex int) goparquet.Value {
	switch fieldType {
	case "utf8", "string", "json":
		if val == nil {
			return goparquet.NullValue()
		}
		switch v := val.(type) {
		case string:
			if v == "" {
				return goparquet.NullValue()
			}
			return goparquet.ValueOf(v).Level(0, 1, columnIndex)
		default:
			b, _ := json.Marshal(val)
			s := string(b)
			if s == "" || s == "null" {
				return goparquet.NullValue()
			}
			return goparquet.ValueOf(s).Level(0, 1, columnIndex)
		}

	case "int64":
		if val == nil {
			return goparquet.ValueOf(int64(0))
		}
		switch v := val.(type) {
		case int64:
			return goparquet.ValueOf(v)
		case float64:
			return goparquet.ValueOf(int64(v))
		case int:
			return goparquet.ValueOf(int64(v))
		case int32:
			return goparquet.ValueOf(int64(v))
		}
		return goparquet.ValueOf(int64(0))

	case "int32":
		if val == nil {
			return goparquet.ValueOf(int32(0))
		}
		switch v := val.(type) {
		case int32:
			return goparquet.ValueOf(v)
		case int64:
			return goparquet.ValueOf(int32(v))
		case float64:
			return goparquet.ValueOf(int32(v))
		case int:
			return goparquet.ValueOf(int32(v))
		}
		return goparquet.ValueOf(int32(0))

	case "float64", "double":
		if val == nil {
			return goparquet.ValueOf(float64(0))
		}
		switch v := val.(type) {
		case float64:
			return goparquet.ValueOf(v)
		case int64:
			return goparquet.ValueOf(float64(v))
		case int:
			return goparquet.ValueOf(float64(v))
		}
		return goparquet.ValueOf(float64(0))

	case "float32", "float":
		if val == nil {
			return goparquet.ValueOf(float32(0))
		}
		switch v := val.(type) {
		case float32:
			return goparquet.ValueOf(v)
		case float64:
			return goparquet.ValueOf(float32(v))
		case int64:
			return goparquet.ValueOf(float32(v))
		}
		return goparquet.ValueOf(float32(0))

	case "bool", "boolean":
		if val == nil {
			return goparquet.ValueOf(false)
		}
		if b, ok := val.(bool); ok {
			return goparquet.ValueOf(b)
		}
		return goparquet.ValueOf(false)

	case "timestamp_ms":
		if val == nil {
			return goparquet.ValueOf(int64(0))
		}
		switch v := val.(type) {
		case time.Time:
			return goparquet.ValueOf(v.UnixMilli())
		case int64:
			return goparquet.ValueOf(v)
		case float64:
			return goparquet.ValueOf(int64(v))
		case string:
			if t := w.parseTimestampString(v); t != nil {
				return goparquet.ValueOf(t.UnixMilli())
			}
		}
		return goparquet.ValueOf(int64(0))

	case "timestamp_us":
		if val == nil {
			return goparquet.ValueOf(int64(0))
		}
		switch v := val.(type) {
		case time.Time:
			return goparquet.ValueOf(v.UnixMicro())
		case int64:
			return goparquet.ValueOf(v)
		case float64:
			return goparquet.ValueOf(int64(v))
		case string:
			if t := w.parseTimestampString(v); t != nil {
				return goparquet.ValueOf(t.UnixMicro())
			}
		}
		return goparquet.ValueOf(int64(0))

	case "date":
		if val == nil {
			return goparquet.ValueOf(int32(0))
		}
		switch v := val.(type) {
		case time.Time:
			return goparquet.ValueOf(int32(v.Unix() / 86400))
		}
		return goparquet.ValueOf(int32(0))

	default:
		if val == nil {
			return goparquet.ValueOf("")
		}
		if s, ok := val.(string); ok {
			return goparquet.ValueOf(s)
		}
		return goparquet.ValueOf("")
	}
}

// parseTimestampString parses a timestamp string
func (w *Writer) parseTimestampString(v string) *time.Time {
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05.000000Z",
		"2006-01-02 15:04:05.000000",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, v); err == nil {
			return &t
		}
	}
	return nil
}

// getCompressionCodec returns the configured compression codec
func (w *Writer) getCompressionCodec() compress.Codec {
	compression := strings.ToLower(w.config.Compression)
	level := w.config.CompressionLevel
	if level == 0 {
		level = 5 // Default level
	}

	switch compression {
	case "zstd":
		// Map compression level to zstd speed
		var zstdLevel zstd.Level
		switch {
		case level <= 1:
			zstdLevel = zstd.SpeedFastest
		case level <= 3:
			zstdLevel = zstd.SpeedDefault
		case level <= 6:
			zstdLevel = zstd.SpeedBetterCompression
		default:
			zstdLevel = zstd.SpeedBestCompression
		}
		return &zstd.Codec{Level: zstdLevel}
	case "gzip":
		return &gzip.Codec{Level: level}
	case "snappy":
		return &snappy.Codec{}
	case "uncompressed", "none", "":
		return nil
	default:
		// Default to zstd
		return &zstd.Codec{Level: zstd.SpeedDefault}
	}
}

// createNode creates a parquet node for a column
// String columns are Optional to allow NULL values (empty strings cause ClickHouse read issues)
func (w *Writer) createNode(name string, fieldType string) goparquet.Node {
	switch fieldType {
	case "utf8", "string", "json":
		return goparquet.Optional(goparquet.String())
	case "int64":
		return goparquet.Int(64)
	case "int32":
		return goparquet.Int(32)
	case "float64", "double":
		return goparquet.Leaf(goparquet.DoubleType)
	case "float32", "float":
		return goparquet.Leaf(goparquet.FloatType)
	case "bool", "boolean":
		return goparquet.Leaf(goparquet.BooleanType)
	case "timestamp_ms":
		return goparquet.Timestamp(goparquet.Millisecond)
	case "timestamp_us":
		return goparquet.Timestamp(goparquet.Microsecond)
	case "date":
		return goparquet.Date()
	default:
		return goparquet.String()
	}
}

// toParquetValue converts a Go value to a parquet value
// NOTE: parquet-go's Value.Level() has memory safety issues with byte arrays,
// so we force AllowNullableFields to false in createNode to avoid optional columns.
func (w *Writer) toParquetValue(val interface{}, fieldType string) goparquet.Value {
	if val == nil {
		return goparquet.NullValue()
	}

	switch fieldType {
	case "utf8", "string", "json":
		if s, ok := val.(string); ok {
			return goparquet.ValueOf(s)
		}
		b, _ := json.Marshal(val)
		return goparquet.ValueOf(string(b))

	case "int64":
		switch v := val.(type) {
		case int64:
			return goparquet.ValueOf(v)
		case float64:
			return goparquet.ValueOf(int64(v))
		case int:
			return goparquet.ValueOf(int64(v))
		}
		return goparquet.NullValue()

	case "int32":
		switch v := val.(type) {
		case int32:
			return goparquet.ValueOf(v)
		case int64:
			return goparquet.ValueOf(int32(v))
		case float64:
			return goparquet.ValueOf(int32(v))
		case int:
			return goparquet.ValueOf(int32(v))
		}
		return goparquet.NullValue()

	case "float64", "double":
		switch v := val.(type) {
		case float64:
			return goparquet.ValueOf(v)
		case int64:
			return goparquet.ValueOf(float64(v))
		case int:
			return goparquet.ValueOf(float64(v))
		}
		return goparquet.NullValue()

	case "float32", "float":
		switch v := val.(type) {
		case float32:
			return goparquet.ValueOf(v)
		case float64:
			return goparquet.ValueOf(float32(v))
		case int64:
			return goparquet.ValueOf(float32(v))
		}
		return goparquet.NullValue()

	case "bool", "boolean":
		if b, ok := val.(bool); ok {
			return goparquet.ValueOf(b)
		}
		return goparquet.NullValue()

	case "timestamp_ms", "timestamp_us":
		switch v := val.(type) {
		case time.Time:
			if fieldType == "timestamp_ms" {
				return goparquet.ValueOf(v.UnixMilli())
			}
			return goparquet.ValueOf(v.UnixMicro())
		case int64:
			return goparquet.ValueOf(v)
		}
		return goparquet.NullValue()

	case "date":
		switch v := val.(type) {
		case time.Time:
			// Days since Unix epoch
			days := int32(v.Unix() / 86400)
			return goparquet.ValueOf(days)
		}
		return goparquet.NullValue()

	default:
		if s, ok := val.(string); ok {
			return goparquet.ValueOf(s)
		}
		return goparquet.NullValue()
	}
}

// convertValue converts a value to the appropriate type
func (w *Writer) convertValue(key string, value interface{}, columnTypes map[string]string) interface{} {
	if value == nil {
		return nil
	}

	fieldType := columnTypes[key]

	switch fieldType {
	case "timestamp_ms", "timestamp_us":
		return w.parseTimestamp(value)
	case "int64":
		return w.parseInt64(value)
	case "int32":
		v := w.parseInt64(value)
		if v != nil {
			return int32(v.(int64))
		}
		return nil
	case "float64", "double":
		return w.parseFloat64(value)
	case "float32", "float":
		v := w.parseFloat64(value)
		if v != nil {
			return float32(v.(float64))
		}
		return nil
	case "bool", "boolean":
		return w.parseBool(value)
	default:
		if s, ok := value.(string); ok {
			return s
		}
		b, _ := json.Marshal(value)
		return string(b)
	}
}

func (w *Writer) parseTimestamp(value interface{}) interface{} {
	switch v := value.(type) {
	case time.Time:
		return v
	case string:
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05.000000Z",
			"2006-01-02 15:04:05.000000",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}
		return nil
	case float64:
		return time.UnixMilli(int64(v))
	case int64:
		return time.UnixMilli(v)
	}
	return nil
}

func (w *Writer) parseInt64(value interface{}) interface{} {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	}
	return nil
}

func (w *Writer) parseFloat64(value interface{}) interface{} {
	switch v := value.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	case int:
		return float64(v)
	}
	return nil
}

func (w *Writer) parseBool(value interface{}) interface{} {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "1"
	}
	return nil
}

// sortRows sorts rows by the configured sorting columns
func (w *Writer) sortRows(rows []map[string]interface{}) {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, sc := range w.config.SortingColumns {
			vi := rows[i][sc.Column]
			vj := rows[j][sc.Column]

			cmp := w.compareValues(vi, vj)
			if cmp == 0 {
				continue
			}

			if sc.Descending {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

func (w *Writer) compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch va := a.(type) {
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			}
			if va > vb {
				return 1
			}
			return 0
		}
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			}
			if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			}
			if va > vb {
				return 1
			}
			return 0
		}
	case time.Time:
		if vb, ok := b.(time.Time); ok {
			if va.Before(vb) {
				return -1
			}
			if va.After(vb) {
				return 1
			}
			return 0
		}
	}

	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}
