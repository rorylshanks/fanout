package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fanout/internal/buffer"
	"fanout/internal/kafka"
	"fanout/internal/metrics"
	"fanout/internal/partitioner"
	"github.com/valyala/fastjson"
)

func TestParseDynamicColumnsDefaults(t *testing.T) {
	// Empty config should add default columns
	columns, order := parseDynamicColumns(nil)

	// Should have 3 default columns
	if len(columns) != 3 {
		t.Fatalf("expected 3 default columns, got %d", len(columns))
	}

	// Check default column sources
	if columns["_timestamp"] != dynamicColumnKafkaTime {
		t.Errorf("expected _timestamp to be kafka_time, got %v", columns["_timestamp"])
	}
	if columns["_offset"] != dynamicColumnKafkaOffset {
		t.Errorf("expected _offset to be kafka_offset, got %v", columns["_offset"])
	}
	if columns["_partition"] != dynamicColumnKafkaPartition {
		t.Errorf("expected _partition to be kafka_partition, got %v", columns["_partition"])
	}

	// Check order: preferred columns first
	expectedOrder := []string{"_timestamp", "_offset", "_partition"}
	if !reflect.DeepEqual(order, expectedOrder) {
		t.Errorf("expected order %v, got %v", expectedOrder, order)
	}
}

func TestParseDynamicColumnsCustom(t *testing.T) {
	cfg := map[string]string{
		"inserted_at": "current_time",
		"kafka_ts":    "kafka_time",
		"msg_offset":  "kafka_offset",
		"part":        "kafka_partition",
	}

	columns, order := parseDynamicColumns(cfg)

	// Should have 4 custom + 3 defaults = 7, but defaults overlap check
	// _timestamp, _offset, _partition are added if not present
	// So we have: inserted_at, kafka_ts, msg_offset, part, _timestamp, _offset, _partition
	if len(columns) != 7 {
		t.Fatalf("expected 7 columns, got %d: %v", len(columns), columns)
	}

	// Check custom column sources
	if columns["inserted_at"] != dynamicColumnCurrentTime {
		t.Errorf("expected inserted_at to be current_time, got %v", columns["inserted_at"])
	}
	if columns["kafka_ts"] != dynamicColumnKafkaTime {
		t.Errorf("expected kafka_ts to be kafka_time, got %v", columns["kafka_ts"])
	}
	if columns["msg_offset"] != dynamicColumnKafkaOffset {
		t.Errorf("expected msg_offset to be kafka_offset, got %v", columns["msg_offset"])
	}
	if columns["part"] != dynamicColumnKafkaPartition {
		t.Errorf("expected part to be kafka_partition, got %v", columns["part"])
	}

	// Order: preferred first (_timestamp, _offset, _partition), then alphabetical
	expectedOrder := []string{"_timestamp", "_offset", "_partition", "inserted_at", "kafka_ts", "msg_offset", "part"}
	if !reflect.DeepEqual(order, expectedOrder) {
		t.Errorf("expected order %v, got %v", expectedOrder, order)
	}
}

func TestParseDynamicColumnsOverrideDefaults(t *testing.T) {
	// Override default column with different source
	cfg := map[string]string{
		"_timestamp": "current_time", // Override default kafka_time
	}

	columns, order := parseDynamicColumns(cfg)

	// Should have 3 columns (overridden _timestamp + defaults _offset, _partition)
	if len(columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(columns))
	}

	// _timestamp should be overridden to current_time
	if columns["_timestamp"] != dynamicColumnCurrentTime {
		t.Errorf("expected _timestamp to be current_time (overridden), got %v", columns["_timestamp"])
	}

	// Order should still have preferred columns first
	expectedOrder := []string{"_timestamp", "_offset", "_partition"}
	if !reflect.DeepEqual(order, expectedOrder) {
		t.Errorf("expected order %v, got %v", expectedOrder, order)
	}
}

func TestParseDynamicColumnsUnknownSource(t *testing.T) {
	cfg := map[string]string{
		"valid":   "kafka_time",
		"invalid": "unknown_source",
	}

	columns, order := parseDynamicColumns(cfg)

	// Invalid source should be skipped
	if _, ok := columns["invalid"]; ok {
		t.Error("expected invalid column to be skipped")
	}

	// Valid column should be present
	if columns["valid"] != dynamicColumnKafkaTime {
		t.Errorf("expected valid to be kafka_time, got %v", columns["valid"])
	}

	// Should have: valid + 3 defaults = 4
	if len(columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(columns))
	}

	// Order: preferred first, then alphabetical custom
	expectedOrder := []string{"_timestamp", "_offset", "_partition", "valid"}
	if !reflect.DeepEqual(order, expectedOrder) {
		t.Errorf("expected order %v, got %v", expectedOrder, order)
	}
}

func TestParseDynamicColumnsCaseInsensitive(t *testing.T) {
	cfg := map[string]string{
		"col1": "KAFKA_TIME",
		"col2": "Current_Time",
		"col3": "KAFKA_OFFSET",
		"col4": "Kafka_Partition",
	}

	columns, _ := parseDynamicColumns(cfg)

	if columns["col1"] != dynamicColumnKafkaTime {
		t.Errorf("expected col1 to be kafka_time, got %v", columns["col1"])
	}
	if columns["col2"] != dynamicColumnCurrentTime {
		t.Errorf("expected col2 to be current_time, got %v", columns["col2"])
	}
	if columns["col3"] != dynamicColumnKafkaOffset {
		t.Errorf("expected col3 to be kafka_offset, got %v", columns["col3"])
	}
	if columns["col4"] != dynamicColumnKafkaPartition {
		t.Errorf("expected col4 to be kafka_partition, got %v", columns["col4"])
	}
}

func TestProcessMessageDynamicColumns(t *testing.T) {
	// Create temp directory for buffer manager
	tmpDir := t.TempDir()

	// Create buffer manager with callback to capture output
	bufMgr := buffer.NewBufferManager(
		tmpDir,
		1000,            // maxEvents
		10*1024*1024,    // maxBytes
		60*1000*1000000, // maxAge (60s in ns)
		func(path string, buf *buffer.DiskBuffer, reason buffer.FlushReason) error {
			return nil
		},
	)
	defer bufMgr.Stop()

	// Create dynamic columns config
	columns, order := parseDynamicColumns(map[string]string{
		"inserted_at": "current_time",
	})

	// Create pipeline with real components
	p := &Pipeline{
		dynamicColumns:     columns,
		dynamicColumnOrder: order,
		partitioner:        partitioner.NewPathPartitioner("partition/"),
		bufferManager:      bufMgr,
		prom:               metrics.New("test"),
		parserPool: sync.Pool{
			New: func() interface{} {
				return &fastjson.Parser{}
			},
		},
		bufferPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, 1024))
			},
		},
	}

	msg := &kafka.Message{
		Partition: 5,
		Offset:    12345,
		Timestamp: 1704067200000, // 2024-01-01T00:00:00Z in milliseconds
		Value:     []byte(`{"event":"test","value":42}`),
	}

	// Get parser and call the real processMessage
	parser := p.parserPool.Get().(*fastjson.Parser)
	p.processMessage(parser, msg)
	p.parserPool.Put(parser)

	// Read back what was written to the buffer
	bufMgr.FlushAll()

	// Wait for flush to complete
	bufMgr.WaitForPendingFlushes(2 * time.Second)

	// Find the buffer file - structure is: tmpDir/buffers/<partition>/buffer_<ts>.bin
	files, _ := filepath.Glob(filepath.Join(tmpDir, "buffers", "*", "*.bin"))
	if len(files) == 0 {
		t.Fatal("expected buffer file to be created")
	}

	// Use the DiskBufferReader to read the binary format
	reader, err := buffer.NewDiskBufferReader(files[0])
	if err != nil {
		t.Fatalf("failed to open buffer reader: %v", err)
	}
	defer reader.Close()

	events, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("failed to read events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	result := string(events[0])

	// Parse as JSON to validate structure
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v\nOutput: %s", err, result)
	}

	// Verify dynamic columns are present
	if _, ok := parsed["_timestamp"]; !ok {
		t.Errorf("expected _timestamp in output, got: %s", result)
	}
	if parsed["_offset"] != float64(12345) {
		t.Errorf("expected _offset=12345, got %v", parsed["_offset"])
	}
	if parsed["_partition"] != float64(5) {
		t.Errorf("expected _partition=5, got %v", parsed["_partition"])
	}
	if _, ok := parsed["inserted_at"]; !ok {
		t.Errorf("expected inserted_at in output, got: %s", result)
	}

	// Verify original fields are preserved
	if parsed["event"] != "test" {
		t.Errorf("expected event=test, got %v", parsed["event"])
	}
	if parsed["value"] != float64(42) {
		t.Errorf("expected value=42, got %v", parsed["value"])
	}

	// Verify the dynamic columns come first (check ordering in raw JSON)
	timestampIdx := strings.Index(result, `"_timestamp"`)
	eventIdx := strings.Index(result, `"event"`)
	if timestampIdx > eventIdx {
		t.Errorf("expected _timestamp before event in JSON output")
	}
}

func TestGetPartitionPathFast(t *testing.T) {
	part := partitioner.NewPathPartitioner("team={{team_id}}/date={{date}}/event={{event}}/")
	p := &Pipeline{partitioner: part}

	var parser fastjson.Parser
	v, err := parser.Parse(`{"team_id":123,"event":"click","timestamp":"2024-01-02T03:04:05Z"}`)
	if err != nil {
		t.Fatalf("parse json: %v", err)
	}

	path := p.getPartitionPathFast(v)
	if path != "team=123/date=2024/01/02/event=click/" {
		t.Fatalf("unexpected partition path: %q", path)
	}
}
