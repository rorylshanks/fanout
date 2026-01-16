package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaultsAndSources(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yml")

	configYAML := `
sources:
  input:
    type: kafka
    topics: ["events"]
    bootstrap_servers: "broker1:9092"
    group_id: "group1"
sinks:
  out:
    type: aws_s3
    bucket: "test-bucket"
    region: "us-east-1"
    encoding:
      parquet:
        schema:
          event: {type: "utf8"}
        json_columns:
          - column: "properties"
`
	if err := os.WriteFile(cfgPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.DataDir != "/data/" {
		t.Fatalf("expected default DataDir, got %q", cfg.DataDir)
	}
	if cfg.MetricsPort != 8075 {
		t.Fatalf("expected default MetricsPort, got %d", cfg.MetricsPort)
	}

	sink := cfg.Sinks["out"]
	if sink.DiskBatching.MaxEvents != 100000 {
		t.Fatalf("expected default MaxEvents, got %d", sink.DiskBatching.MaxEvents)
	}
	if sink.DiskBatching.TimeoutSecs != 60 {
		t.Fatalf("expected default TimeoutSecs, got %d", sink.DiskBatching.TimeoutSecs)
	}
	if sink.DiskBatching.Backpressure.MaxPendingFlushes != 100 {
		t.Fatalf("expected default MaxPendingFlushes, got %d", sink.DiskBatching.Backpressure.MaxPendingFlushes)
	}
	if sink.Encoding.Parquet.RowsPerFile != 10000 {
		t.Fatalf("expected default RowsPerFile, got %d", sink.Encoding.Parquet.RowsPerFile)
	}
	if sink.Encoding.Parquet.MaxRowsPerRowGroup != 1000 {
		t.Fatalf("expected default MaxRowsPerRowGroup, got %d", sink.Encoding.Parquet.MaxRowsPerRowGroup)
	}
	if sink.Encoding.Parquet.PageBufferBytes != 64*1024 {
		t.Fatalf("expected default PageBufferBytes, got %d", sink.Encoding.Parquet.PageBufferBytes)
	}
	if sink.Encoding.Parquet.UseFileBufferPool == nil || !*sink.Encoding.Parquet.UseFileBufferPool {
		t.Fatalf("expected default UseFileBufferPool true")
	}

	if len(sink.Encoding.Parquet.JsonColumns) != 1 {
		t.Fatalf("expected 1 json column, got %d", len(sink.Encoding.Parquet.JsonColumns))
	}
	jc := sink.Encoding.Parquet.JsonColumns[0]
	if jc.MaxSubcolumns != 1024 || jc.BucketCount != 256 || jc.MaxDepth != 10 {
		t.Fatalf("expected default json column limits, got %+v", jc)
	}

	if _, name, err := cfg.GetKafkaSource(); err != nil || name != "input" {
		t.Fatalf("expected kafka source 'input', got name=%q err=%v", name, err)
	}
	if _, name, err := cfg.GetS3Sink(); err != nil || name != "out" {
		t.Fatalf("expected s3 sink 'out', got name=%q err=%v", name, err)
	}
	if _, _, err := cfg.GetFilesystemSink(); err == nil {
		t.Fatal("expected missing filesystem sink error")
	}
}

func TestParseBootstrapServers(t *testing.T) {
	servers := ParseBootstrapServers("host1:9092, host2:9092\nhost3:9092,,")
	if len(servers) != 3 {
		t.Fatalf("expected 3 servers, got %d", len(servers))
	}
	if servers[0] != "host1:9092" || servers[1] != "host2:9092" || servers[2] != "host3:9092" {
		t.Fatalf("unexpected servers: %v", servers)
	}
}
