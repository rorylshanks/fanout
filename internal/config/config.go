package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the entire configuration file
type Config struct {
	DataDir string            `yaml:"data_dir"`
	Sources map[string]Source `yaml:"sources"`
	Sinks   map[string]Sink   `yaml:"sinks"`
}

// Source represents a Kafka source configuration
type Source struct {
	Type              string            `yaml:"type"`
	Topics            []string          `yaml:"topics"`
	BootstrapServers  string            `yaml:"bootstrap_servers"`
	GroupID           string            `yaml:"group_id"`
	AutoOffsetReset   string            `yaml:"auto_offset_reset"`
	TLS               TLSConfig         `yaml:"tls"`
	LibrdkafkaOptions map[string]string `yaml:"librdkafka_options"`
	ChannelBufferSize int               `yaml:"channel_buffer_size"`
	FetchDefaultBytes int               `yaml:"fetch_default_bytes"`
	FetchMaxBytes     int               `yaml:"fetch_max_bytes"`
	MaxProcessingMs   int               `yaml:"max_processing_time_ms"`
}

// TLSConfig represents TLS settings
type TLSConfig struct {
	Enabled bool `yaml:"enabled"`
}

// Sink represents an output sink configuration
type Sink struct {
	Type              string        `yaml:"type"`
	Inputs            []string      `yaml:"inputs"`
	Bucket            string        `yaml:"bucket"`
	Region            string        `yaml:"region"`
	KeyPrefix         string        `yaml:"key_prefix"`
	FilenameExtension string        `yaml:"filename_extension"`
	Compression       string        `yaml:"compression"`
	DiskBatching      DiskBatching  `yaml:"disk_batching"`
	Encoding          EncodingConfig `yaml:"encoding"`
	Request           RequestConfig `yaml:"request"`
	// For filesystem sink
	Path string `yaml:"path"`
}

// DiskBatching represents batch configuration
type DiskBatching struct {
	MaxEvents        int   `yaml:"max_events"`
	MaxBytes         int64 `yaml:"max_bytes"`
	TimeoutSecs      int   `yaml:"timeout_secs"`
	TimeoutSplaySecs int   `yaml:"timeout_splay_secs"` // Random splay +/- this value
	// Backpressure configuration
	Backpressure BackpressureConfig `yaml:"backpressure"`
}

func (d DiskBatching) Timeout() time.Duration {
	return time.Duration(d.TimeoutSecs) * time.Second
}

func (d DiskBatching) TimeoutSplay() time.Duration {
	return time.Duration(d.TimeoutSplaySecs) * time.Second
}

// BackpressureConfig configures backpressure and concurrency settings
type BackpressureConfig struct {
	// MaxPendingFlushes is the maximum flush jobs in queue before blocking writes
	// Default: 100
	MaxPendingFlushes int `yaml:"max_pending_flushes"`
	// MaxConcurrentFlushes is the number of parallel flush workers
	// Default: 4
	MaxConcurrentFlushes int `yaml:"max_concurrent_flushes"`
	// MaxOpenFiles is the LRU cache size for open buffer file handles
	// Default: 256
	MaxOpenFiles int `yaml:"max_open_files"`
	// MaxTotalBytes is the maximum bytes buffered before backpressure kicks in
	// Default: 50GB
	MaxTotalBytes int64 `yaml:"max_total_bytes"`
}

// EncodingConfig represents encoding settings
type EncodingConfig struct {
	Codec   string        `yaml:"codec"`
	Parquet ParquetConfig `yaml:"parquet"`
}

// ParquetConfig represents parquet-specific settings
type ParquetConfig struct {
	Compression             string                    `yaml:"compression"`
	CompressionLevel        int                       `yaml:"compression_level"`
	RowsPerFile             int                       `yaml:"rows_per_file"`
	MaxRowsPerRowGroup      int                       `yaml:"max_rows_per_row_group"`
	PageBufferBytes         int                       `yaml:"page_buffer_bytes"`
	UseFileBufferPool       *bool                     `yaml:"use_file_buffer_pool"`
	AllowNullableFields     bool                      `yaml:"allow_nullable_fields"`
	UseMemoryMappedFiles    bool                      `yaml:"use_memory_mapped_files"`
	SortingColumns          []SortingColumn           `yaml:"sorting_columns"`
	JsonColumns             []JsonColumnConfig        `yaml:"json_columns"`
	Schema                  map[string]SchemaField    `yaml:"schema"`
}

// SortingColumn represents a column to sort by
type SortingColumn struct {
	Column     string `yaml:"column"`
	Descending bool   `yaml:"descending"`
}

// JsonColumnConfig represents configuration for JSON column expansion
type JsonColumnConfig struct {
	Column             string `yaml:"column"`
	MaxSubcolumns      int    `yaml:"max_subcolumns"`
	BucketCount        int    `yaml:"bucket_count"`
	MaxDepth           int    `yaml:"max_depth"`
	KeepOriginalColumn bool   `yaml:"keep_original_column"`
}

// SchemaField represents a field in the schema
type SchemaField struct {
	Type                         string `yaml:"type"`
	BloomFilter                  bool   `yaml:"bloom_filter"`
	BloomFilterNumDistinctValues int    `yaml:"bloom_filter_num_distinct_values"`
}

// SchemaFieldWithName wraps a schema field with its name
type SchemaFieldWithName struct {
	Name  string
	Field SchemaField
}

// RequestConfig represents request/retry settings
type RequestConfig struct {
	Concurrency          int `yaml:"concurrency"`
	InFlightLimit        int `yaml:"in_flight_limit"`
	RetryInitialBackoff  int `yaml:"retry_initial_backoff_secs"`
	RetryMaxDuration     int `yaml:"retry_max_duration_secs"`
}

// Load reads and parses a configuration file
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	if cfg.DataDir == "" {
		cfg.DataDir = "/data/"
	}

	for name, sink := range cfg.Sinks {
		if sink.DiskBatching.MaxEvents == 0 {
			sink.DiskBatching.MaxEvents = 100000
		}
		if sink.DiskBatching.MaxBytes == 0 {
			sink.DiskBatching.MaxBytes = 10 * 1024 * 1024 * 1024 // 10GB per buffer
		}
		if sink.DiskBatching.TimeoutSecs == 0 {
			sink.DiskBatching.TimeoutSecs = 60
		}
		// Backpressure defaults
		if sink.DiskBatching.Backpressure.MaxPendingFlushes == 0 {
			sink.DiskBatching.Backpressure.MaxPendingFlushes = 100
		}
		if sink.DiskBatching.Backpressure.MaxConcurrentFlushes == 0 {
			sink.DiskBatching.Backpressure.MaxConcurrentFlushes = 4
		}
		if sink.DiskBatching.Backpressure.MaxOpenFiles == 0 {
			sink.DiskBatching.Backpressure.MaxOpenFiles = 256
		}
		if sink.DiskBatching.Backpressure.MaxTotalBytes == 0 {
			sink.DiskBatching.Backpressure.MaxTotalBytes = 50 * 1024 * 1024 * 1024 // 50GB total
		}
		if sink.Encoding.Parquet.RowsPerFile == 0 {
			sink.Encoding.Parquet.RowsPerFile = 10000
		}
		if sink.Encoding.Parquet.MaxRowsPerRowGroup == 0 {
			sink.Encoding.Parquet.MaxRowsPerRowGroup = 1000
		}
		if sink.Encoding.Parquet.PageBufferBytes == 0 {
			sink.Encoding.Parquet.PageBufferBytes = 64 * 1024
		}
		if sink.Encoding.Parquet.UseFileBufferPool == nil {
			enabled := true
			sink.Encoding.Parquet.UseFileBufferPool = &enabled
		}
		if sink.Encoding.Parquet.CompressionLevel == 0 {
			sink.Encoding.Parquet.CompressionLevel = 5
		}
		for i := range sink.Encoding.Parquet.JsonColumns {
			jc := &sink.Encoding.Parquet.JsonColumns[i]
			if jc.MaxSubcolumns == 0 {
				jc.MaxSubcolumns = 1024
			}
			if jc.BucketCount == 0 {
				jc.BucketCount = 256
			}
			if jc.MaxDepth == 0 {
				jc.MaxDepth = 10
			}
		}
		cfg.Sinks[name] = sink
	}

	return &cfg, nil
}

// GetKafkaSource returns the first Kafka source found in config
func (c *Config) GetKafkaSource() (*Source, string, error) {
	for name, src := range c.Sources {
		if src.Type == "kafka" {
			return &src, name, nil
		}
	}
	return nil, "", fmt.Errorf("no kafka source found in config")
}

// GetS3Sink returns the first S3 sink found in config
func (c *Config) GetS3Sink() (*Sink, string, error) {
	for name, sink := range c.Sinks {
		if sink.Type == "aws_s3" {
			return &sink, name, nil
		}
	}
	return nil, "", fmt.Errorf("no S3 sink found in config")
}

// GetFilesystemSink returns the first filesystem sink found in config
func (c *Config) GetFilesystemSink() (*Sink, string, error) {
	for name, sink := range c.Sinks {
		if sink.Type == "filesystem" {
			return &sink, name, nil
		}
	}
	return nil, "", fmt.Errorf("no filesystem sink found in config")
}

// ParseBootstrapServers splits the bootstrap servers string
func ParseBootstrapServers(servers string) []string {
	servers = strings.TrimSpace(servers)
	servers = strings.ReplaceAll(servers, "\n", ",")
	parts := strings.Split(servers, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
