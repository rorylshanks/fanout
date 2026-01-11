package sink

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"fanout/internal/config"
)

// Sink defines the interface for output destinations
type Sink interface {
	// Write writes data to the sink at the given path
	Write(ctx context.Context, path string, data []byte) error
	// WriteFromReader writes data from a reader to the sink
	WriteFromReader(ctx context.Context, path string, r io.Reader, size int64) error
	// Close closes the sink
	Close() error
}

// S3Sink writes parquet files to S3
type S3Sink struct {
	client     *s3.Client
	bucket     string
	keyPrefix  string
	region     string
	concurrency int
}

// NewS3Sink creates a new S3 sink
func NewS3Sink(cfg *config.Sink) (*S3Sink, error) {
	ctx := context.Background()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg)

	concurrency := cfg.Request.Concurrency
	if concurrency == 0 {
		concurrency = 64
	}

	return &S3Sink{
		client:      client,
		bucket:      cfg.Bucket,
		keyPrefix:   cfg.KeyPrefix,
		region:      cfg.Region,
		concurrency: concurrency,
	}, nil
}

// Write writes data to S3
func (s *S3Sink) Write(ctx context.Context, path string, data []byte) error {
	key := s.keyPrefix + path
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        &bytesReader{data: data},
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

// WriteFromReader writes data from a reader to S3
func (s *S3Sink) WriteFromReader(ctx context.Context, path string, r io.Reader, size int64) error {
	key := s.keyPrefix + path
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          r,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String("application/octet-stream"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	return nil
}

// Close closes the S3 sink
func (s *S3Sink) Close() error {
	return nil
}

// GetFullPath returns the full S3 path
func (s *S3Sink) GetFullPath(path string) string {
	key := s.keyPrefix + path
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}
	return fmt.Sprintf("s3://%s/%s", s.bucket, key)
}

// bytesReader implements io.Reader for a byte slice
type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// FilesystemSink writes parquet files to the local filesystem
type FilesystemSink struct {
	basePath string
}

// NewFilesystemSink creates a new filesystem sink
func NewFilesystemSink(cfg *config.Sink) (*FilesystemSink, error) {
	path := cfg.Path
	if path == "" {
		path = cfg.KeyPrefix // Use key_prefix as path if path not set
	}

	// Create base directory
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	return &FilesystemSink{
		basePath: path,
	}, nil
}

// Write writes data to the filesystem
func (f *FilesystemSink) Write(ctx context.Context, path string, data []byte) error {
	fullPath := filepath.Join(f.basePath, path)

	// Create directory if needed
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write file
	if err := os.WriteFile(fullPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// WriteFromReader writes data from a reader to the filesystem
func (f *FilesystemSink) WriteFromReader(ctx context.Context, path string, r io.Reader, size int64) error {
	fullPath := filepath.Join(f.basePath, path)

	// Create directory if needed
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create file
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Copy data
	if _, err := io.Copy(file, r); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// Close closes the filesystem sink
func (f *FilesystemSink) Close() error {
	return nil
}

// GetFullPath returns the full filesystem path
func (f *FilesystemSink) GetFullPath(path string) string {
	return filepath.Join(f.basePath, path)
}

// NewSink creates a sink based on configuration
func NewSink(cfg *config.Sink) (Sink, error) {
	switch cfg.Type {
	case "aws_s3":
		return NewS3Sink(cfg)
	case "filesystem":
		return NewFilesystemSink(cfg)
	default:
		return nil, fmt.Errorf("unsupported sink type: %s", cfg.Type)
	}
}

// GenerateFilename generates a unique filename for a parquet file
func GenerateFilename(extension string) string {
	ts := time.Now().UnixNano()
	return fmt.Sprintf("data_%d.%s", ts, extension)
}
