package parquet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"fanout/internal/buffer"
	"fanout/internal/config"
)

// BatchProcessor handles the sorted-split-batch mode
// It reads events from disk buffers, sorts them, and writes parquet files
type BatchProcessor struct {
	config        config.ParquetConfig
	writer        *Writer
	rowsPerFile   int
	sortingCols   []config.SortingColumn
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor(cfg config.ParquetConfig, bufferDir string) *BatchProcessor {
	return &BatchProcessor{
		config:      cfg,
		writer:      NewWriter(cfg, bufferDir),
		rowsPerFile: cfg.RowsPerFile,
		sortingCols: cfg.SortingColumns,
	}
}

// ProcessBuffer reads a disk buffer, sorts, and writes parquet files
// Returns paths of generated parquet files
func (p *BatchProcessor) ProcessBuffer(bufferPath string, outputDir string) ([]string, error) {
	// Read all events from buffer
	reader, err := buffer.NewDiskBufferReader(bufferPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open buffer: %w", err)
	}
	defer reader.Close()

	events, err := p.readEventsFromBuffer(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read events: %w", err)
	}

	if len(events) == 0 {
		return nil, nil
	}

	// Sort events if sorting columns are configured
	if len(p.sortingCols) > 0 {
		p.sortEvents(events)
	}

	// Split into files
	return p.writeParquetFiles(events, outputDir)
}

// readEventsFromBuffer reads all events from a disk buffer
func (p *BatchProcessor) readEventsFromBuffer(reader *buffer.DiskBufferReader) ([]map[string]interface{}, error) {
	rawEvents, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	events := make([]map[string]interface{}, 0, len(rawEvents))
	for _, raw := range rawEvents {
		var event map[string]interface{}
		if err := json.Unmarshal(raw, &event); err != nil {
			// Skip invalid JSON
			continue
		}
		events = append(events, event)
	}

	return events, nil
}

// sortEvents sorts events by the configured sorting columns
func (p *BatchProcessor) sortEvents(events []map[string]interface{}) {
	sort.SliceStable(events, func(i, j int) bool {
		for _, sc := range p.sortingCols {
			vi := events[i][sc.Column]
			vj := events[j][sc.Column]

			cmp := compareValues(vi, vj)
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

// writeParquetFiles splits events into files of rowsPerFile each
func (p *BatchProcessor) writeParquetFiles(events []map[string]interface{}, outputDir string) ([]string, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	var paths []string

	for i := 0; i < len(events); i += p.rowsPerFile {
		end := i + p.rowsPerFile
		if end > len(events) {
			end = len(events)
		}

		batch := events[i:end]
		filename := buildFilename()
		path := filepath.Join(outputDir, filename)

		if err := p.writeParquetFile(batch, path); err != nil {
			return paths, fmt.Errorf("failed to write parquet file %s: %w", path, err)
		}

		paths = append(paths, path)
	}

	return paths, nil
}

// writeParquetFile writes a single parquet file
func (p *BatchProcessor) writeParquetFile(events []map[string]interface{}, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return p.writer.WriteEvents(file, events)
}

// WriteToBuffer writes events to a byte buffer (for S3 upload)
func (p *BatchProcessor) WriteToBuffer(events []map[string]interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := p.writer.WriteEvents(&buf, events); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ProcessBufferToWriter processes a buffer and writes to an io.Writer
func (p *BatchProcessor) ProcessBufferToWriter(bufferPath string, w io.Writer) error {
	reader, err := buffer.NewDiskBufferReader(bufferPath)
	if err != nil {
		return fmt.Errorf("failed to open buffer: %w", err)
	}
	defer reader.Close()

	events, err := p.readEventsFromBuffer(reader)
	if err != nil {
		return fmt.Errorf("failed to read events: %w", err)
	}

	if len(events) == 0 {
		return nil
	}

	if len(p.sortingCols) > 0 {
		p.sortEvents(events)
	}

	return p.writer.WriteEvents(w, events)
}

// compareValues compares two values for sorting
func compareValues(a, b interface{}) int {
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
	case bool:
		if vb, ok := b.(bool); ok {
			if !va && vb {
				return -1
			}
			if va && !vb {
				return 1
			}
			return 0
		}
	}

	// Fall back to string comparison
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

// SortedSplitBatchWriter handles the complete sorted-split-batch workflow
type SortedSplitBatchWriter struct {
	processor   *BatchProcessor
	maxEvents   int
	outputDir   string
}

// NewSortedSplitBatchWriter creates a new sorted-split-batch writer
func NewSortedSplitBatchWriter(cfg config.ParquetConfig, maxEvents int, outputDir string, bufferDir string) *SortedSplitBatchWriter {
	return &SortedSplitBatchWriter{
		processor: NewBatchProcessor(cfg, bufferDir),
		maxEvents: maxEvents,
		outputDir: outputDir,
	}
}

// ProcessPartition processes all events for a partition
// Returns a function that yields parquet file paths
func (w *SortedSplitBatchWriter) ProcessPartition(bufferPath, partitionPath string) ([]string, error) {
	outputDir := filepath.Join(w.outputDir, partitionPath)
	return w.processor.ProcessBuffer(bufferPath, outputDir)
}
