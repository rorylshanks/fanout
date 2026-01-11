package parquet

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/tidwall/gjson"
	"fanout/internal/config"
	"fanout/internal/logging"
)

// StreamingProcessor handles parquet writing with minimal memory usage
// It reads events in chunks, sorts externally, and writes 10k row files
type StreamingProcessor struct {
	config      config.ParquetConfig
	writer      *Writer
	rowsPerFile int
	sortingCols []config.SortingColumn
	tempDir     string
}

// ParquetFileInfo contains information about a written parquet file
type ParquetFileInfo struct {
	Path     string
	RowCount int
}

// NewStreamingProcessor creates a streaming processor
func NewStreamingProcessor(cfg config.ParquetConfig, tempDir string) *StreamingProcessor {
	rowsPerFile := cfg.RowsPerFile
	if rowsPerFile <= 0 {
		rowsPerFile = 10000
	}
	return &StreamingProcessor{
		config:      cfg,
		writer:      NewWriter(cfg),
		rowsPerFile: rowsPerFile,
		sortingCols: cfg.SortingColumns,
		tempDir:     tempDir,
	}
}

// ProcessBufferToFiles reads a disk buffer and writes multiple parquet files
// Returns information about each generated file including row counts
func (p *StreamingProcessor) ProcessBufferToFiles(bufferPath string, outputDir string) ([]ParquetFileInfo, error) {
	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output dir: %w", err)
	}

	// If sorting is required, use external sort
	if len(p.sortingCols) > 0 {
		return p.processWithSort(bufferPath, outputDir)
	}

	// No sorting - stream directly
	return p.processWithoutSort(bufferPath, outputDir)
}

// processWithoutSort streams directly from buffer to parquet files
func (p *StreamingProcessor) processWithoutSort(bufferPath string, outputDir string) ([]ParquetFileInfo, error) {
	file, err := os.Open(bufferPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open buffer: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 256*1024)
	var files []ParquetFileInfo
	var batch []map[string]interface{}

	for {
		// Read length prefix
		var lenBuf [4]byte
		_, err := io.ReadFull(reader, lenBuf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return files, fmt.Errorf("failed to read length: %w", err)
		}

		length := binary.BigEndian.Uint32(lenBuf[:])
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return files, fmt.Errorf("failed to read data: %w", err)
		}

		// Parse JSON
		var event map[string]interface{}
		if err := json.Unmarshal(data, &event); err != nil {
			continue // Skip invalid JSON
		}

		batch = append(batch, event)

		// Write batch when full
		if len(batch) >= p.rowsPerFile {
			path := filepath.Join(outputDir, buildFilename())
			if err := p.writeBatchToParquet(batch, path); err != nil {
				return files, err
			}
			files = append(files, ParquetFileInfo{Path: path, RowCount: len(batch)})
			batch = batch[:0] // Reset batch, reuse memory
		}
	}

	// Write remaining events
	if len(batch) > 0 {
		path := filepath.Join(outputDir, buildFilename())
		if err := p.writeBatchToParquet(batch, path); err != nil {
			return files, err
		}
		files = append(files, ParquetFileInfo{Path: path, RowCount: len(batch)})
	}

	return files, nil
}

// processWithSort uses external merge sort for large datasets
func (p *StreamingProcessor) processWithSort(bufferPath string, outputDir string) ([]ParquetFileInfo, error) {
	// Create a unique temp directory for this specific flush operation
	// This prevents concurrent flushes from overwriting each other's chunk files
	flushChunkDir := filepath.Join(outputDir, "chunks")
	if err := os.MkdirAll(flushChunkDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create chunk dir: %w", err)
	}
	defer os.RemoveAll(flushChunkDir) // Clean up chunk files when done

	// Phase 1: Read chunks, sort each, write to temp files
	chunkSize := p.rowsPerFile * 2 // Sort 2x batch size at a time
	phase1Start := time.Now()
	tempFiles, totalEvents, err := p.createSortedChunks(bufferPath, chunkSize, flushChunkDir)
	phase1Duration := time.Since(phase1Start)
	if err != nil {
		p.cleanupTempFiles(tempFiles)
		return nil, err
	}
	defer p.cleanupTempFiles(tempFiles)

	if totalEvents == 0 {
		return nil, nil
	}

	// Phase 2: Merge sorted chunks and write final parquet files
	phase2Start := time.Now()
	result, err := p.mergeSortedChunks(tempFiles, outputDir, totalEvents)
	phase2Duration := time.Since(phase2Start)

	if totalEvents > 10000 {
		logging.TraceLog("perf_process_with_sort", map[string]interface{}{
			"events":     totalEvents,
			"phase1_sec": phase1Duration.Seconds(),
			"phase2_sec": phase2Duration.Seconds(),
		})
	}

	return result, err
}

// sortedChunk represents a temporary sorted chunk file
type sortedChunk struct {
	path     string
	file     *os.File
	reader   *bufio.Reader
	nextRaw  []byte
	nextKeys []interface{}
	done     bool
}

type sortEvent struct {
	raw  []byte
	keys []interface{}
}

// createSortedChunks reads the buffer, sorts chunks, writes temp files
func (p *StreamingProcessor) createSortedChunks(bufferPath string, chunkSize int, chunkDir string) ([]*sortedChunk, int, error) {
	file, err := os.Open(bufferPath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open buffer: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 256*1024)
	var chunks []*sortedChunk
	var batch []sortEvent
	totalEvents := 0

	for {
		// Read length prefix
		var lenBuf [4]byte
		_, err := io.ReadFull(reader, lenBuf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return chunks, totalEvents, fmt.Errorf("failed to read length: %w", err)
		}

		length := binary.BigEndian.Uint32(lenBuf[:])
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return chunks, totalEvents, fmt.Errorf("failed to read data: %w", err)
		}

		if !gjson.ValidBytes(data) {
			continue
		}

		batch = append(batch, sortEvent{
			raw:  data,
			keys: p.extractSortKeys(data),
		})
		totalEvents++

		// Sort and write chunk when full
		if len(batch) >= chunkSize {
			chunk, err := p.writeSortedChunk(batch, len(chunks), chunkDir)
			if err != nil {
				return chunks, totalEvents, err
			}
			chunks = append(chunks, chunk)
			batch = make([]sortEvent, 0, chunkSize)
		}
	}

	// Write remaining chunk
	if len(batch) > 0 {
		chunk, err := p.writeSortedChunk(batch, len(chunks), chunkDir)
		if err != nil {
			return chunks, totalEvents, err
		}
		chunks = append(chunks, chunk)
	}

	return chunks, totalEvents, nil
}

// writeSortedChunk sorts events and writes to a temp file
func (p *StreamingProcessor) writeSortedChunk(events []sortEvent, chunkNum int, chunkDir string) (*sortedChunk, error) {
	// Sort in place
	sort.SliceStable(events, func(i, j int) bool {
		return p.compareSortKeys(events[i].keys, events[j].keys) < 0
	})

	// Write to temp file with length-prefixed JSON
	tempPath := filepath.Join(chunkDir, fmt.Sprintf("chunk_%05d.tmp", chunkNum))
	file, err := os.Create(tempPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	writer := bufio.NewWriterSize(file, 128*1024)
	for _, event := range events {
		// Write length-prefixed
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(event.raw)))
		if _, err := writer.Write(lenBuf[:]); err != nil {
			file.Close()
			return nil, err
		}
		if _, err := writer.Write(event.raw); err != nil {
			file.Close()
			return nil, err
		}
	}

	if err := writer.Flush(); err != nil {
		file.Close()
		return nil, err
	}
	file.Close()

	return &sortedChunk{path: tempPath}, nil
}

// mergeSortedChunks performs k-way merge and writes parquet files
func (p *StreamingProcessor) mergeSortedChunks(chunks []*sortedChunk, outputDir string, totalEvents int) ([]ParquetFileInfo, error) {
	mergeStart := time.Now()
	var totalReadTime, totalWriteTime time.Duration

	// Open all chunk files for reading
	for _, chunk := range chunks {
		file, err := os.Open(chunk.path)
		if err != nil {
			logging.ErrorLog("chunk_open_failed", map[string]interface{}{"error": err.Error(), "path": chunk.path})
			return nil, err
		}
		chunk.file = file
		chunk.reader = bufio.NewReaderSize(file, 64*1024)

		// Read first event from each chunk
		if err := p.readNextEvent(chunk); err != nil && err != io.EOF {
			return nil, err
		}
	}
	defer func() {
		for _, chunk := range chunks {
			if chunk.file != nil {
				chunk.file.Close()
			}
		}
	}()

	// Build min-heap for merge
	h := &eventHeap{chunks: chunks, processor: p}
	heap.Init(h)

	// Merge and write parquet files
	var files []ParquetFileInfo
	var batchRaw [][]byte

	for h.Len() > 0 {
		// Get minimum event
		readStart := time.Now()
		minChunk := heap.Pop(h).(*sortedChunk)
		batchRaw = append(batchRaw, minChunk.nextRaw)

		// Read next event from this chunk
		if err := p.readNextEvent(minChunk); err != nil && err != io.EOF {
			return files, err
		}
		if !minChunk.done {
			heap.Push(h, minChunk)
		}
		totalReadTime += time.Since(readStart)

		// Write batch when full
		if len(batchRaw) >= p.rowsPerFile {
			writeStart := time.Now()
			path := filepath.Join(outputDir, buildFilename())
			events, err := p.parseBatchEvents(batchRaw)
			if err != nil {
				return files, err
			}
			if err := p.writeBatchToParquet(events, path); err != nil {
				return files, err
			}
			totalWriteTime += time.Since(writeStart)
			files = append(files, ParquetFileInfo{Path: path, RowCount: len(batchRaw)})
			batchRaw = batchRaw[:0]
		}
	}

	// Write remaining batch
	if len(batchRaw) > 0 {
		writeStart := time.Now()
		path := filepath.Join(outputDir, buildFilename())
		events, err := p.parseBatchEvents(batchRaw)
		if err != nil {
			return files, err
		}
		if err := p.writeBatchToParquet(events, path); err != nil {
			return files, err
		}
		totalWriteTime += time.Since(writeStart)
		files = append(files, ParquetFileInfo{Path: path, RowCount: len(batchRaw)})
	}

	if totalEvents > 10000 {
		logging.TraceLog("perf_merge_sorted_chunks", map[string]interface{}{
			"events":    totalEvents,
			"files":     len(files),
			"read_sec":  totalReadTime.Seconds(),
			"write_sec": totalWriteTime.Seconds(),
			"total_sec": time.Since(mergeStart).Seconds(),
		})
	}

	return files, nil
}

// readNextEvent reads the next event from a chunk
func (p *StreamingProcessor) readNextEvent(chunk *sortedChunk) error {
	var lenBuf [4]byte
	_, err := io.ReadFull(chunk.reader, lenBuf[:])
	if err == io.EOF {
		chunk.done = true
		return io.EOF
	}
	if err != nil {
		return err
	}

	length := binary.BigEndian.Uint32(lenBuf[:])
	data := make([]byte, length)
	if _, err := io.ReadFull(chunk.reader, data); err != nil {
		return err
	}

	if !gjson.ValidBytes(data) {
		return p.readNextEvent(chunk)
	}

	chunk.nextRaw = data
	chunk.nextKeys = p.extractSortKeys(data)
	return nil
}

// eventHeap implements heap.Interface for k-way merge
type eventHeap struct {
	chunks    []*sortedChunk
	processor *StreamingProcessor
}

func (h *eventHeap) Len() int { return len(h.chunks) }

func (h *eventHeap) Less(i, j int) bool {
	return h.processor.compareSortKeys(h.chunks[i].nextKeys, h.chunks[j].nextKeys) < 0
}

func (h *eventHeap) Swap(i, j int) {
	h.chunks[i], h.chunks[j] = h.chunks[j], h.chunks[i]
}

func (h *eventHeap) Push(x interface{}) {
	h.chunks = append(h.chunks, x.(*sortedChunk))
}

func (h *eventHeap) Pop() interface{} {
	old := h.chunks
	n := len(old)
	x := old[n-1]
	h.chunks = old[0 : n-1]
	return x
}

// compareEvents compares two events based on sorting columns
func (p *StreamingProcessor) compareSortKeys(a, b []interface{}) int {
	for idx, sc := range p.sortingCols {
		var va interface{}
		var vb interface{}
		if idx < len(a) {
			va = a[idx]
		}
		if idx < len(b) {
			vb = b[idx]
		}

		cmp := compareValues(va, vb)
		if cmp == 0 {
			continue
		}

		if sc.Descending {
			return -cmp
		}
		return cmp
	}
	return 0
}

func (p *StreamingProcessor) extractSortKeys(data []byte) []interface{} {
	keys := make([]interface{}, len(p.sortingCols))
	for idx, sc := range p.sortingCols {
		res := gjson.GetBytes(data, sc.Column)
		if !res.Exists() {
			keys[idx] = nil
			continue
		}
		switch res.Type {
		case gjson.String:
			keys[idx] = res.String()
		case gjson.Number:
			keys[idx] = res.Float()
		case gjson.True, gjson.False:
			keys[idx] = res.Bool()
		case gjson.Null:
			keys[idx] = nil
		default:
			keys[idx] = res.String()
		}
	}
	return keys
}

func (p *StreamingProcessor) parseBatchEvents(batchRaw [][]byte) ([]map[string]interface{}, error) {
	events := make([]map[string]interface{}, 0, len(batchRaw))
	for _, raw := range batchRaw {
		var event map[string]interface{}
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

// writeBatchToParquet writes a batch of events to a parquet file atomically
// It writes to a temp file first, then renames to prevent partial/corrupted files
func (p *StreamingProcessor) writeBatchToParquet(events []map[string]interface{}, path string) error {
	if len(events) == 0 {
		return nil
	}

	// Write to temp file first
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		logging.ErrorLog("parquet_temp_create_failed", map[string]interface{}{"error": err.Error(), "path": tempPath})
		return err
	}

	// Write the parquet data
	if err := p.writer.WriteEvents(file, events); err != nil {
		file.Close()
		os.Remove(tempPath)
		logging.ErrorLog("parquet_write_failed", map[string]interface{}{"error": err.Error(), "path": tempPath})
		return err
	}

	// Sync to disk to ensure data is written
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tempPath)
		logging.ErrorLog("parquet_sync_failed", map[string]interface{}{"error": err.Error(), "path": tempPath})
		return err
	}

	if err := file.Close(); err != nil {
		os.Remove(tempPath)
		logging.ErrorLog("parquet_close_failed", map[string]interface{}{"error": err.Error(), "path": tempPath})
		return err
	}

	// Atomically rename to final path
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		logging.ErrorLog("parquet_rename_failed", map[string]interface{}{"error": err.Error(), "path": path})
		return err
	}

	return nil
}

// cleanupTempFiles removes temporary chunk files
func (p *StreamingProcessor) cleanupTempFiles(chunks []*sortedChunk) {
	for _, chunk := range chunks {
		if chunk.file != nil {
			chunk.file.Close()
		}
		os.Remove(chunk.path)
	}
}
