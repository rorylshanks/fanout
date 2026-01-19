package buffer

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"fanout/internal/logging"
)

// ErrBufferFlushing is returned when attempting to write to a buffer that is being flushed
var ErrBufferFlushing = errors.New("buffer is being flushed, cannot reopen")

// DiskBuffer writes events to disk as they arrive, keeping no data in RAM
// Format: [4 bytes length][data][4 bytes length][data]...
type DiskBuffer struct {
	mu         sync.Mutex
	file       *os.File
	writer     *bufio.Writer
	path       string
	eventCount int
	byteCount  int64
	createdAt  time.Time
	maxAge     time.Duration // Per-buffer timeout (may include splay)
	isOpen     bool
	isFlushing bool // Once set, the buffer cannot be reopened
}

// NewDiskBuffer creates a new disk buffer in the given directory
func NewDiskBuffer(baseDir, partitionPath string) (*DiskBuffer, error) {
	return NewDiskBufferWithMaxAge(baseDir, partitionPath, 0)
}

// NewDiskBufferWithMaxAge creates a new disk buffer with a specific max age
func NewDiskBufferWithMaxAge(baseDir, partitionPath string, maxAge time.Duration) (*DiskBuffer, error) {
	// Create directory structure
	dir := filepath.Join(baseDir, "buffers", partitionPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create buffer directory: %w", err)
	}

	// Create buffer file with timestamp
	filename := fmt.Sprintf("buffer_%d.bin", time.Now().UnixNano())
	path := filepath.Join(dir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create buffer file: %w", err)
	}

	return &DiskBuffer{
		file:      file,
		writer:    bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		path:      path,
		createdAt: time.Now(),
		maxAge:    maxAge,
		isOpen:    true,
	}, nil
}

// openFile opens the file handle if it's not already open
func (b *DiskBuffer) openFile() error {
	if b.isOpen && b.file != nil {
		return nil
	}

	// Once a buffer is marked for flushing, it cannot be reopened
	// This prevents race conditions where we read the file while another goroutine writes
	if b.isFlushing {
		return ErrBufferFlushing
	}

	file, err := os.OpenFile(b.path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen buffer file: %w", err)
	}

	b.file = file
	b.writer = bufio.NewWriterSize(file, 64*1024)
	b.isOpen = true
	return nil
}

// closeFile closes the file handle but keeps the buffer metadata
func (b *DiskBuffer) closeFile() error {
	if !b.isOpen || b.file == nil {
		return nil
	}

	if err := b.writer.Flush(); err != nil {
		b.file.Close()
		b.isOpen = false
		b.file = nil
		b.writer = nil
		return err
	}

	err := b.file.Close()
	b.isOpen = false
	b.file = nil
	b.writer = nil
	return err
}

// Write appends an event to the disk buffer
func (b *DiskBuffer) Write(data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure file is open
	if err := b.openFile(); err != nil {
		return err
	}

	// Write length prefix (4 bytes, big endian)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	if _, err := b.writer.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}

	// Write data
	if _, err := b.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	b.eventCount++
	b.byteCount += int64(4 + len(data))

	return nil
}

// Flush ensures all buffered data is written to disk
func (b *DiskBuffer) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.isOpen || b.writer == nil {
		return nil
	}

	if err := b.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}
	return b.file.Sync()
}

// EventCount returns the number of events in the buffer
func (b *DiskBuffer) EventCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.eventCount
}

// ByteCount returns the total bytes written
func (b *DiskBuffer) ByteCount() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.byteCount
}

// Age returns how long since the buffer was created
func (b *DiskBuffer) Age() time.Duration {
	return time.Since(b.createdAt)
}

// MaxAge returns the buffer's max age (timeout)
func (b *DiskBuffer) MaxAge() time.Duration {
	return b.maxAge
}

// IsExpired returns true if the buffer has exceeded its max age
func (b *DiskBuffer) IsExpired() bool {
	if b.maxAge <= 0 {
		return false
	}
	return time.Since(b.createdAt) >= b.maxAge
}

// Path returns the buffer file path
func (b *DiskBuffer) Path() string {
	return b.path
}

// IsOpen returns whether the file handle is currently open
func (b *DiskBuffer) IsOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.isOpen
}

// Close closes the buffer file completely
func (b *DiskBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closeFile()
}

// CloseForFlush marks the buffer as flushing and closes it
// After this call, the buffer cannot be reopened by concurrent writes
func (b *DiskBuffer) CloseForFlush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.isFlushing = true
	return b.closeFile()
}

// IsFlushing returns whether the buffer is being flushed
func (b *DiskBuffer) IsFlushing() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.isFlushing
}

// CloseHandle closes just the file handle (for LRU eviction)
func (b *DiskBuffer) CloseHandle() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closeFile()
}

// Delete removes the buffer file from disk
func (b *DiskBuffer) Delete() error {
	return os.Remove(b.path)
}

// DiskBufferReader reads events from a disk buffer
type DiskBufferReader struct {
	file   *os.File
	reader *bufio.Reader
}

// NewDiskBufferReader opens a disk buffer for reading
func NewDiskBufferReader(path string) (*DiskBufferReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open buffer file: %w", err)
	}

	return &DiskBufferReader{
		file:   file,
		reader: bufio.NewReaderSize(file, 64*1024),
	}, nil
}

// Read reads the next event from the buffer
// Returns io.EOF when no more events
func (r *DiskBufferReader) Read() ([]byte, error) {
	// Read length prefix
	var lenBuf [4]byte
	if _, err := io.ReadFull(r.reader, lenBuf[:]); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lenBuf[:])

	// Read data
	data := make([]byte, length)
	if _, err := io.ReadFull(r.reader, data); err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	return data, nil
}

// ReadAll reads all events from the buffer
func (r *DiskBufferReader) ReadAll() ([][]byte, error) {
	var events [][]byte
	for {
		data, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		events = append(events, data)
	}
	return events, nil
}

// Close closes the reader
func (r *DiskBufferReader) Close() error {
	return r.file.Close()
}

// FlushReason indicates why a buffer was flushed
type FlushReason string

const (
	FlushReasonSize  FlushReason = "size"  // Buffer reached max events or max bytes
	FlushReasonTime  FlushReason = "time"  // Buffer reached max age
	FlushReasonForce FlushReason = "force" // Forced flush (shutdown, backpressure, explicit)
)

// FlushJob represents a buffer that needs to be flushed
type FlushJob struct {
	PartitionPath string
	Buffer        *DiskBuffer
	Reason        FlushReason
}

// BackpressureConfig configures backpressure behavior
type BackpressureConfig struct {
	// MaxPendingFlushes is the maximum number of flush jobs that can be queued
	// When exceeded, Write() will block until space is available
	MaxPendingFlushes int
	// MaxConcurrentFlushes is the number of concurrent flush workers
	MaxConcurrentFlushes int
	// MaxOpenFiles is the LRU cache size for open file handles
	MaxOpenFiles int
	// MaxTotalBytes is the maximum total bytes across all buffers
	// When exceeded, largest buffers will be force-flushed
	MaxTotalBytes int64
}

// DefaultBackpressureConfig returns sensible defaults
func DefaultBackpressureConfig() BackpressureConfig {
	return BackpressureConfig{
		MaxPendingFlushes:    100,
		MaxConcurrentFlushes: 4,
		MaxOpenFiles:         256,
		MaxTotalBytes:        50 * 1024 * 1024 * 1024, // 50GB
	}
}

// BufferManagerStats tracks buffer manager metrics
type BufferManagerStats struct {
	ActiveBuffers    int64
	TotalBytes       int64
	PendingFlushes   int64
	CompletedFlushes int64
	FailedFlushes    int64
	BlockedWrites    int64
	OpenFileHandles  int64
	FileHandleEvicts int64
}

// BufferManager manages multiple disk buffers by partition path with backpressure
type BufferManager struct {
	mu            sync.RWMutex
	buffers       map[string]*DiskBuffer
	lastFlushAt   map[string]time.Time
	baseDir       string
	maxEvents     int
	maxBytes      int64
	maxAge        time.Duration
	maxAgeSplay   time.Duration // Random splay +/- for timeout
	onFlush       func(partitionPath string, buffer *DiskBuffer, reason FlushReason) error
	backpressure  BackpressureConfig
	flushQueue    chan FlushJob
	flushSem      chan struct{} // Semaphore for concurrent flushes
	stats         BufferManagerStats
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	totalBytes    int64 // atomic
	blockedWrites int64 // atomic
	handleEvicts  int64 // atomic
	fileHandles   *LRUCache
}

// NewBufferManager creates a new buffer manager with backpressure support
func NewBufferManager(baseDir string, maxEvents int, maxBytes int64, maxAge time.Duration, onFlush func(string, *DiskBuffer, FlushReason) error) *BufferManager {
	return NewBufferManagerWithBackpressure(baseDir, maxEvents, maxBytes, maxAge, 0, onFlush, DefaultBackpressureConfig())
}

// NewBufferManagerWithBackpressure creates a buffer manager with custom backpressure config
func NewBufferManagerWithBackpressure(
	baseDir string,
	maxEvents int,
	maxBytes int64,
	maxAge time.Duration,
	maxAgeSplay time.Duration,
	onFlush func(string, *DiskBuffer, FlushReason) error,
	bp BackpressureConfig,
) *BufferManager {
	ctx, cancel := context.WithCancel(context.Background())

	m := &BufferManager{
		buffers:      make(map[string]*DiskBuffer),
		lastFlushAt:  make(map[string]time.Time),
		baseDir:      baseDir,
		maxEvents:    maxEvents,
		maxBytes:     maxBytes,
		maxAge:       maxAge,
		maxAgeSplay:  maxAgeSplay,
		onFlush:      onFlush,
		backpressure: bp,
		flushQueue:   make(chan FlushJob, bp.MaxPendingFlushes),
		flushSem:     make(chan struct{}, bp.MaxConcurrentFlushes),
		ctx:          ctx,
		cancel:       cancel,
	}

	// Seed random for splay calculation
	rand.Seed(time.Now().UnixNano())

	// Create LRU cache for file handles
	m.fileHandles = NewLRUCache(bp.MaxOpenFiles, func(key string, value interface{}) {
		// On eviction, close the file handle
		if buf, ok := value.(*DiskBuffer); ok {
			buf.CloseHandle()
			atomic.AddInt64(&m.handleEvicts, 1)
		}
	})

	// Start flush workers
	for i := 0; i < bp.MaxConcurrentFlushes; i++ {
		m.wg.Add(1)
		go m.flushWorker()
	}

	return m
}

// flushWorker processes flush jobs from the queue
// Workers drain the entire queue before exiting, even after context cancellation
func (m *BufferManager) flushWorker() {
	defer m.wg.Done()

	// Drain the queue completely - don't exit on context cancellation
	// This ensures graceful shutdown flushes all buffers
	for job := range m.flushQueue {
		// Acquire semaphore slot (blocking - no context check)
		m.flushSem <- struct{}{}

		// Process flush
		err := m.processFlush(job)

		// Release semaphore
		<-m.flushSem

		if err != nil {
			atomic.AddInt64(&m.stats.FailedFlushes, 1)
			logging.ErrorLog("flush_failed", map[string]interface{}{
				"partition": job.PartitionPath,
				"error":     err.Error(),
			})
		} else {
			atomic.AddInt64(&m.stats.CompletedFlushes, 1)
		}

		atomic.AddInt64(&m.stats.PendingFlushes, -1)
	}
}

// processFlush handles a single flush job
func (m *BufferManager) processFlush(job FlushJob) error {
	eventCount := job.Buffer.EventCount()
	byteCount := job.Buffer.ByteCount()

	logging.DebugLog("flush_start", map[string]interface{}{
		"partition": job.PartitionPath,
		"reason":    string(job.Reason),
		"events":    eventCount,
		"bytes":     byteCount,
	})

	// Remove from LRU cache
	m.fileHandles.Remove(job.PartitionPath)

	// Close the buffer for flushing - this prevents concurrent writes from reopening it
	if err := job.Buffer.CloseForFlush(); err != nil {
		logging.ErrorLog("flush_close_failed", map[string]interface{}{
			"partition": job.PartitionPath,
			"error":     err.Error(),
		})
		return err
	}

	// Update total bytes
	atomic.AddInt64(&m.totalBytes, -byteCount)

	// Call the flush handler with reason
	if m.onFlush != nil {
		if err := m.onFlush(job.PartitionPath, job.Buffer, job.Reason); err != nil {
			logging.ErrorLog("flush_handler_failed", map[string]interface{}{
				"partition": job.PartitionPath,
				"error":     err.Error(),
			})
			return err
		}
	}
	m.mu.Lock()
	m.lastFlushAt[job.PartitionPath] = time.Now()
	m.mu.Unlock()

	logging.DebugLog("flush_complete", map[string]interface{}{
		"partition": job.PartitionPath,
		"events":    eventCount,
	})

	return nil
}

// randomTimeout returns a timeout with random splay applied
// If splay is 10s and timeout is 60s, returns a random value between 50s and 70s
func (m *BufferManager) randomTimeout() time.Duration {
	if m.maxAgeSplay <= 0 {
		return m.maxAge
	}
	// Calculate random offset in range [-splay, +splay]
	splayNs := m.maxAgeSplay.Nanoseconds()
	offset := rand.Int63n(2*splayNs+1) - splayNs
	return m.maxAge + time.Duration(offset)
}

// Write writes an event to the appropriate partition buffer
// This will block if backpressure limits are exceeded
func (m *BufferManager) Write(partitionPath string, data []byte) error {
	// Check total bytes backpressure
	for atomic.LoadInt64(&m.totalBytes) >= m.backpressure.MaxTotalBytes {
		atomic.AddInt64(&m.blockedWrites, 1)
		// Force flush the largest buffer
		if err := m.forceFlushLargest(); err != nil {
			return fmt.Errorf("backpressure flush failed: %w", err)
		}
		// Small sleep to prevent tight loop
		time.Sleep(10 * time.Millisecond)
	}

	m.mu.Lock()

	buffer, exists := m.buffers[partitionPath]
	if !exists {
		var err error
		buffer, err = NewDiskBufferWithMaxAge(m.baseDir, partitionPath, m.randomTimeout())
		if err != nil {
			m.mu.Unlock()
			return err
		}
		m.buffers[partitionPath] = buffer
		atomic.AddInt64(&m.stats.ActiveBuffers, 1)
	}

	// Touch LRU cache to keep this buffer's file handle open
	m.fileHandles.Put(partitionPath, buffer)

	m.mu.Unlock()

	if err := buffer.Write(data); err != nil {
		if errors.Is(err, ErrBufferFlushing) {
			return m.retryWrite(partitionPath, data, buffer)
		}
		return err
	}

	atomic.AddInt64(&m.totalBytes, int64(len(data)+4))

	// Check if buffer should be flushed
	if m.shouldFlush(partitionPath, buffer) {
		return m.queueFlushIfSame(partitionPath, buffer)
	}

	return nil
}

// retryWrite handles writes when the current buffer is being flushed
// It either uses an existing new buffer or creates a fresh one
func (m *BufferManager) retryWrite(partitionPath string, data []byte, oldBuffer *DiskBuffer) error {
	var target *DiskBuffer
	created := false

	m.mu.Lock()
	buffer, exists := m.buffers[partitionPath]
	if exists && buffer != oldBuffer && !buffer.IsFlushing() {
		target = buffer
	} else {
		var err error
		target, err = NewDiskBufferWithMaxAge(m.baseDir, partitionPath, m.randomTimeout())
		if err != nil {
			m.mu.Unlock()
			return err
		}
		m.buffers[partitionPath] = target
		atomic.AddInt64(&m.stats.ActiveBuffers, 1)
		created = true
	}

	// Touch LRU cache to keep this buffer's file handle open
	m.fileHandles.Put(partitionPath, target)
	m.mu.Unlock()

	if err := target.Write(data); err != nil {
		if created {
			m.mu.Lock()
			if m.buffers[partitionPath] == target {
				delete(m.buffers, partitionPath)
				atomic.AddInt64(&m.stats.ActiveBuffers, -1)
			}
			m.mu.Unlock()
		}
		return err
	}

	atomic.AddInt64(&m.totalBytes, int64(len(data)+4))
	return nil
}

// shouldFlush checks if a buffer should be flushed
func (m *BufferManager) shouldFlush(partitionPath string, buffer *DiskBuffer) bool {
	return buffer.EventCount() >= m.maxEvents ||
		buffer.ByteCount() >= m.maxBytes ||
		m.isBufferExpired(partitionPath, buffer)
}

func (m *BufferManager) isBufferExpired(partitionPath string, buffer *DiskBuffer) bool {
	if buffer.MaxAge() <= 0 {
		return false
	}
	m.mu.RLock()
	lastFlushAt, hasFlush := m.lastFlushAt[partitionPath]
	m.mu.RUnlock()
	if !hasFlush {
		return buffer.Age() >= buffer.MaxAge()
	}
	return time.Since(lastFlushAt) >= buffer.MaxAge()
}

// queueFlushIfSame queues a partition for flushing only if the buffer matches
// This prevents a race condition where we flush a newly-created buffer that wasn't the one that triggered the flush
func (m *BufferManager) queueFlushIfSame(partitionPath string, expectedBuffer *DiskBuffer) error {
	m.mu.Lock()
	buffer, exists := m.buffers[partitionPath]
	if !exists {
		m.mu.Unlock()
		return nil
	}
	// Only flush if it's the same buffer that triggered the flush
	if buffer != expectedBuffer {
		m.mu.Unlock()
		return nil
	}
	delete(m.buffers, partitionPath)
	atomic.AddInt64(&m.stats.ActiveBuffers, -1)
	m.mu.Unlock()

	job := FlushJob{
		PartitionPath: partitionPath,
		Buffer:        buffer,
		Reason:        FlushReasonSize, // Size-based flush (max events or max bytes)
	}

	// This will block if queue is full (backpressure)
	select {
	case m.flushQueue <- job:
		atomic.AddInt64(&m.stats.PendingFlushes, 1)
		return nil
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

// queueFlushWithReason queues a partition for flushing with a specific reason
func (m *BufferManager) queueFlushWithReason(partitionPath string, reason FlushReason) error {
	m.mu.Lock()
	buffer, exists := m.buffers[partitionPath]
	if !exists {
		m.mu.Unlock()
		return nil
	}
	delete(m.buffers, partitionPath)
	atomic.AddInt64(&m.stats.ActiveBuffers, -1)
	m.mu.Unlock()

	job := FlushJob{
		PartitionPath: partitionPath,
		Buffer:        buffer,
		Reason:        reason,
	}

	// This will block if queue is full (backpressure)
	select {
	case m.flushQueue <- job:
		atomic.AddInt64(&m.stats.PendingFlushes, 1)
		return nil
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

// forceFlushLargest forces a flush of the largest buffer
func (m *BufferManager) forceFlushLargest() error {
	m.mu.Lock()
	var largestPath string
	var largestSize int64

	for path, buf := range m.buffers {
		size := buf.ByteCount()
		if size > largestSize {
			largestSize = size
			largestPath = path
		}
	}
	m.mu.Unlock()

	if largestPath != "" {
		return m.queueFlushWithReason(largestPath, FlushReasonForce)
	}
	return nil
}

// forceFlushOldest forces a flush of the oldest buffer
func (m *BufferManager) forceFlushOldest() error {
	m.mu.Lock()
	var oldestPath string
	var oldestAge time.Duration

	for path, buf := range m.buffers {
		age := buf.Age()
		if age > oldestAge {
			oldestAge = age
			oldestPath = path
		}
	}
	m.mu.Unlock()

	if oldestPath != "" {
		return m.queueFlushWithReason(oldestPath, FlushReasonForce)
	}
	return nil
}

// FlushPartition flushes a specific partition buffer (for compatibility)
func (m *BufferManager) FlushPartition(partitionPath string) error {
	return m.queueFlushWithReason(partitionPath, FlushReasonForce)
}

// FlushAll flushes all partition buffers
func (m *BufferManager) FlushAll() error {
	m.mu.Lock()
	paths := make([]string, 0, len(m.buffers))
	var totalEvents int
	var totalBytes int64
	for path, buf := range m.buffers {
		paths = append(paths, path)
		totalEvents += buf.EventCount()
		totalBytes += buf.ByteCount()
	}
	m.mu.Unlock()

	logging.InfoLog("flush_all_start", map[string]interface{}{
		"buffers":      len(paths),
		"total_events": totalEvents,
		"total_bytes":  totalBytes,
	})

	for i, path := range paths {
		if err := m.queueFlushWithReason(path, FlushReasonForce); err != nil {
			logging.ErrorLog("flush_all_queue_failed", map[string]interface{}{
				"path":   path,
				"index":  i,
				"total":  len(paths),
				"error":  err.Error(),
			})
			return err
		}
	}

	logging.InfoLog("flush_all_queued", map[string]interface{}{
		"buffers": len(paths),
	})

	return nil
}

// FlushExpired flushes buffers that have exceeded their individual maxAge (with splay)
func (m *BufferManager) FlushExpired() error {
	m.mu.RLock()
	var expired []string
	for path, buffer := range m.buffers {
		if m.isBufferExpired(path, buffer) {
			expired = append(expired, path)
		}
	}
	m.mu.RUnlock()

	if len(expired) > m.backpressure.MaxConcurrentFlushes {
		expired = expired[:m.backpressure.MaxConcurrentFlushes]
	}

	for _, path := range expired {
		if err := m.queueFlushWithReason(path, FlushReasonTime); err != nil {
			return err
		}
	}

	return nil
}

// StartPeriodicFlush starts a background goroutine to flush expired buffers
func (m *BufferManager) StartPeriodicFlush(interval time.Duration, stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := m.FlushExpired(); err != nil {
					logging.ErrorLog("flush_expired_failed", map[string]interface{}{"error": err.Error()})
				}
			case <-stop:
				return
			case <-m.ctx.Done():
				return
			}
		}
	}()
}

// Stop gracefully stops the buffer manager, waiting for pending flushes
func (m *BufferManager) Stop() error {
	// Flush all remaining buffers
	if err := m.FlushAll(); err != nil {
		logging.ErrorLog("flush_all_failed", map[string]interface{}{"error": err.Error()})
	}

	// Close the flush queue
	close(m.flushQueue)

	// Cancel context
	m.cancel()

	// Wait for workers to finish
	m.wg.Wait()

	// Clear LRU cache (closes all file handles)
	m.fileHandles.Clear()

	return nil
}

// WaitForPendingFlushes blocks until all pending flushes complete
func (m *BufferManager) WaitForPendingFlushes(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	startPending := atomic.LoadInt64(&m.stats.PendingFlushes)

	logging.InfoLog("flush_wait_start", map[string]interface{}{
		"pending_flushes": startPending,
		"timeout_secs":    timeout.Seconds(),
	})

	for atomic.LoadInt64(&m.stats.PendingFlushes) > 0 {
		if time.Now().After(deadline) {
			pending := atomic.LoadInt64(&m.stats.PendingFlushes)
			logging.ErrorLog("flush_wait_timeout", map[string]interface{}{
				"remaining": pending,
			})
			return fmt.Errorf("timeout waiting for %d pending flushes", pending)
		}

		// Log progress every 5 seconds
		if time.Since(lastLog) >= 5*time.Second {
			pending := atomic.LoadInt64(&m.stats.PendingFlushes)
			completed := atomic.LoadInt64(&m.stats.CompletedFlushes)
			failed := atomic.LoadInt64(&m.stats.FailedFlushes)
			logging.InfoLog("flush_wait_progress", map[string]interface{}{
				"pending":   pending,
				"completed": completed,
				"failed":    failed,
			})
			lastLog = time.Now()
		}

		time.Sleep(100 * time.Millisecond)
	}

	completed := atomic.LoadInt64(&m.stats.CompletedFlushes)
	failed := atomic.LoadInt64(&m.stats.FailedFlushes)
	logging.InfoLog("flush_wait_done", map[string]interface{}{
		"completed": completed,
		"failed":    failed,
	})

	return nil
}

// Stats returns current buffer manager statistics
func (m *BufferManager) Stats() BufferManagerStats {
	m.mu.RLock()
	activeBuffers := int64(len(m.buffers))
	m.mu.RUnlock()

	return BufferManagerStats{
		ActiveBuffers:    activeBuffers,
		TotalBytes:       atomic.LoadInt64(&m.totalBytes),
		PendingFlushes:   atomic.LoadInt64(&m.stats.PendingFlushes),
		CompletedFlushes: atomic.LoadInt64(&m.stats.CompletedFlushes),
		FailedFlushes:    atomic.LoadInt64(&m.stats.FailedFlushes),
		BlockedWrites:    atomic.LoadInt64(&m.blockedWrites),
		OpenFileHandles:  int64(m.fileHandles.Len()),
		FileHandleEvicts: atomic.LoadInt64(&m.handleEvicts),
	}
}

// IsPressured returns true if backpressure is being applied
func (m *BufferManager) IsPressured() bool {
	stats := m.Stats()
	return stats.TotalBytes >= m.backpressure.MaxTotalBytes/2 ||
		stats.PendingFlushes >= int64(m.backpressure.MaxPendingFlushes)/2
}
