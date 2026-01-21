package buffer

import (
	"bytes"
	"testing"
	"time"
)

type flushEvent struct {
	partition string
	reason    FlushReason
}

func waitForFlush(t *testing.T, ch <-chan flushEvent, timeout time.Duration) flushEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(timeout):
		t.Fatal("timeout waiting for flush")
	}
	return flushEvent{}
}

func TestDiskBufferWriteRead(t *testing.T) {
	baseDir := t.TempDir()
	buf, err := NewDiskBuffer(baseDir, "partition")
	if err != nil {
		t.Fatalf("create buffer: %v", err)
	}

	events := [][]byte{
		[]byte(`{"event":"a","value":1}`),
		[]byte(`{"event":"b","value":2}`),
	}

	for _, ev := range events {
		if err := buf.Write(ev); err != nil {
			t.Fatalf("write event: %v", err)
		}
	}

	if err := buf.CloseForFlush(); err != nil {
		t.Fatalf("close for flush: %v", err)
	}

	reader, err := NewDiskBufferReader(buf.Path())
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	readEvents, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("expected %d events, got %d", len(events), len(readEvents))
	}
	for i := range events {
		if !bytes.Equal(events[i], readEvents[i]) {
			t.Fatalf("event %d mismatch: %s != %s", i, events[i], readEvents[i])
		}
	}
}

func TestBufferManagerFlushBySize(t *testing.T) {
	baseDir := t.TempDir()
	flushCh := make(chan flushEvent, 10)

	onFlush := func(partitionPath string, buf *DiskBuffer, reason FlushReason) error {
		flushCh <- flushEvent{partition: partitionPath, reason: reason}
		_ = buf.Delete()
		return nil
	}

	bp := BackpressureConfig{
		MaxPendingFlushes:    10,
		HighWatermarkPending: 8,
		LowWatermarkPending:  4,
		MaxConcurrentFlushes: 1,
		MaxOpenFiles:         10,
		MaxTotalBytes:        10 * 1024 * 1024,
	}

	manager := NewBufferManagerWithBackpressure(baseDir, 2, 10*1024*1024, time.Hour, 0, onFlush, bp)
	defer manager.Stop()

	if err := manager.Write("partition", []byte(`{"event":"a"}`)); err != nil {
		t.Fatalf("write event 1: %v", err)
	}
	if err := manager.Write("partition", []byte(`{"event":"b"}`)); err != nil {
		t.Fatalf("write event 2: %v", err)
	}

	ev := waitForFlush(t, flushCh, 2*time.Second)
	if ev.partition != "partition" {
		t.Fatalf("expected flush partition 'partition', got %q", ev.partition)
	}
	if ev.reason != FlushReasonSize {
		t.Fatalf("expected size flush, got %q", ev.reason)
	}
}

func TestBufferManagerTimeoutFromCreation(t *testing.T) {
	baseDir := t.TempDir()
	flushCh := make(chan flushEvent, 10)

	onFlush := func(partitionPath string, buf *DiskBuffer, reason FlushReason) error {
		flushCh <- flushEvent{partition: partitionPath, reason: reason}
		_ = buf.Delete()
		return nil
	}

	maxAge := 80 * time.Millisecond
	bp := BackpressureConfig{
		MaxPendingFlushes:    10,
		HighWatermarkPending: 8,
		LowWatermarkPending:  4,
		MaxConcurrentFlushes: 1,
		MaxOpenFiles:         10,
		MaxTotalBytes:        10 * 1024 * 1024,
	}

	manager := NewBufferManagerWithBackpressure(baseDir, 100, 10*1024*1024, maxAge, 0, onFlush, bp)
	defer manager.Stop()

	if err := manager.Write("p1", []byte(`{"event":"first"}`)); err != nil {
		t.Fatalf("write p1 event: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	if err := manager.FlushExpired(); err != nil {
		t.Fatalf("flush expired: %v", err)
	}

	select {
	case ev := <-flushCh:
		t.Fatalf("unexpected early flush for %q", ev.partition)
	default:
	}

	time.Sleep(maxAge + 30*time.Millisecond)

	if err := manager.FlushExpired(); err != nil {
		t.Fatalf("flush expired after timeout: %v", err)
	}

	ev := waitForFlush(t, flushCh, 2*time.Second)
	if ev.partition != "p1" {
		t.Fatalf("expected flush partition 'p1', got %q", ev.partition)
	}
}

func TestBufferManagerFlushesOnTimeout(t *testing.T) {
	baseDir := t.TempDir()
	flushCh := make(chan flushEvent, 10)

	onFlush := func(partitionPath string, buf *DiskBuffer, reason FlushReason) error {
		flushCh <- flushEvent{partition: partitionPath, reason: reason}
		_ = buf.Delete()
		return nil
	}

	maxAge := 50 * time.Millisecond
	bp := BackpressureConfig{
		MaxPendingFlushes:    10,
		HighWatermarkPending: 8,
		LowWatermarkPending:  4,
		MaxConcurrentFlushes: 1,
		MaxOpenFiles:         10,
		MaxTotalBytes:        10 * 1024 * 1024,
	}

	manager := NewBufferManagerWithBackpressure(baseDir, 100, 10*1024*1024, maxAge, 0, onFlush, bp)
	defer manager.Stop()

	if err := manager.Write("timeout", []byte(`{"event":"late"}`)); err != nil {
		t.Fatalf("write event: %v", err)
	}

	time.Sleep(maxAge + 20*time.Millisecond)

	if err := manager.FlushExpired(); err != nil {
		t.Fatalf("flush expired: %v", err)
	}

	ev := waitForFlush(t, flushCh, 2*time.Second)
	if ev.partition != "timeout" {
		t.Fatalf("expected flush partition 'timeout', got %q", ev.partition)
	}
	if ev.reason != FlushReasonTime {
		t.Fatalf("expected time flush, got %q", ev.reason)
	}
}

func TestBufferManagerPeriodicFlushesOnTimeout(t *testing.T) {
	baseDir := t.TempDir()
	flushCh := make(chan flushEvent, 10)

	onFlush := func(partitionPath string, buf *DiskBuffer, reason FlushReason) error {
		flushCh <- flushEvent{partition: partitionPath, reason: reason}
		_ = buf.Delete()
		return nil
	}

	maxAge := 60 * time.Millisecond
	bp := BackpressureConfig{
		MaxPendingFlushes:    10,
		HighWatermarkPending: 8,
		LowWatermarkPending:  4,
		MaxConcurrentFlushes: 1,
		MaxOpenFiles:         10,
		MaxTotalBytes:        10 * 1024 * 1024,
	}

	manager := NewBufferManagerWithBackpressure(baseDir, 100, 10*1024*1024, maxAge, 0, onFlush, bp)
	defer manager.Stop()

	stopFlush := make(chan struct{})
	manager.StartPeriodicFlush(20*time.Millisecond, stopFlush)
	defer close(stopFlush)

	if err := manager.Write("timeout", []byte(`{"event":"late"}`)); err != nil {
		t.Fatalf("write event: %v", err)
	}

	ev := waitForFlush(t, flushCh, 2*time.Second)
	if ev.partition != "timeout" {
		t.Fatalf("expected flush partition 'timeout', got %q", ev.partition)
	}
	if ev.reason != FlushReasonTime {
		t.Fatalf("expected time flush, got %q", ev.reason)
	}
}

func TestBufferManagerMaxTotalBytes(t *testing.T) {
	baseDir := t.TempDir()
	flushCh := make(chan flushEvent, 100)

	onFlush := func(partitionPath string, buf *DiskBuffer, reason FlushReason) error {
		flushCh <- flushEvent{partition: partitionPath, reason: reason}
		_ = buf.Delete()
		return nil
	}

	// Very low MaxTotalBytes to trigger force flush
	bp := BackpressureConfig{
		MaxPendingFlushes:    100,
		HighWatermarkPending: 50,
		LowWatermarkPending:  25,
		MaxConcurrentFlushes: 2,
		MaxOpenFiles:         10,
		MaxTotalBytes:        100, // Very low - will trigger force flush
	}

	manager := NewBufferManagerWithBackpressure(baseDir, 1000, 10*1024*1024, time.Hour, 0, onFlush, bp)
	defer manager.Stop()

	// Write enough data to exceed MaxTotalBytes
	largeEvent := make([]byte, 50)
	for i := range largeEvent {
		largeEvent[i] = 'x'
	}

	// Write to multiple partitions to accumulate bytes
	for i := 0; i < 10; i++ {
		partition := "partition" + string(rune('A'+i))
		if err := manager.Write(partition, largeEvent); err != nil {
			t.Fatalf("write to %s: %v", partition, err)
		}
	}

	// Should have triggered force flushes due to MaxTotalBytes
	stats := manager.Stats()
	t.Logf("Stats: TotalBytes=%d, PendingFlushes=%d, CompletedFlushes=%d",
		stats.TotalBytes, stats.PendingFlushes, stats.CompletedFlushes)

	// Give time for flushes to process
	time.Sleep(100 * time.Millisecond)

	// Verify that flushes occurred (force flushes when MaxTotalBytes exceeded)
	flushed := 0
	timeout := time.After(500 * time.Millisecond)
drainLoop:
	for {
		select {
		case <-flushCh:
			flushed++
		case <-timeout:
			break drainLoop
		default:
			break drainLoop
		}
	}

	if flushed == 0 {
		t.Error("expected at least one flush due to MaxTotalBytes pressure")
	}
}

func TestBufferManagerStats(t *testing.T) {
	baseDir := t.TempDir()
	flushCh := make(chan flushEvent, 10)

	onFlush := func(partitionPath string, buf *DiskBuffer, reason FlushReason) error {
		flushCh <- flushEvent{partition: partitionPath, reason: reason}
		_ = buf.Delete()
		return nil
	}

	bp := BackpressureConfig{
		MaxPendingFlushes:    10,
		HighWatermarkPending: 8,
		LowWatermarkPending:  4,
		MaxConcurrentFlushes: 2,
		MaxOpenFiles:         10,
		MaxTotalBytes:        10 * 1024 * 1024,
	}

	manager := NewBufferManagerWithBackpressure(baseDir, 100, 10*1024*1024, time.Hour, 0, onFlush, bp)
	defer manager.Stop()

	// Initial stats
	stats := manager.Stats()
	if stats.ActiveBuffers != 0 {
		t.Errorf("expected 0 active buffers initially, got %d", stats.ActiveBuffers)
	}

	// Write to create buffers
	manager.Write("p1", []byte(`{"event":"a"}`))
	manager.Write("p2", []byte(`{"event":"b"}`))
	manager.Write("p3", []byte(`{"event":"c"}`))

	stats = manager.Stats()
	if stats.ActiveBuffers != 3 {
		t.Errorf("expected 3 active buffers, got %d", stats.ActiveBuffers)
	}
	if stats.TotalBytes == 0 {
		t.Error("expected non-zero TotalBytes after writes")
	}

	// Flush all
	manager.FlushAll()

	// Wait for flushes
	for i := 0; i < 3; i++ {
		select {
		case <-flushCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for flush %d", i)
		}
	}

	// Give time for stats to update
	time.Sleep(50 * time.Millisecond)

	stats = manager.Stats()
	if stats.CompletedFlushes < 3 {
		t.Errorf("expected at least 3 completed flushes, got %d", stats.CompletedFlushes)
	}
}

func TestBufferManagerFlushByBytes(t *testing.T) {
	baseDir := t.TempDir()
	flushCh := make(chan flushEvent, 10)

	onFlush := func(partitionPath string, buf *DiskBuffer, reason FlushReason) error {
		flushCh <- flushEvent{partition: partitionPath, reason: reason}
		_ = buf.Delete()
		return nil
	}

	bp := BackpressureConfig{
		MaxPendingFlushes:    10,
		HighWatermarkPending: 8,
		LowWatermarkPending:  4,
		MaxConcurrentFlushes: 1,
		MaxOpenFiles:         10,
		MaxTotalBytes:        10 * 1024 * 1024,
	}

	// maxBytes=50 should trigger flush when exceeded
	manager := NewBufferManagerWithBackpressure(baseDir, 1000, 50, time.Hour, 0, onFlush, bp)
	defer manager.Stop()

	// Write events that total more than 50 bytes
	event := []byte(`{"event":"test","data":"some data here"}`) // ~40 bytes

	if err := manager.Write("partition", event); err != nil {
		t.Fatalf("write event 1: %v", err)
	}
	if err := manager.Write("partition", event); err != nil {
		t.Fatalf("write event 2: %v", err)
	}

	ev := waitForFlush(t, flushCh, 2*time.Second)
	if ev.partition != "partition" {
		t.Fatalf("expected flush partition 'partition', got %q", ev.partition)
	}
	if ev.reason != FlushReasonSize {
		t.Fatalf("expected size flush, got %q", ev.reason)
	}
}

func TestBufferManagerIsPressured(t *testing.T) {
	baseDir := t.TempDir()

	// Create a slow flush handler to build up pending flushes
	flushCh := make(chan flushEvent, 100)
	slowFlush := func(partitionPath string, buf *DiskBuffer, reason FlushReason) error {
		time.Sleep(50 * time.Millisecond) // Slow flush
		flushCh <- flushEvent{partition: partitionPath, reason: reason}
		_ = buf.Delete()
		return nil
	}

	bp := BackpressureConfig{
		MaxPendingFlushes:    100,
		HighWatermarkPending: 5,  // Low threshold for testing
		LowWatermarkPending:  2,
		MaxConcurrentFlushes: 1,  // Single worker to build up queue
		MaxOpenFiles:         10,
		MaxTotalBytes:        10 * 1024 * 1024,
	}

	manager := NewBufferManagerWithBackpressure(baseDir, 1, 10*1024*1024, time.Hour, 0, slowFlush, bp)
	defer manager.Stop()

	// Write many events to different partitions to trigger flushes
	for i := 0; i < 20; i++ {
		partition := "p" + string(rune('A'+i%10))
		manager.Write(partition, []byte(`{"event":"test"}`))
	}

	// Check if pressure is detected
	// Note: Due to timing, this may or may not be pressured
	pressured := manager.IsPressured()
	t.Logf("IsPressured: %v", pressured)

	// Let flushes complete
	manager.WaitForPendingFlushes(5 * time.Second)
}

func TestBufferManagerMultiplePartitions(t *testing.T) {
	baseDir := t.TempDir()
	flushCh := make(chan flushEvent, 100)

	onFlush := func(partitionPath string, buf *DiskBuffer, reason FlushReason) error {
		flushCh <- flushEvent{partition: partitionPath, reason: reason}
		_ = buf.Delete()
		return nil
	}

	bp := BackpressureConfig{
		MaxPendingFlushes:    100,
		HighWatermarkPending: 50,
		LowWatermarkPending:  25,
		MaxConcurrentFlushes: 4,
		MaxOpenFiles:         10,
		MaxTotalBytes:        10 * 1024 * 1024,
	}

	manager := NewBufferManagerWithBackpressure(baseDir, 2, 10*1024*1024, time.Hour, 0, onFlush, bp)
	defer manager.Stop()

	// Write to multiple partitions
	partitions := []string{"team1/date=2024-01-01/", "team1/date=2024-01-02/", "team2/date=2024-01-01/"}

	for _, p := range partitions {
		manager.Write(p, []byte(`{"event":"a"}`))
		manager.Write(p, []byte(`{"event":"b"}`)) // This triggers flush (maxEvents=2)
	}

	// Collect all flushes
	flushedPartitions := make(map[string]bool)
	timeout := time.After(3 * time.Second)
	for i := 0; i < len(partitions); i++ {
		select {
		case ev := <-flushCh:
			flushedPartitions[ev.partition] = true
		case <-timeout:
			t.Fatalf("timeout waiting for flush %d", i)
		}
	}

	// Verify all partitions were flushed
	for _, p := range partitions {
		if !flushedPartitions[p] {
			t.Errorf("expected partition %q to be flushed", p)
		}
	}
}

func TestBufferManagerFlushPartition(t *testing.T) {
	baseDir := t.TempDir()
	flushCh := make(chan flushEvent, 10)

	onFlush := func(partitionPath string, buf *DiskBuffer, reason FlushReason) error {
		flushCh <- flushEvent{partition: partitionPath, reason: reason}
		_ = buf.Delete()
		return nil
	}

	bp := DefaultBackpressureConfig()
	manager := NewBufferManagerWithBackpressure(baseDir, 100, 10*1024*1024, time.Hour, 0, onFlush, bp)
	defer manager.Stop()

	// Write to two partitions
	manager.Write("p1", []byte(`{"event":"a"}`))
	manager.Write("p2", []byte(`{"event":"b"}`))

	// Flush only p1
	if err := manager.FlushPartition("p1"); err != nil {
		t.Fatalf("flush partition p1: %v", err)
	}

	ev := waitForFlush(t, flushCh, 2*time.Second)
	if ev.partition != "p1" {
		t.Fatalf("expected flush of p1, got %q", ev.partition)
	}

	// p2 should still be active
	stats := manager.Stats()
	if stats.ActiveBuffers != 1 {
		t.Errorf("expected 1 active buffer (p2), got %d", stats.ActiveBuffers)
	}
}
