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
