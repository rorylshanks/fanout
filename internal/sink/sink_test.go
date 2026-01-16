package sink

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fanout/internal/config"
)

func TestFilesystemSinkWriteAndRead(t *testing.T) {
	baseDir := t.TempDir()
	cfg := &config.Sink{Type: "filesystem", Path: baseDir}
	fs, err := NewFilesystemSink(cfg)
	if err != nil {
		t.Fatalf("new filesystem sink: %v", err)
	}

	data := []byte("hello")
	if err := fs.Write(context.Background(), "path/file.txt", data); err != nil {
		t.Fatalf("write: %v", err)
	}

	fullPath := filepath.Join(baseDir, "path", "file.txt")
	got, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("unexpected file contents: %s", got)
	}
}

func TestFilesystemSinkWriteFromReader(t *testing.T) {
	baseDir := t.TempDir()
	cfg := &config.Sink{Type: "filesystem", Path: baseDir}
	fs, err := NewFilesystemSink(cfg)
	if err != nil {
		t.Fatalf("new filesystem sink: %v", err)
	}

	reader := strings.NewReader("streamed")
	if err := fs.WriteFromReader(context.Background(), "stream/file.txt", reader, int64(reader.Len())); err != nil {
		t.Fatalf("write from reader: %v", err)
	}

	fullPath := filepath.Join(baseDir, "stream", "file.txt")
	got, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(got) != "streamed" {
		t.Fatalf("unexpected file contents: %s", got)
	}
}

func TestGenerateFilename(t *testing.T) {
	name := GenerateFilename("parquet")
	if !strings.HasSuffix(name, ".parquet") {
		t.Fatalf("expected .parquet suffix, got %q", name)
	}
}

func TestBytesReader(t *testing.T) {
	r := &bytesReader{data: []byte("abc")}
	buf := make([]byte, 2)
	n, err := r.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("read error: %v", err)
	}
	if n != 2 || string(buf) != "ab" {
		t.Fatalf("unexpected read: n=%d buf=%s", n, buf)
	}
	buf = make([]byte, 2)
	n, err = r.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("read error: %v", err)
	}
	if n != 1 || string(buf[:1]) != "c" {
		t.Fatalf("unexpected read: n=%d buf=%s", n, buf)
	}
}

func TestNewSinkUnsupported(t *testing.T) {
	_, err := NewSink(&config.Sink{Type: "bogus"})
	if err == nil {
		t.Fatal("expected error for unsupported sink type")
	}
}
