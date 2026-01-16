package logging

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestLoggerLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{level: Warn, out: &buf}

	logger.log(Info, "info_msg", nil)
	if buf.Len() != 0 {
		t.Fatalf("expected no output for info at warn level")
	}

	logger.log(Error, "err_msg", map[string]interface{}{"key": "val"})
	out := buf.String()
	if !strings.Contains(out, `"level":"error"`) || !strings.Contains(out, `"msg":"err_msg"`) {
		t.Fatalf("unexpected log output: %s", out)
	}
}

func TestSetLevelFromEnv(t *testing.T) {
	origLevel := defaultLogger.level
	origOut := defaultLogger.out
	t.Cleanup(func() {
		defaultLogger.level = origLevel
		defaultLogger.out = origOut
		os.Unsetenv("LOG_LEVEL")
	})

	if err := os.Setenv("LOG_LEVEL", "debug"); err != nil {
		t.Fatalf("set env: %v", err)
	}

	var buf bytes.Buffer
	defaultLogger.out = &buf
	SetLevelFromEnv()
	DebugLog("debug_msg", nil)

	out := buf.String()
	if !strings.Contains(out, `"level":"debug"`) || !strings.Contains(out, `"msg":"debug_msg"`) {
		t.Fatalf("unexpected log output: %s", out)
	}
}
