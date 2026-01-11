package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

type Level int

const (
	Trace Level = iota
	Debug
	Info
	Warn
	Error
)

type Logger struct {
	mu    sync.Mutex
	level Level
	out   io.Writer
}

var defaultLogger = &Logger{
	level: Info,
	out:   os.Stdout,
}

func SetLevelFromEnv() {
	level := os.Getenv("LOG_LEVEL")
	if level == "" {
		return
	}
	SetLevel(level)
}

func SetLevel(level string) {
	defaultLogger.SetLevel(level)
}

func (l *Logger) SetLevel(level string) {
	switch strings.ToLower(level) {
	case "trace":
		l.level = Trace
	case "debug":
		l.level = Debug
	case "info":
		l.level = Info
	case "warn", "warning":
		l.level = Warn
	case "error":
		l.level = Error
	default:
		l.level = Info
	}
}

func TraceLog(msg string, fields map[string]interface{}) {
	defaultLogger.log(Trace, msg, fields)
}

func DebugLog(msg string, fields map[string]interface{}) {
	defaultLogger.log(Debug, msg, fields)
}

func InfoLog(msg string, fields map[string]interface{}) {
	defaultLogger.log(Info, msg, fields)
}

func WarnLog(msg string, fields map[string]interface{}) {
	defaultLogger.log(Warn, msg, fields)
}

func ErrorLog(msg string, fields map[string]interface{}) {
	defaultLogger.log(Error, msg, fields)
}

func (l *Logger) log(level Level, msg string, fields map[string]interface{}) {
	if level < l.level {
		return
	}
	entry := map[string]interface{}{
		"ts":    time.Now().Format(time.RFC3339Nano),
		"level": levelString(level),
		"msg":   msg,
	}
	for k, v := range fields {
		entry[k] = v
	}
	data, err := json.Marshal(entry)
	if err != nil {
		l.mu.Lock()
		fmt.Fprintf(l.out, "{\"ts\":\"%s\",\"level\":\"error\",\"msg\":\"failed to marshal log\",\"error\":%q}\n", time.Now().Format(time.RFC3339Nano), err.Error())
		l.mu.Unlock()
		return
	}
	l.mu.Lock()
	l.out.Write(data)
	l.out.Write([]byte("\n"))
	l.mu.Unlock()
}

func levelString(level Level) string {
	switch level {
	case Trace:
		return "trace"
	case Debug:
		return "debug"
	case Info:
		return "info"
	case Warn:
		return "warn"
	case Error:
		return "error"
	default:
		return "info"
	}
}
