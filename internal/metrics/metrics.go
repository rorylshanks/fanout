package metrics

import (
	"net/http"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics for the fanout
type Metrics struct {
	// Input metrics (Kafka)
	MessagesReceived prometheus.Counter
	BytesReceived    prometheus.Counter

	// Processing metrics
	MessagesProcessed prometheus.Counter
	ParseErrors       prometheus.Counter
	BufferWriteErrors prometheus.Counter

	// Processing latency histograms (per-message timing)
	ParseLatency     prometheus.Histogram
	MetadataLatency  prometheus.Histogram
	PartitionLatency prometheus.Histogram
	BufferLatency    prometheus.Histogram

	// Buffer metrics
	ActiveBuffers    prometheus.Gauge
	TotalBufferBytes prometheus.Gauge
	PendingFlushes   prometheus.Gauge
	BlockedWrites    prometheus.Counter
	FileHandleEvicts prometheus.Counter
	OpenFileHandles  prometheus.Gauge

	// Output metrics (S3/sink)
	FlushesCompleted prometheus.Counter
	FlushesFailed    prometheus.Counter
	FlushByReason    *prometheus.CounterVec // Flushes by reason (size, time, force)
	BytesWritten     prometheus.Counter
	FilesUploaded    prometheus.Counter
	RowsWritten      prometheus.Counter

	// Flush latency histograms
	ParquetWriteLatency prometheus.Histogram
	SinkWriteLatency    prometheus.Histogram
	TotalFlushLatency   prometheus.Histogram

	// Backpressure
	BackpressureActive prometheus.Gauge

	// Runtime metrics
	Goroutines  prometheus.Gauge
	HeapAlloc   prometheus.Gauge
	HeapSys     prometheus.Gauge
	GCPauseNs   prometheus.Gauge
	GCRuns      prometheus.Counter
	LastGCPause prometheus.Gauge

	// Throughput gauges (calculated rates)
	MessageRate  prometheus.Gauge
	BytesInRate  prometheus.Gauge
	BytesOutRate prometheus.Gauge
	FlushRate    prometheus.Gauge
}

// New creates and registers all Prometheus metrics
func New(namespace string) *Metrics {
	if namespace == "" {
		namespace = "fanout"
	}

	m := &Metrics{
		// Input metrics
		MessagesReceived: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_received_total",
			Help:      "Total number of messages received from Kafka",
		}),
		BytesReceived: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "bytes_received_total",
			Help:      "Total bytes received from Kafka",
		}),

		// Processing metrics
		MessagesProcessed: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "messages_processed_total",
			Help:      "Total number of messages successfully processed",
		}),
		ParseErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "parse_errors_total",
			Help:      "Total number of JSON parse errors",
		}),
		BufferWriteErrors: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "buffer_write_errors_total",
			Help:      "Total number of buffer write errors",
		}),

		// Processing latency histograms (microseconds)
		ParseLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "parse_latency_microseconds",
			Help:      "JSON parse latency in microseconds",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
		}),
		MetadataLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "metadata_latency_microseconds",
			Help:      "Metadata injection latency in microseconds",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		}),
		PartitionLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "partition_latency_microseconds",
			Help:      "Partition path calculation latency in microseconds",
			Buckets:   []float64{0.5, 1, 2, 5, 10, 25, 50, 100},
		}),
		BufferLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "buffer_write_latency_microseconds",
			Help:      "Buffer write latency in microseconds",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500},
		}),

		// Buffer metrics
		ActiveBuffers: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_buffers",
			Help:      "Number of active partition buffers",
		}),
		TotalBufferBytes: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "total_buffer_bytes",
			Help:      "Total bytes across all buffers",
		}),
		PendingFlushes: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "pending_flushes",
			Help:      "Number of flushes waiting in queue",
		}),
		BlockedWrites: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "blocked_writes_total",
			Help:      "Total number of writes blocked by backpressure",
		}),
		FileHandleEvicts: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "file_handle_evicts_total",
			Help:      "Total number of file handle LRU evictions",
		}),
		OpenFileHandles: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "open_file_handles",
			Help:      "Number of open file handles in LRU cache",
		}),

		// Output metrics
		FlushesCompleted: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "flushes_completed_total",
			Help:      "Total number of successful buffer flushes",
		}),
		FlushesFailed: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "flushes_failed_total",
			Help:      "Total number of failed buffer flushes",
		}),
		FlushByReason: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "flushes_by_reason_total",
			Help:      "Total number of buffer flushes by reason (size, time, force)",
		}, []string{"reason"}),
		BytesWritten: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "bytes_written_total",
			Help:      "Total bytes written to sink (S3/filesystem)",
		}),
		FilesUploaded: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "files_uploaded_total",
			Help:      "Total parquet files uploaded to sink",
		}),
		RowsWritten: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rows_written_total",
			Help:      "Total rows written to parquet files",
		}),

		// Flush latency histograms (milliseconds)
		ParquetWriteLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "parquet_write_latency_milliseconds",
			Help:      "Parquet file write latency in milliseconds",
			Buckets:   []float64{10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000},
		}),
		SinkWriteLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "sink_write_latency_milliseconds",
			Help:      "Sink (S3) write latency in milliseconds",
			Buckets:   []float64{10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		}),
		TotalFlushLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "flush_latency_milliseconds",
			Help:      "Total flush latency in milliseconds (parquet + sink)",
			Buckets:   []float64{50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000},
		}),

		// Backpressure
		BackpressureActive: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "backpressure_active",
			Help:      "Whether backpressure is currently active (1) or not (0)",
		}),

		// Runtime metrics
		Goroutines: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "goroutines",
			Help:      "Number of goroutines",
		}),
		HeapAlloc: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "heap_alloc_bytes",
			Help:      "Bytes allocated on heap",
		}),
		HeapSys: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "heap_sys_bytes",
			Help:      "Bytes obtained from system for heap",
		}),
		GCPauseNs: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "gc_pause_ns",
			Help:      "Last GC pause duration in nanoseconds",
		}),
		GCRuns: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "gc_runs_total",
			Help:      "Total number of GC runs",
		}),
		LastGCPause: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "gc_last_pause_seconds",
			Help:      "Duration of last GC pause in seconds",
		}),

		// Throughput rates (calculated)
		MessageRate: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "message_rate",
			Help:      "Current message processing rate (messages/second)",
		}),
		BytesInRate: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "bytes_in_rate",
			Help:      "Current input bytes rate (bytes/second)",
		}),
		BytesOutRate: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "bytes_out_rate",
			Help:      "Current output bytes rate (bytes/second)",
		}),
		FlushRate: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "flush_rate",
			Help:      "Current flush rate (flushes/second)",
		}),
	}

	return m
}

// UpdateRuntimeMetrics updates Go runtime metrics
func (m *Metrics) UpdateRuntimeMetrics() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	m.Goroutines.Set(float64(runtime.NumGoroutine()))
	m.HeapAlloc.Set(float64(memStats.HeapAlloc))
	m.HeapSys.Set(float64(memStats.HeapSys))

	if memStats.NumGC > 0 {
		m.LastGCPause.Set(float64(memStats.PauseNs[(memStats.NumGC+255)%256]) / 1e9)
	}
}

// Handler returns the Prometheus HTTP handler
func Handler() http.Handler {
	return promhttp.Handler()
}

// ObserveDuration is a helper to record duration in a histogram
func ObserveDuration(h prometheus.Histogram, start time.Time, unit time.Duration) {
	h.Observe(float64(time.Since(start)) / float64(unit))
}
