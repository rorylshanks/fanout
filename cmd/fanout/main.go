package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/valyala/fastjson"

	"fanout/internal/buffer"
	"fanout/internal/config"
	"fanout/internal/kafka"
	"fanout/internal/logging"
	"fanout/internal/metrics"
	pqwriter "fanout/internal/parquet"
	"fanout/internal/partitioner"
	"fanout/internal/sink"
)

// Metrics tracks detailed performance metrics
type Metrics struct {
	// Counters (atomic)
	messagesReceived  int64
	messagesProcessed int64
	bytesReceived     int64
	parseErrors       int64
	bufferWriteErrors int64
	flushesCompleted  int64
	flushesFailed     int64
	parquetBytesOut   int64
	rowsWritten       int64
	parquetFiles      int64
	filesUploaded     int64

	// Timing accumulators (nanoseconds, atomic)
	parseTimeNs        int64
	metadataTimeNs     int64
	partitionTimeNs    int64
	bufferWriteTimeNs  int64
	flushTimeNs        int64
	parquetWriteTimeNs int64
	sinkWriteTimeNs    int64

	parquetJSONWallNs   int64
	parquetJSONCPUNs    int64
	parquetSchemaWallNs int64
	parquetSchemaCPUNs  int64
	parquetInitWallNs   int64
	parquetInitCPUNs    int64
	parquetConvWallNs   int64
	parquetConvCPUNs    int64
	parquetWriteWallNs  int64
	parquetWriteCPUNs   int64
	parquetCloseWallNs  int64
	parquetCloseCPUNs   int64

	// Snapshot for rate calculation
	lastSnapshot   time.Time
	lastMsgCount   int64
	lastBytesIn    int64
	lastBytesOut   int64
	lastFlushCount int64
}

func main() {
	logging.SetLevelFromEnv()

	// Get config path from argument or use default
	configPath := "config.yml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	// Load configuration
	cfg, err := config.Load(configPath)
	if err != nil {
		logging.ErrorLog("config_load_failed", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}

	// Get Kafka source
	kafkaSource, _, err := cfg.GetKafkaSource()
	if err != nil {
		logging.ErrorLog("config_kafka_source_failed", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}

	// Get sink configuration - try S3 first, fall back to filesystem
	sinkCfg, _, err := cfg.GetS3Sink()
	if err != nil {
		sinkCfg, _, err = cfg.GetFilesystemSink()
	}
	if err != nil {
		logging.ErrorLog("config_sink_failed", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}

	// Extract partition template from key_prefix before creating sink.
	// The partitioner will use the template to generate full paths (e.g., "events/123/2026/01/11/").
	// For S3 sinks, we clear the keyPrefix so the sink doesn't double-prefix.
	// For filesystem sinks, the path is used as base directory.
	partitionTemplate := sinkCfg.KeyPrefix
	if sinkCfg.Type == "aws_s3" {
		sinkCfg.KeyPrefix = "" // Clear to avoid double-prefixing in S3 sink
	}

	// Create sink
	outputSink, err := sink.NewSink(sinkCfg)
	if err != nil {
		logging.ErrorLog("sink_create_failed", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}
	defer outputSink.Close()

	// Create path partitioner using the full key_prefix template
	pathPartitioner := partitioner.NewPathPartitioner(partitionTemplate)

	bufferDir := filepath.Join(cfg.DataDir, "parquet_buffers")

	// Create batch processor (for fallback)
	batchProcessor := pqwriter.NewBatchProcessor(sinkCfg.Encoding.Parquet, bufferDir)

	// Create temp directory for streaming processor
	tempDir := filepath.Join(cfg.DataDir, "temp")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		logging.ErrorLog("temp_dir_create_failed", map[string]interface{}{"error": err.Error(), "path": tempDir})
		os.Exit(1)
	}

	// Create streaming processor for memory-efficient parquet writing
	streamingProcessor := pqwriter.NewStreamingProcessor(sinkCfg.Encoding.Parquet, tempDir, bufferDir)

	// Configure backpressure from config file
	bpConfig := buffer.BackpressureConfig{
		MaxPendingFlushes:    sinkCfg.DiskBatching.Backpressure.MaxPendingFlushes,
		MaxConcurrentFlushes: sinkCfg.DiskBatching.Backpressure.MaxConcurrentFlushes,
		MaxOpenFiles:         sinkCfg.DiskBatching.Backpressure.MaxOpenFiles,
		MaxTotalBytes:        sinkCfg.DiskBatching.Backpressure.MaxTotalBytes,
	}

	// Create pipeline
	pipeline := NewPipeline(cfg, kafkaSource, sinkCfg, pathPartitioner, batchProcessor, streamingProcessor, outputSink, bpConfig, tempDir)

	// Handle shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logging.InfoLog("shutdown_signal", nil)
		cancel()
	}()

	// Run pipeline
	if err := pipeline.Run(ctx); err != nil {
		logging.ErrorLog("pipeline_error", map[string]interface{}{"error": err.Error()})
		os.Exit(1)
	}

	logging.InfoLog("shutdown_complete", nil)
}

// Pipeline orchestrates the entire data flow
type Pipeline struct {
	cfg                *config.Config
	kafkaSource        *config.Source
	sinkCfg            *config.Sink
	partitioner        *partitioner.PathPartitioner
	batchProcessor     *pqwriter.BatchProcessor
	streamingProcessor *pqwriter.StreamingProcessor
	sink               sink.Sink
	bufferManager      *buffer.BufferManager
	bpConfig           buffer.BackpressureConfig
	metrics            Metrics
	prom               *metrics.Metrics // Prometheus metrics
	startTime          time.Time
	tempDir            string

	// Worker pool for message processing
	numWorkers        int
	uploadConcurrency int
	msgChan           chan *kafka.Message
	workerWg          sync.WaitGroup
	parserPool        sync.Pool
	bufferPool        sync.Pool
}

// NewPipeline creates a new pipeline
func NewPipeline(
	cfg *config.Config,
	kafkaSource *config.Source,
	sinkCfg *config.Sink,
	part *partitioner.PathPartitioner,
	batchProcessor *pqwriter.BatchProcessor,
	streamingProcessor *pqwriter.StreamingProcessor,
	s sink.Sink,
	bpConfig buffer.BackpressureConfig,
	tempDir string,
) *Pipeline {
	numWorkers := runtime.NumCPU()
	if numWorkers < 4 {
		numWorkers = 4
	}
	if numWorkers > 32 {
		numWorkers = 32
	}

	// Upload concurrency controls parallel S3 uploads within each flush
	uploadConcurrency := 8
	if sinkCfg.Request.Concurrency > 0 {
		uploadConcurrency = sinkCfg.Request.Concurrency
	}

	// Initialize Prometheus metrics
	prom := metrics.New("fanout")

	pipeline := &Pipeline{
		cfg:                cfg,
		kafkaSource:        kafkaSource,
		sinkCfg:            sinkCfg,
		partitioner:        part,
		batchProcessor:     batchProcessor,
		streamingProcessor: streamingProcessor,
		sink:               s,
		bpConfig:           bpConfig,
		startTime:          time.Now(),
		tempDir:            tempDir,
		numWorkers:         numWorkers,
		uploadConcurrency:  uploadConcurrency,
		msgChan:            make(chan *kafka.Message, numWorkers*1000),
		prom:               prom,
		parserPool: sync.Pool{
			New: func() interface{} {
				return new(fastjson.Parser)
			},
		},
		bufferPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, 16*1024))
			},
		},
		metrics: Metrics{
			lastSnapshot: time.Now(),
		},
	}

	pipeline.streamingProcessor.SetStageRecorder(pipeline.recordParquetStage)
	return pipeline
}

// Run starts the pipeline
func (p *Pipeline) Run(ctx context.Context) error {
	// Create buffer manager with backpressure
	p.bufferManager = buffer.NewBufferManagerWithBackpressure(
		p.cfg.DataDir,
		p.sinkCfg.DiskBatching.MaxEvents,
		p.sinkCfg.DiskBatching.MaxBytes,
		p.sinkCfg.DiskBatching.Timeout(),
		p.sinkCfg.DiskBatching.TimeoutSplay(),
		p.onBufferFlush,
		p.bpConfig,
	)

	// Start periodic flush
	stopFlush := make(chan struct{})
	p.bufferManager.StartPeriodicFlush(10*time.Second, stopFlush)

	// Start worker pool
	logging.InfoLog("workers_start", map[string]interface{}{"count": p.numWorkers})
	for i := 0; i < p.numWorkers; i++ {
		p.workerWg.Add(1)
		go p.messageWorker(ctx)
	}

	// Create Kafka consumer - messages go to channel
	consumer, err := kafka.NewConsumer(p.kafkaSource, p.enqueueMessage)
	if err != nil {
		return fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Start consuming
	if err := consumer.Start(); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	// Start Prometheus metrics HTTP server
	go p.startMetricsServer()

	// Start stats reporter
	stopStats := make(chan struct{})
	go p.reportStats(stopStats)

	// Wait for shutdown
	<-ctx.Done()

	logging.InfoLog("consumer_stopping", nil)
	// Stop consumer first to prevent new messages
	if err := consumer.Stop(); err != nil {
		logging.ErrorLog("consumer_stop_failed", map[string]interface{}{"error": err.Error()})
	}

	// Close message channel and wait for workers
	logging.InfoLog("workers_wait", nil)
	close(p.msgChan)
	p.workerWg.Wait()

	logging.InfoLog("flush_periodic_stop", nil)
	close(stopFlush)

	logging.InfoLog("flush_remaining", nil)
	// Stop buffer manager gracefully (flushes all + waits for workers)
	if err := p.bufferManager.Stop(); err != nil {
		logging.ErrorLog("buffer_manager_stop_failed", map[string]interface{}{"error": err.Error()})
	}

	// Wait for pending flushes to complete
	logging.InfoLog("flush_pending_wait", nil)
	if err := p.bufferManager.WaitForPendingFlushes(60 * time.Second); err != nil {
		logging.WarnLog("flush_pending_timeout", map[string]interface{}{"error": err.Error()})
	}

	// Stop stats reporting before final summary.
	close(stopStats)

	// Final stats
	p.printFinalStats()

	return nil
}

// enqueueMessage puts a message on the channel for worker processing
func (p *Pipeline) enqueueMessage(msg *kafka.Message) error {
	atomic.AddInt64(&p.metrics.messagesReceived, 1)
	atomic.AddInt64(&p.metrics.bytesReceived, int64(len(msg.Value)))
	// Update Prometheus metrics
	p.prom.MessagesReceived.Inc()
	p.prom.BytesReceived.Add(float64(len(msg.Value)))
	p.msgChan <- msg
	return nil
}

// messageWorker processes messages from the channel
// Workers drain the entire channel before exiting to ensure graceful shutdown
func (p *Pipeline) messageWorker(ctx context.Context) {
	defer p.workerWg.Done()

	// Get parser from pool
	parser := p.parserPool.Get().(*fastjson.Parser)
	defer p.parserPool.Put(parser)

	// Drain the channel completely - don't exit on context cancellation
	// This ensures all buffered messages are processed before shutdown
	for msg := range p.msgChan {
		p.processMessage(parser, msg)
	}
}

// processMessage processes a single Kafka message using fastjson
func (p *Pipeline) processMessage(parser *fastjson.Parser, msg *kafka.Message) {
	// Parse with fastjson (much faster than stdlib)
	parseStart := time.Now()
	v, err := parser.ParseBytes(msg.Value)
	if err != nil {
		atomic.AddInt64(&p.metrics.parseErrors, 1)
		p.prom.ParseErrors.Inc()
		return
	}
	parseDuration := time.Since(parseStart)
	atomic.AddInt64(&p.metrics.parseTimeNs, parseDuration.Nanoseconds())
	p.prom.ParseLatency.Observe(float64(parseDuration.Microseconds()))

	// Extract partition fields using fastjson (no full parse needed)
	metaStart := time.Now()

	// Get buffer from pool
	buf := p.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()

	// Build output JSON with metadata injected
	// This is faster than parse->modify->serialize
	buf.WriteByte('{')

	// Add Kafka metadata first
	buf.WriteString(`"_timestamp":"`)
	buf.WriteString(time.UnixMilli(msg.Timestamp).Format(time.RFC3339Nano))
	buf.WriteString(`","_offset":`)
	var numBuf [20]byte
	buf.Write(strconv.AppendInt(numBuf[:0], msg.Offset, 10))
	buf.WriteString(`,"_partition":`)
	buf.Write(strconv.AppendInt(numBuf[:0], int64(msg.Partition), 10))

	// Copy all existing fields
	obj, _ := v.Object()
	obj.Visit(func(key []byte, val *fastjson.Value) {
		buf.WriteByte(',')
		buf.WriteByte('"')
		buf.Write(key)
		buf.WriteString(`":`)
		buf.WriteString(val.String())
	})

	buf.WriteByte('}')
	data := buf.Bytes()

	metaDuration := time.Since(metaStart)
	atomic.AddInt64(&p.metrics.metadataTimeNs, metaDuration.Nanoseconds())
	p.prom.MetadataLatency.Observe(float64(metaDuration.Microseconds()))

	// Get partition path using fastjson values directly
	partStart := time.Now()
	partitionPath := p.getPartitionPathFast(v)
	partDuration := time.Since(partStart)
	atomic.AddInt64(&p.metrics.partitionTimeNs, partDuration.Nanoseconds())
	p.prom.PartitionLatency.Observe(float64(partDuration.Microseconds()))

	// Write to buffer
	bufStart := time.Now()
	if err := p.bufferManager.Write(partitionPath, data); err != nil {
		p.bufferPool.Put(buf)
		atomic.AddInt64(&p.metrics.bufferWriteErrors, 1)
		p.prom.BufferWriteErrors.Inc()
		return
	}
	p.bufferPool.Put(buf)
	bufDuration := time.Since(bufStart)
	atomic.AddInt64(&p.metrics.bufferWriteTimeNs, bufDuration.Nanoseconds())
	p.prom.BufferLatency.Observe(float64(bufDuration.Microseconds()))
	atomic.AddInt64(&p.metrics.messagesProcessed, 1)
	p.prom.MessagesProcessed.Inc()
}

// getPartitionPathFast extracts partition path using fastjson and the configured partitioner
func (p *Pipeline) getPartitionPathFast(v *fastjson.Value) string {
	// Extract field values needed for partitioning
	fieldNames := p.partitioner.GetFieldNames()
	fields := make(map[string]string, len(fieldNames))

	for _, name := range fieldNames {
		switch name {
		case "team_id":
			fields[name] = strconv.FormatInt(v.GetInt64("team_id"), 10)
		case "date":
			// Extract date from timestamp field
			if ts := v.Get("timestamp"); ts != nil {
				if tsStr := ts.GetStringBytes(); len(tsStr) >= 10 {
					// Format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
					dateStr := string(tsStr[:10])
					// Convert YYYY-MM-DD to YYYY/MM/DD
					fields[name] = dateStr[:4] + "/" + dateStr[5:7] + "/" + dateStr[8:10]
				}
			}
			if fields[name] == "" {
				fields[name] = time.Now().Format("2006/01/02")
			}
		default:
			// For other fields, try to get as string
			if val := v.Get(name); val != nil {
				switch val.Type() {
				case fastjson.TypeString:
					fields[name] = string(val.GetStringBytes())
				case fastjson.TypeNumber:
					fields[name] = strconv.FormatInt(val.GetInt64(), 10)
				default:
					fields[name] = val.String()
				}
			}
		}
	}

	return p.partitioner.GetPartitionPathFromParsed(fields)
}

// onBufferFlush is called when a buffer is ready to be flushed
func (p *Pipeline) onBufferFlush(partitionPath string, buf *buffer.DiskBuffer, reason buffer.FlushReason) error {
	flushStart := time.Now()
	ctx := context.Background()

	// Record flush reason
	p.prom.FlushByReason.WithLabelValues(string(reason)).Inc()

	// Create a unique temp output directory for this flush
	flushTempDir := filepath.Join(p.tempDir, fmt.Sprintf("flush_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(flushTempDir, 0755); err != nil {
		atomic.AddInt64(&p.metrics.flushesFailed, 1)
		p.prom.FlushesFailed.Inc()
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(flushTempDir) // Clean up temp files

	// Use streaming processor to write parquet files (splits into 10k row files)
	parquetStart := time.Now()
	parquetFiles, err := p.streamingProcessor.ProcessBufferToFiles(buf.Path(), flushTempDir)
	if err != nil {
		atomic.AddInt64(&p.metrics.flushesFailed, 1)
		p.prom.FlushesFailed.Inc()
		return fmt.Errorf("failed to process buffer: %w", err)
	}
	parquetDuration := time.Since(parquetStart)
	atomic.AddInt64(&p.metrics.parquetWriteTimeNs, parquetDuration.Nanoseconds())
	p.prom.ParquetWriteLatency.Observe(float64(parquetDuration.Milliseconds()))

	// Write parquet files to sink in parallel
	sinkStart := time.Now()
	var totalBytes int64
	var totalRows int64
	var uploadErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	sem := make(chan struct{}, p.uploadConcurrency)

	for _, fileInfo := range parquetFiles {
		wg.Add(1)
		go func(pqInfo pqwriter.ParquetFileInfo) {
			defer wg.Done()

			// Acquire semaphore slot
			sem <- struct{}{}
			defer func() { <-sem }()

			// Check if another upload already failed
			if uploadErr != nil {
				return
			}

			file, err := os.Open(pqInfo.Path)
			if err != nil {
				errOnce.Do(func() { uploadErr = fmt.Errorf("failed to open parquet file: %w", err) })
				return
			}
			info, err := file.Stat()
			if err != nil {
				file.Close()
				errOnce.Do(func() { uploadErr = fmt.Errorf("failed to stat parquet file: %w", err) })
				return
			}
			p.prom.FileSizeBytes.Observe(float64(info.Size()))

			// Generate sink path
			filename := filepath.Base(pqInfo.Path)
			fullPath := filepath.Join(partitionPath, filename)

			// Write to sink
			if err := p.sink.WriteFromReader(ctx, fullPath, file, info.Size()); err != nil {
				file.Close()
				errOnce.Do(func() { uploadErr = fmt.Errorf("failed to write to sink: %w", err) })
				return
			}
			if err := file.Close(); err != nil {
				logging.WarnLog("parquet_close_failed", map[string]interface{}{"error": err.Error(), "path": pqInfo.Path})
			}
			atomic.AddInt64(&p.metrics.filesUploaded, 1)
			atomic.AddInt64(&totalBytes, info.Size())
			atomic.AddInt64(&totalRows, int64(pqInfo.RowCount))
		}(fileInfo)
	}
	wg.Wait()

	if uploadErr != nil {
		atomic.AddInt64(&p.metrics.flushesFailed, 1)
		p.prom.FlushesFailed.Inc()
		return uploadErr
	}
	sinkDuration := time.Since(sinkStart)
	atomic.AddInt64(&p.metrics.sinkWriteTimeNs, sinkDuration.Nanoseconds())
	p.prom.SinkWriteLatency.Observe(float64(sinkDuration.Milliseconds()))
	atomic.AddInt64(&p.metrics.parquetBytesOut, totalBytes)
	atomic.AddInt64(&p.metrics.rowsWritten, totalRows)
	p.prom.BytesWritten.Add(float64(totalBytes))
	p.prom.FilesUploaded.Add(float64(len(parquetFiles)))
	p.prom.RowsWritten.Add(float64(totalRows))
	atomic.AddInt64(&p.metrics.flushesCompleted, 1)
	p.prom.FlushesCompleted.Inc()
	flushDuration := time.Since(flushStart)
	atomic.AddInt64(&p.metrics.flushTimeNs, flushDuration.Nanoseconds())
	p.prom.TotalFlushLatency.Observe(float64(flushDuration.Milliseconds()))

	// Delete buffer file
	if err := buf.Delete(); err != nil {
		logging.WarnLog("buffer_delete_failed", map[string]interface{}{"error": err.Error(), "path": buf.Path()})
	}

	return nil
}

// startMetricsServer starts the Prometheus HTTP metrics server
func (p *Pipeline) startMetricsServer() {
	http.Handle("/metrics", metrics.Handler())
	port := strconv.Itoa(p.cfg.MetricsPort)
	if _, err := strconv.Atoi(port); err != nil {
		logging.ErrorLog("metrics_server_invalid_port", map[string]interface{}{"port": port, "error": err.Error()})
		port = strconv.Itoa(p.cfg.MetricsPort)
	}
	logging.InfoLog("metrics_server_start", map[string]interface{}{"port": port})
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		logging.ErrorLog("metrics_server_failed", map[string]interface{}{"error": err.Error()})
	}
}

// reportStats periodically reports pipeline statistics (once per second)
func (p *Pipeline) reportStats(stop <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.updatePrometheusGauges()
			p.printDetailedStats()
		case <-stop:
			return
		}
	}
}

func (p *Pipeline) recordParquetStage(stage string, wallNs int64, cpuNs int64) {
	switch stage {
	case pqwriter.ParquetStageFile:
		atomic.AddInt64(&p.metrics.parquetFiles, 1)
	case pqwriter.ParquetStageJSON:
		atomic.AddInt64(&p.metrics.parquetJSONWallNs, wallNs)
		atomic.AddInt64(&p.metrics.parquetJSONCPUNs, cpuNs)
	case pqwriter.ParquetStageSchema:
		atomic.AddInt64(&p.metrics.parquetSchemaWallNs, wallNs)
		atomic.AddInt64(&p.metrics.parquetSchemaCPUNs, cpuNs)
	case pqwriter.ParquetStageWriterInit:
		atomic.AddInt64(&p.metrics.parquetInitWallNs, wallNs)
		atomic.AddInt64(&p.metrics.parquetInitCPUNs, cpuNs)
	case pqwriter.ParquetStageRowConvert:
		atomic.AddInt64(&p.metrics.parquetConvWallNs, wallNs)
		atomic.AddInt64(&p.metrics.parquetConvCPUNs, cpuNs)
	case pqwriter.ParquetStageRowWrite:
		atomic.AddInt64(&p.metrics.parquetWriteWallNs, wallNs)
		atomic.AddInt64(&p.metrics.parquetWriteCPUNs, cpuNs)
	case pqwriter.ParquetStageWriterClose:
		atomic.AddInt64(&p.metrics.parquetCloseWallNs, wallNs)
		atomic.AddInt64(&p.metrics.parquetCloseCPUNs, cpuNs)
	}
}

// updatePrometheusGauges updates all gauge-type Prometheus metrics
func (p *Pipeline) updatePrometheusGauges() {
	// Buffer stats
	bufStats := p.bufferManager.Stats()
	p.prom.ActiveBuffers.Set(float64(bufStats.ActiveBuffers))
	p.prom.TotalBufferBytes.Set(float64(bufStats.TotalBytes))
	p.prom.PendingFlushes.Set(float64(bufStats.PendingFlushes))
	p.prom.OpenFileHandles.Set(float64(bufStats.OpenFileHandles))

	// Backpressure status
	if p.bufferManager.IsPressured() {
		p.prom.BackpressureActive.Set(1)
	} else {
		p.prom.BackpressureActive.Set(0)
	}

	// Runtime metrics
	p.prom.UpdateRuntimeMetrics()

	// Calculate rates
	now := time.Now()
	elapsed := now.Sub(p.metrics.lastSnapshot).Seconds()
	if elapsed > 0.1 {
		msgRecv := atomic.LoadInt64(&p.metrics.messagesReceived)
		bytesIn := atomic.LoadInt64(&p.metrics.bytesReceived)
		bytesOut := atomic.LoadInt64(&p.metrics.parquetBytesOut)
		flushOk := atomic.LoadInt64(&p.metrics.flushesCompleted)

		p.prom.MessageRate.Set(float64(msgRecv-p.metrics.lastMsgCount) / elapsed)
		p.prom.BytesInRate.Set(float64(bytesIn-p.metrics.lastBytesIn) / elapsed)
		p.prom.BytesOutRate.Set(float64(bytesOut-p.metrics.lastBytesOut) / elapsed)
		p.prom.FlushRate.Set(float64(flushOk-p.metrics.lastFlushCount) / elapsed)
	}
}

// printDetailedStats prints comprehensive benchmark information
func (p *Pipeline) printDetailedStats() {
	now := time.Now()
	uptime := now.Sub(p.startTime)

	// Get current metrics (atomic reads)
	msgRecv := atomic.LoadInt64(&p.metrics.messagesReceived)
	msgProc := atomic.LoadInt64(&p.metrics.messagesProcessed)
	bytesIn := atomic.LoadInt64(&p.metrics.bytesReceived)
	bytesOut := atomic.LoadInt64(&p.metrics.parquetBytesOut)
	rowsOut := atomic.LoadInt64(&p.metrics.rowsWritten)
	parseErrs := atomic.LoadInt64(&p.metrics.parseErrors)
	bufErrs := atomic.LoadInt64(&p.metrics.bufferWriteErrors)
	flushOk := atomic.LoadInt64(&p.metrics.flushesCompleted)
	flushFail := atomic.LoadInt64(&p.metrics.flushesFailed)
	filesUploaded := atomic.LoadInt64(&p.metrics.filesUploaded)

	// Get timing metrics (convert to milliseconds for readability)
	parseMs := float64(atomic.LoadInt64(&p.metrics.parseTimeNs)) / 1e6
	metaMs := float64(atomic.LoadInt64(&p.metrics.metadataTimeNs)) / 1e6
	partMs := float64(atomic.LoadInt64(&p.metrics.partitionTimeNs)) / 1e6
	bufWriteMs := float64(atomic.LoadInt64(&p.metrics.bufferWriteTimeNs)) / 1e6
	flushMs := float64(atomic.LoadInt64(&p.metrics.flushTimeNs)) / 1e6
	parquetMs := float64(atomic.LoadInt64(&p.metrics.parquetWriteTimeNs)) / 1e6
	sinkMs := float64(atomic.LoadInt64(&p.metrics.sinkWriteTimeNs)) / 1e6

	// Calculate rates since last snapshot
	elapsed := now.Sub(p.metrics.lastSnapshot).Seconds()
	if elapsed < 0.1 {
		elapsed = 1.0 // Avoid division issues on first call
	}

	msgRate := float64(msgRecv-p.metrics.lastMsgCount) / elapsed
	bytesInRate := float64(bytesIn-p.metrics.lastBytesIn) / elapsed
	bytesOutRate := float64(bytesOut-p.metrics.lastBytesOut) / elapsed
	flushRate := float64(flushOk-p.metrics.lastFlushCount) / elapsed

	// Update snapshot
	p.metrics.lastSnapshot = now
	p.metrics.lastMsgCount = msgRecv
	p.metrics.lastBytesIn = bytesIn
	p.metrics.lastBytesOut = bytesOut
	p.metrics.lastFlushCount = flushOk

	// Get memory stats
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	// Get buffer manager stats
	bufStats := p.bufferManager.Stats()

	// Calculate average time per message (if we have messages)
	avgParseUs := float64(0)
	avgMetaUs := float64(0)
	avgPartUs := float64(0)
	avgBufUs := float64(0)
	if msgProc > 0 {
		avgParseUs = (parseMs * 1000) / float64(msgProc)
		avgMetaUs = (metaMs * 1000) / float64(msgProc)
		avgPartUs = (partMs * 1000) / float64(msgProc)
		avgBufUs = (bufWriteMs * 1000) / float64(msgProc)
	}

	// Calculate average flush time
	avgFlushMs := float64(0)
	avgParquetMs := float64(0)
	avgSinkMs := float64(0)
	if flushOk > 0 {
		avgFlushMs = flushMs / float64(flushOk)
		avgParquetMs = parquetMs / float64(flushOk)
		avgSinkMs = sinkMs / float64(flushOk)
	}

	// Backpressure indicator
	pressureStr := ""
	if p.bufferManager.IsPressured() {
		pressureStr = " [BACKPRESSURE]"
	}

	avgMsgRate := float64(0)
	if uptime.Seconds() > 0 {
		avgMsgRate = float64(msgRecv) / uptime.Seconds()
	}
	avgFileSizeMB := float64(0)
	if filesUploaded > 0 {
		avgFileSizeMB = float64(bytesOut) / float64(filesUploaded) / (1024 * 1024)
	}

	// Print stats
	fmt.Println("─────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("BENCHMARK [uptime=%s]%s\n", uptime.Round(time.Second), pressureStr)
	fmt.Println("─────────────────────────────────────────────────────────────────────────────")

	// Throughput
	fmt.Printf("THROUGHPUT: %.0f msg/s | in=%.1f MB/s | out=%.1f MB/s | flushes=%.1f/s\n",
		msgRate,
		bytesInRate/(1024*1024),
		bytesOutRate/(1024*1024),
		flushRate,
	)
	fmt.Printf("AVERAGES:   %.0f msg/s | file=%.2f MB\n", avgMsgRate, avgFileSizeMB)

	// Totals
	fmt.Printf("TOTALS:     recv=%d proc=%d rows=%d | in=%.1f MB out=%.1f MB | flushes=%d (fail=%d)\n",
		msgRecv,
		msgProc,
		rowsOut,
		float64(bytesIn)/(1024*1024),
		float64(bytesOut)/(1024*1024),
		flushOk,
		flushFail,
	)

	// Per-message timing (microseconds)
	fmt.Printf("MSG TIME:   parse=%.1fµs meta=%.1fµs partition=%.1fµs buffer=%.1fµs | total=%.1fµs\n",
		avgParseUs,
		avgMetaUs,
		avgPartUs,
		avgBufUs,
		avgParseUs+avgMetaUs+avgPartUs+avgBufUs,
	)

	// Per-flush timing (milliseconds)
	fmt.Printf("FLUSH TIME: parquet=%.1fms sink=%.1fms | total=%.1fms\n",
		avgParquetMs,
		avgSinkMs,
		avgFlushMs,
	)

	filesWritten := atomic.LoadInt64(&p.metrics.parquetFiles)
	if filesWritten > 0 {
		avgStageMs := func(ns int64) float64 {
			return float64(ns) / 1e6 / float64(filesWritten)
		}
		fmt.Printf("PARQUET:    json=%.1f/%.1f schema=%.1f/%.1f init=%.1f/%.1f convert=%.1f/%.1f write=%.1f/%.1f close=%.1f/%.1f ms (wall/cpu per file)\n",
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetJSONWallNs)),
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetJSONCPUNs)),
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetSchemaWallNs)),
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetSchemaCPUNs)),
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetInitWallNs)),
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetInitCPUNs)),
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetConvWallNs)),
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetConvCPUNs)),
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetWriteWallNs)),
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetWriteCPUNs)),
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetCloseWallNs)),
			avgStageMs(atomic.LoadInt64(&p.metrics.parquetCloseCPUNs)),
		)
	}

	// Memory
	fmt.Printf("MEMORY:     alloc=%.1f MB sys=%.1f MB | heap=%.1f MB (objects=%d) | gc=%d\n",
		float64(mem.Alloc)/(1024*1024),
		float64(mem.Sys)/(1024*1024),
		float64(mem.HeapAlloc)/(1024*1024),
		mem.HeapObjects,
		mem.NumGC,
	)

	// Buffers
	fmt.Printf("BUFFERS:    active=%d total=%.1f MB | pending=%d blocked=%d | handles=%d evicts=%d\n",
		bufStats.ActiveBuffers,
		float64(bufStats.TotalBytes)/(1024*1024),
		bufStats.PendingFlushes,
		bufStats.BlockedWrites,
		bufStats.OpenFileHandles,
		bufStats.FileHandleEvicts,
	)

	// Errors
	if parseErrs > 0 || bufErrs > 0 {
		fmt.Printf("ERRORS:     parse=%d buffer=%d\n", parseErrs, bufErrs)
	}

	// Goroutines
	fmt.Printf("RUNTIME:    goroutines=%d | numCPU=%d\n",
		runtime.NumGoroutine(),
		runtime.NumCPU(),
	)
}

// printFinalStats prints final statistics on shutdown
func (p *Pipeline) printFinalStats() {
	uptime := time.Since(p.startTime)
	bufStats := p.bufferManager.Stats()

	// Get final metrics
	msgRecv := atomic.LoadInt64(&p.metrics.messagesReceived)
	msgProc := atomic.LoadInt64(&p.metrics.messagesProcessed)
	bytesIn := atomic.LoadInt64(&p.metrics.bytesReceived)
	bytesOut := atomic.LoadInt64(&p.metrics.parquetBytesOut)
	rowsOut := atomic.LoadInt64(&p.metrics.rowsWritten)
	parseErrs := atomic.LoadInt64(&p.metrics.parseErrors)
	bufErrs := atomic.LoadInt64(&p.metrics.bufferWriteErrors)
	flushOk := atomic.LoadInt64(&p.metrics.flushesCompleted)
	flushFail := atomic.LoadInt64(&p.metrics.flushesFailed)
	filesUploaded := atomic.LoadInt64(&p.metrics.filesUploaded)

	// Get timing totals
	parseMs := float64(atomic.LoadInt64(&p.metrics.parseTimeNs)) / 1e6
	metaMs := float64(atomic.LoadInt64(&p.metrics.metadataTimeNs)) / 1e6
	partMs := float64(atomic.LoadInt64(&p.metrics.partitionTimeNs)) / 1e6
	bufWriteMs := float64(atomic.LoadInt64(&p.metrics.bufferWriteTimeNs)) / 1e6
	flushMs := float64(atomic.LoadInt64(&p.metrics.flushTimeNs)) / 1e6
	parquetMs := float64(atomic.LoadInt64(&p.metrics.parquetWriteTimeNs)) / 1e6
	sinkMs := float64(atomic.LoadInt64(&p.metrics.sinkWriteTimeNs)) / 1e6

	// Get memory stats
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	fmt.Println()
	fmt.Println("═════════════════════════════════════════════════════════════════════════════")
	fmt.Println("                           FINAL STATISTICS")
	fmt.Println("═════════════════════════════════════════════════════════════════════════════")
	fmt.Printf("Runtime:            %s\n", uptime.Round(time.Second))
	fmt.Println("─────────────────────────────────────────────────────────────────────────────")
	fmt.Println("MESSAGES:")
	fmt.Printf("  Received:         %d\n", msgRecv)
	fmt.Printf("  Processed:        %d\n", msgProc)
	fmt.Printf("  Avg throughput:   %.0f msg/s\n", float64(msgRecv)/uptime.Seconds())
	if filesUploaded > 0 {
		fmt.Printf("  Avg file size:    %.2f MB\n", float64(bytesOut)/float64(filesUploaded)/(1024*1024))
	}
	fmt.Println("─────────────────────────────────────────────────────────────────────────────")
	fmt.Println("DATA:")
	fmt.Printf("  Bytes in:         %.2f MB\n", float64(bytesIn)/(1024*1024))
	fmt.Printf("  Bytes out:        %.2f MB\n", float64(bytesOut)/(1024*1024))
	fmt.Printf("  Rows out:         %d\n", rowsOut)
	fmt.Printf("  Compression:      %.1f%%\n", (1-float64(bytesOut)/float64(bytesIn))*100)
	fmt.Printf("  Avg in rate:      %.2f MB/s\n", float64(bytesIn)/(1024*1024)/uptime.Seconds())
	fmt.Printf("  Avg out rate:     %.2f MB/s\n", float64(bytesOut)/(1024*1024)/uptime.Seconds())
	fmt.Printf("  Avg rows out:     %.2f rows/s\n", float64(rowsOut)/uptime.Seconds())
	fmt.Println("─────────────────────────────────────────────────────────────────────────────")
	fmt.Println("FLUSHES:")
	fmt.Printf("  Completed:        %d\n", flushOk)
	fmt.Printf("  Failed:           %d\n", flushFail)
	fmt.Printf("  Blocked writes:   %d\n", bufStats.BlockedWrites)
	fmt.Printf("  Handle evicts:    %d\n", bufStats.FileHandleEvicts)
	fmt.Println("─────────────────────────────────────────────────────────────────────────────")
	fmt.Println("CPU TIME (total across all messages/flushes):")
	fmt.Printf("  JSON parse:       %.2f s\n", parseMs/1000)
	fmt.Printf("  Add metadata:     %.2f s\n", metaMs/1000)
	fmt.Printf("  Partitioning:     %.2f s\n", partMs/1000)
	fmt.Printf("  Buffer write:     %.2f s\n", bufWriteMs/1000)
	fmt.Printf("  Parquet encode:   %.2f s\n", parquetMs/1000)
	fmt.Printf("  Sink write:       %.2f s\n", sinkMs/1000)
	fmt.Printf("  Total flush:      %.2f s\n", flushMs/1000)
	fmt.Println("─────────────────────────────────────────────────────────────────────────────")
	fmt.Println("ERRORS:")
	fmt.Printf("  Parse errors:     %d\n", parseErrs)
	fmt.Printf("  Buffer errors:    %d\n", bufErrs)
	fmt.Println("─────────────────────────────────────────────────────────────────────────────")
	fmt.Println("MEMORY (final):")
	fmt.Printf("  Alloc:            %.2f MB\n", float64(mem.Alloc)/(1024*1024))
	fmt.Printf("  Sys:              %.2f MB\n", float64(mem.Sys)/(1024*1024))
	fmt.Printf("  Heap:             %.2f MB\n", float64(mem.HeapAlloc)/(1024*1024))
	fmt.Printf("  GC cycles:        %d\n", mem.NumGC)
	fmt.Println("═════════════════════════════════════════════════════════════════════════════")
}
