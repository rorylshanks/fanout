package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"fanout/internal/config"
	"fanout/internal/logging"
)

// Message represents a Kafka message
type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
	Timestamp int64
}

// MessageHandler is called for each message consumed
type MessageHandler func(msg *Message) error

// Consumer wraps a Kafka consumer group
type Consumer struct {
	client   sarama.ConsumerGroup
	handler  MessageHandler
	topics   []string
	ready    chan bool
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

const (
	initialRetryBackoff = 1 * time.Second
	maxRetryBackoff     = 30 * time.Second
	startupReadyTimeout = 2 * time.Minute
)

// NewConsumer creates a new Kafka consumer
func NewConsumer(src *config.Source, handler MessageHandler) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Return.Errors = true

	// Set offset reset policy
	switch src.AutoOffsetReset {
	case "earliest":
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "latest":
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	channelBufferSize := src.ChannelBufferSize
	if channelBufferSize <= 0 {
		channelBufferSize = 64
	}
	fetchDefaultBytes := src.FetchDefaultBytes
	if fetchDefaultBytes <= 0 {
		fetchDefaultBytes = 256 * 1024
	}
	fetchMaxBytes := src.FetchMaxBytes
	if fetchMaxBytes <= 0 {
		fetchMaxBytes = 2 * 1024 * 1024
	}
	if fetchMaxBytes < fetchDefaultBytes {
		fetchMaxBytes = fetchDefaultBytes
	}
	maxProcessingMs := src.MaxProcessingMs
	if maxProcessingMs <= 0 {
		maxProcessingMs = 100
	}

	cfg.ChannelBufferSize = channelBufferSize
	cfg.Consumer.Fetch.Default = int32(fetchDefaultBytes)
	cfg.Consumer.Fetch.Max = int32(fetchMaxBytes)
	cfg.Consumer.MaxProcessingTime = time.Duration(maxProcessingMs) * time.Millisecond
	cfg.Net.DialTimeout = 10 * time.Second
	cfg.Net.ReadTimeout = 30 * time.Second
	cfg.Net.WriteTimeout = 30 * time.Second

	// Configure TLS if enabled
	if src.TLS.Enabled {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	brokers := config.ParseBootstrapServers(src.BootstrapServers)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no bootstrap servers configured")
	}

	client, err := sarama.NewConsumerGroup(brokers, src.GroupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		client:  client,
		handler: handler,
		topics:  src.Topics,
		ready:   make(chan bool),
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// Start begins consuming messages
func (c *Consumer) Start() error {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		backoff := initialRetryBackoff
		for {
			handler := &consumerGroupHandler{
				handler: c.handler,
				ready:   c.ready,
			}

			if err := c.client.Consume(c.ctx, c.topics, handler); err != nil {
				if c.ctx.Err() != nil {
					return
				}
				logging.ErrorLog("kafka_consume_error", map[string]interface{}{"error": err.Error()})
				time.Sleep(backoff)
				backoff *= 2
				if backoff > maxRetryBackoff {
					backoff = maxRetryBackoff
				}
				continue
			}

			if c.ctx.Err() != nil {
				return
			}

			backoff = initialRetryBackoff
			c.ready = make(chan bool)
		}
	}()

	// Wait until consumer is ready
	select {
	case <-c.ready:
	case <-time.After(startupReadyTimeout):
		return fmt.Errorf("kafka consumer start timeout after %s", startupReadyTimeout)
	}
	logging.InfoLog("kafka_consumer_ready", nil)

	// Handle errors in background
	go func() {
		for err := range c.client.Errors() {
			logging.ErrorLog("kafka_consumer_error", map[string]interface{}{"error": err.Error()})
		}
	}()

	return nil
}

// Stop gracefully stops the consumer
func (c *Consumer) Stop() error {
	c.cancel()
	c.wg.Wait()
	return c.client.Close()
}

// Ready returns a channel that's closed when consumer is ready
func (c *Consumer) Ready() <-chan bool {
	return c.ready
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	handler MessageHandler
	ready   chan bool
}

func (h *consumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *consumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		m := &Message{
			Key:       msg.Key,
			Value:     msg.Value,
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp.UnixMilli(),
		}

		if err := h.handler(m); err != nil {
			logging.ErrorLog("kafka_message_handle_failed", map[string]interface{}{"error": err.Error()})
			continue
		}

		sess.MarkMessage(msg, "")
	}
	return nil
}
