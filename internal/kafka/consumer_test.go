package kafka

import (
	"testing"

	"fanout/internal/config"
)

func TestNewConsumerRequiresBootstrapServers(t *testing.T) {
	src := &config.Source{
		Type:             "kafka",
		Topics:           []string{"events"},
		GroupID:          "group1",
		BootstrapServers: "  \n",
	}

	_, err := NewConsumer(src, func(msg *Message) error { return nil })
	if err == nil {
		t.Fatal("expected error for missing bootstrap servers")
	}
}
