// consumer/kafka_consumer.go
package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

// Config holds all Kafka config values.
type Config struct {
	Brokers []string
	GroupID string
	Topics  []string
	Version sarama.KafkaVersion
}

// KafkaConsumer wraps the Sarama consumer group.
type KafkaConsumer struct {
	group  sarama.ConsumerGroup
	topics []string
}

// NewKafkaConsumer creates a new consumer with the given config.
func NewKafkaConsumer(cfg Config) (*KafkaConsumer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = cfg.Version
	saramaCfg.Consumer.Return.Errors = true

	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	return &KafkaConsumer{group: group, topics: cfg.Topics}, nil
}

// Subscribe starts consuming with the provided handler and blocks until context is done.
func (kc *KafkaConsumer) Subscribe(ctx context.Context, handler sarama.ConsumerGroupHandler) error {
	defer kc.group.Close()

	// capture errors
	go func() {
		for err := range kc.group.Errors() {
			log.Printf("Kafka consumer error: %v", err)
		}
	}()

	for {
		if err := kc.group.Consume(ctx, kc.topics, handler); err != nil {
			log.Printf("Error consuming messages: %v", err)
			time.Sleep(time.Second)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
