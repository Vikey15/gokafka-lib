package consumer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// KafkaConsumer wraps the Sarama ConsumerGroup.
type KafkaConsumer struct {
	group sarama.ConsumerGroup
	topic string
}

// NewKafkaConsumer creates and returns a Sarama ConsumerGroup that will join `groupID` on `topic`.
func NewKafkaConsumer(brokers []string, topic string, groupID string) *KafkaConsumer {
	config := sarama.NewConfig()
	// Specify the protocol version (must match your Kafka cluster).
	// Adjust this constant if your broker is newer—e.g. sarama.V3_0_0_0, etc.
	config.Version = sarama.V2_5_0_0

	// Return errors from the consumer to the Errors() channel
	config.Consumer.Return.Errors = true

	// Enable auto‐commit; commit intervals
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Failed to create Sarama ConsumerGroup: %v", err)
	}

	return &KafkaConsumer{
		group: group,
		topic: topic,
	}
}

// StartConsuming begins a blocking loop. For each incoming message, it calls `handler(key,value)`.
func (kc *KafkaConsumer) StartConsuming(handler func(key, value []byte)) {
	// Set up a cancellable context to handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Trap SIGINT / SIGTERM to cancel context and exit
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// Build our Sarama ConsumerGroupHandler
	saramaHandler := &consumerGroupHandler{handlerFunc: handler}

	// 1) Run the main consumption loop in a goroutine
	go func() {
		for {
			if err := kc.group.Consume(ctx, []string{kc.topic}, saramaHandler); err != nil {
				log.Printf("Error from consumer: %v", err)
				time.Sleep(1 * time.Second)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// 2) Read from the Errors() channel in a separate goroutine
	go func() {
		for err := range kc.group.Errors() {
			log.Printf("Consumer group error: %v", err)
		}
	}()

	// 3) Block until a termination signal is received
	<-sigterm
	fmt.Println("KafkaConsumer shutting down...")
	cancel()

	// Give Sarama a brief moment to clean up
	time.Sleep(500 * time.Millisecond)
	if err := kc.group.Close(); err != nil {
		log.Printf("Error closing Sarama ConsumerGroup: %v", err)
	}
}

// Close manually shuts down the consumer group (if you want to stop from code).
func (kc *KafkaConsumer) Close() {
	if err := kc.group.Close(); err != nil {
		log.Printf("Error closing Sarama ConsumerGroup: %v", err)
	}
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	handlerFunc func(key, value []byte)
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim is called once per partition assigned to this consumer.
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.handlerFunc(msg.Key, msg.Value)
		session.MarkMessage(msg, "")
	}
	return nil
}
