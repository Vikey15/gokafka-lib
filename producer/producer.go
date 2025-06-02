package producer

import (
	"log"
	"time"

	"github.com/IBM/sarama"
)

// KafkaProducer wraps a Sarama SyncProducer.
type KafkaProducer struct {
	producer sarama.SyncProducer
	topic    string
}

// NewKafkaProducer creates a SyncProducer that writes to `topic`.
func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	config := sarama.NewConfig()
	// Wait for all in‐sync replicas to ack the message (durability)
	config.Producer.RequiredAcks = sarama.WaitForAll
	// Use a hash partitioner so messages with the same key go to the same partition
	config.Producer.Partitioner = sarama.NewHashPartitioner
	// Return successes so SendMessage blocks until broker ack
	config.Producer.Return.Successes = true
	// Optionally tune write timeout
	config.Producer.Timeout = 10 * time.Second

	syncProd, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create Sarama SyncProducer: %v", err)
	}

	return &KafkaProducer{
		producer: syncProd,
		topic:    topic,
	}
}

// SendMessage sends a single message with given key and value.
// Blocks until Kafka acks or returns an error.
func (kp *KafkaProducer) SendMessage(key, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: kp.topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	partition, offset, err := kp.producer.SendMessage(msg)
	if err != nil {
		return err
	}
	log.Printf("Message stored in topic=%s partition=%d offset=%d\n", kp.topic, partition, offset)
	return nil
}

// Close shuts down the producer and flushes any pending messages.
func (kp *KafkaProducer) Close() {
	if err := kp.producer.Close(); err != nil {
		log.Printf("Error closing Sarama SyncProducer: %v", err)
	}
}
