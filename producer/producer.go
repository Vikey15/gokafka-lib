package producer

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
)

// KafkaProducer wraps a Sarama SyncProducer.
type KafkaProducer struct {
	producer sarama.SyncProducer
	topic    string
}

// NewKafkaProducer creates a SyncProducer that writes to `topic`.
func NewKafkaProducer(brokers []string, topic string) (*KafkaProducer, error) {
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
	}, nil
}

func (kp *KafkaProducer) SendMessage(responseData string) (int32, int64, error) {
	msg := &sarama.ProducerMessage{
		Topic: kp.topic,
		Value: sarama.StringEncoder(responseData),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("content-type"),
				Value: []byte("text/plain"),
			},
		},
	}

	partition, offset, err := kp.producer.SendMessage(msg)
	if err != nil {
		return 0, 0, err
	}
	return partition, offset, nil
}

// Close shuts down the producer and flushes any pending messages.
func (kp *KafkaProducer) Close() error {
	return kp.producer.Close()
}
