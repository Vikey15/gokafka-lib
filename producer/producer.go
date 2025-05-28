package producer

import (
    "context"
    "github.com/segmentio/kafka-go"
    "time"
)

type KafkaProducer struct {
    writer *kafka.Writer
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
    writer := &kafka.Writer{
        Addr:         kafka.TCP(brokers...),
        Topic:        topic,
        Balancer:     &kafka.LeastBytes{},
        WriteTimeout: 10 * time.Second,
    }
    return &KafkaProducer{writer: writer}
}

func (kp *KafkaProducer) SendMessage(key, value []byte) error {
    return kp.writer.WriteMessages(context.Background(), kafka.Message{
        Key:   key,
        Value: value,
    })
}

func (kp *KafkaProducer) Close() {
    _ = kp.writer.Close()
}
