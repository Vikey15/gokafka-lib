package consumer

import (
    "context"
    "fmt"
    "github.com/segmentio/kafka-go"
    "time"
)

type KafkaConsumer struct {
    reader *kafka.Reader
}

func NewKafkaConsumer(brokers []string, topic string, groupID string) *KafkaConsumer {
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:     brokers,
        GroupID:     groupID,
        Topic:       topic,
        StartOffset: kafka.FirstOffset,
    })
    return &KafkaConsumer{reader: reader}
}

func (kc *KafkaConsumer) StartConsuming(handler func(key, value []byte)) {
    for {
        msg, err := kc.reader.ReadMessage(context.Background())
        if err != nil {
            fmt.Println("Consumer error:", err)
            time.Sleep(1 * time.Second)
            continue
        }
        handler(msg.Key, msg.Value)
    }
}

func (kc *KafkaConsumer) Close() {
    _ = kc.reader.Close()
}
