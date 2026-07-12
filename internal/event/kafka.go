package event

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

// KafkaPublisher writes DecisionEvents to Kafka.
//
// The underlying writer runs in Async mode: WriteMessages enqueues the message
// and returns immediately, so the request path never blocks on a broker round
// trip. Because Async discards the per-write error return, delivery failures
// are surfaced through the Completion callback (logged here) instead.
//
// Messages are keyed by UserID and partitioned with a Hash balancer so every
// event for a user lands on the same partition, preserving per-user ordering.
type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			Async:                  true,
			AllowAutoTopicCreation: true,
			Completion: func(msgs []kafka.Message, err error) {
				if err != nil {
					slog.Error("kafka publish failed", "topic", topic, "messages", len(msgs), "err", err)
					return
				}
				slog.Debug("kafka publish delivered", "topic", topic, "messages", len(msgs))
			},
		},
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, e DecisionEvent) error {
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.UserID),
		Value: payload,
	})
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
