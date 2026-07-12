package pipeline

import (
	"context"
	"errors"

	"github.com/segmentio/kafka-go"
)

// EnsureTopic creates the topic if it does not already exist, so the consumer
// can start before any producer has published. Without this, a group reader
// that joins while the topic is absent gets an empty partition assignment and
// sits idle even after the topic is later auto-created by the first publish.
//
// An "already exists" result is treated as success, making this safe to call on
// every startup.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		}},
	})
	if err != nil {
		return err
	}
	if terr := resp.Errors[topic]; terr != nil && !errors.Is(terr, kafka.TopicAlreadyExists) {
		return terr
	}
	return nil
}
