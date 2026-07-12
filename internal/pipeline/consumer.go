package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/segmentio/kafka-go"

	"github.com/Antrikshgwal/Vergil/internal/audit"
	"github.com/Antrikshgwal/Vergil/internal/event"
)

// Consumer reads DecisionEvents from Kafka and persists them via an AuditStore.
//
// Offsets are committed manually. FetchMessage returns a message without moving
// the committed offset, and CommitMessages runs only after the event has been
// durably saved. That ordering is at-least-once: a crash between save and
// commit replays the message, which the idempotent upsert (added in 4.3) makes
// safe to reprocess.
type Consumer struct {
	reader *kafka.Reader
	store  audit.AuditStore
}

// NewConsumer builds a group consumer. CommitInterval is left at zero so the
// reader never auto-commits — offsets advance only through CommitMessages.
func NewConsumer(brokers []string, topic, groupID string, store audit.AuditStore) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		}),
		store: store,
	}
}

// Run consumes until ctx is cancelled. Returns nil on a clean cancellation and
// an error if fetching, saving, or committing fails.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		var e event.DecisionEvent
		if err := json.Unmarshal(m.Value, &e); err != nil {
			// Poison message: it will never decode, so commit past it instead
			// of blocking the partition forever.
			slog.Error("skip malformed message",
				"partition", m.Partition, "offset", m.Offset, "err", err)
			if err := c.reader.CommitMessages(ctx, m); err != nil {
				return err
			}
			continue
		}

		if err := c.store.Save(ctx, e); err != nil {
			// Do not commit: the offset stays put so the message is redelivered
			// after a restart. Fail fast here; retry/backpressure is later work.
			slog.Error("save failed, not committing", "txn_id", e.TxnID, "err", err)
			return err
		}

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			return err
		}
		slog.Info("audit persisted",
			"txn_id", e.TxnID, "classification", e.Classification,
			"partition", m.Partition, "offset", m.Offset)
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
