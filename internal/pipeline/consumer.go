package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Antrikshgwal/Vergil/internal/audit"
	"github.com/Antrikshgwal/Vergil/internal/event"
)

const (
	// batchFillTimeout bounds how long we wait to top up a batch after the
	// first message arrives. It trades a little latency for larger, cheaper
	// commits.
	batchFillTimeout = 50 * time.Millisecond

	// drainTimeout caps how long a batch already in flight may take to finish
	// saving and committing after a shutdown signal, so a graceful stop still
	// terminates promptly.
	drainTimeout = 30 * time.Second
)

// Consumer reads DecisionEvents from Kafka and persists them via an AuditStore
// using a hand-rolled bounded worker pool.
//
// Each iteration fetches a batch of up to batchSize messages, fans them out to
// workers over a dispatch channel, and waits on a WaitGroup barrier until every
// save has completed. Only then are the batch's offsets committed — commit
// strictly follows the Postgres writes. If any save fails the batch is left
// uncommitted, so it is redelivered and reprocessed (made safe by the
// idempotent upsert in 4.3). This is at-least-once delivery.
type Consumer struct {
	reader    *kafka.Reader
	store     audit.AuditStore
	workers   int
	batchSize int
}

// NewConsumer builds a group consumer with a worker pool of the given width.
// CommitInterval is left at zero so the reader never auto-commits — offsets
// advance only through CommitMessages after a batch is durably saved.
func NewConsumer(brokers []string, topic, groupID string, store audit.AuditStore, workers, batchSize int) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		}),
		store:     store,
		workers:   workers,
		batchSize: batchSize,
	}
}

// Run consumes until ctx is cancelled. On a shutdown signal it stops fetching
// new work but always finishes and commits any batch already in flight (a
// graceful drain), then returns nil. It returns an error if fetching, saving,
// or committing fails.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msgs, err := c.fetchBatch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				slog.Info("consumer stopped fetching, drained")
				return nil
			}
			return err
		}
		if len(msgs) == 0 {
			continue
		}

		drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
		start := time.Now()
		perr := c.processBatch(drainCtx, msgs)
		if perr == nil {
			perr = c.reader.CommitMessages(drainCtx, msgs...)
		}
		cancel()
		if perr != nil {
			return perr
		}

		slog.Info("batch committed",
			"messages", len(msgs),
			"duration_ms", time.Since(start).Milliseconds(),
			"lag", c.reader.Stats().Lag,
		)
	}
}

// fetchBatch blocks for the first message, then greedily tops up the batch with
// any messages already available, up to batchSize or batchFillTimeout.
func (c *Consumer) fetchBatch(ctx context.Context) ([]kafka.Message, error) {
	first, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}
	msgs := make([]kafka.Message, 0, c.batchSize)
	msgs = append(msgs, first)

	for len(msgs) < c.batchSize {
		fillCtx, cancel := context.WithTimeout(ctx, batchFillTimeout)
		m, err := c.reader.FetchMessage(fillCtx)
		cancel()
		if err != nil {
			break // timeout (batch is full enough) or parent cancelled
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// processBatch fans the batch out to the worker pool and blocks until every
// message has been saved. Returns an error if any save failed, so the caller
// can skip the commit and let the batch redeliver.
func (c *Consumer) processBatch(ctx context.Context, msgs []kafka.Message) error {
	jobs := make(chan kafka.Message) // dispatch channel
	var failures atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range jobs {
				c.handleOne(ctx, m, &failures)
			}
		}()
	}

	for _, m := range msgs {
		jobs <- m
	}
	close(jobs)
	wg.Wait()

	if failures.Load() > 0 {
		return errors.New("batch had failed saves, leaving offsets uncommitted")
	}
	return nil
}

// handleOne processes one message with panic recovery. A panic in Save (or
// anywhere below) is caught so it cannot kill the worker goroutine or crash the
// process — the pool survives. A recovered panic is treated like a save failure:
// it increments the failure count so the batch is not committed and the message
// is redelivered.
func (c *Consumer) handleOne(ctx context.Context, m kafka.Message, failures *atomic.Int64) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("worker recovered from panic",
				"partition", m.Partition, "offset", m.Offset, "panic", r)
			failures.Add(1)
		}
	}()
	if err := c.saveMessage(ctx, m); err != nil {
		failures.Add(1)
	}
}

// saveMessage decodes and persists one message. A message that never decodes is
// a poison pill: it is logged and skipped (treated as done) so it does not block
// the batch commit forever. Only a Save failure is reported as an error.
func (c *Consumer) saveMessage(ctx context.Context, m kafka.Message) error {
	var e event.DecisionEvent
	if err := json.Unmarshal(m.Value, &e); err != nil {
		slog.Error("skip malformed message",
			"partition", m.Partition, "offset", m.Offset, "err", err)
		return nil
	}
	if err := c.store.Save(ctx, e); err != nil {
		slog.Error("save failed", "txn_id", e.TxnID, "offset", m.Offset, "err", err)
		return err
	}
	slog.Info("audit persisted",
		"txn_id", e.TxnID, "classification", e.Classification,
		"partition", m.Partition, "offset", m.Offset)
	return nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
