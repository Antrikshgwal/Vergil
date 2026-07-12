package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"

	"github.com/Antrikshgwal/Vergil/internal/event"
)

// fakeStore is a concurrency-safe AuditStore that records saved events and can
// be told to fail on a specific txn id.
type fakeStore struct {
	mu     sync.Mutex
	saved  []event.DecisionEvent
	failOn string
}

func (f *fakeStore) Save(ctx context.Context, e event.DecisionEvent) error {
	if e.TxnID == f.failOn {
		return errors.New("boom")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved = append(f.saved, e)
	return nil
}

func (f *fakeStore) Close() {}

func (f *fakeStore) savedIDs() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make(map[string]bool, len(f.saved))
	for _, e := range f.saved {
		ids[e.TxnID] = true
	}
	return ids
}

func msg(t *testing.T, txnID string) kafka.Message {
	t.Helper()
	b, err := json.Marshal(event.DecisionEvent{TxnID: txnID, Classification: "ALLOW"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafka.Message{Value: b}
}

func newTestConsumer(store *fakeStore) *Consumer {
	// The reader is unused by processBatch, so leave it nil.
	return &Consumer{store: store, workers: 4, batchSize: 100}
}

func TestProcessBatchAllSaved(t *testing.T) {
	store := &fakeStore{}
	c := newTestConsumer(store)

	msgs := make([]kafka.Message, 0, 50)
	for i := 0; i < 50; i++ {
		msgs = append(msgs, msg(t, string(rune('a'+i%26))+string(rune('0'+i/26))))
	}

	if err := c.processBatch(context.Background(), msgs); err != nil {
		t.Fatalf("processBatch: unexpected error %v", err)
	}
	if got := len(store.saved); got != len(msgs) {
		t.Errorf("saved %d events, want %d", got, len(msgs))
	}
}

func TestProcessBatchSaveFailureBlocksCommit(t *testing.T) {
	store := &fakeStore{failOn: "bad"}
	c := newTestConsumer(store)

	msgs := []kafka.Message{msg(t, "ok1"), msg(t, "bad"), msg(t, "ok2")}

	err := c.processBatch(context.Background(), msgs)
	if err == nil {
		t.Fatal("expected error when a save fails, got nil")
	}
	// The good messages are still saved; the caller skips the commit so the
	// whole batch redelivers.
	ids := store.savedIDs()
	if !ids["ok1"] || !ids["ok2"] {
		t.Errorf("expected ok1 and ok2 saved, got %v", ids)
	}
	if ids["bad"] {
		t.Error("failed message must not be recorded as saved")
	}
}

func TestProcessBatchSkipsPoison(t *testing.T) {
	store := &fakeStore{}
	c := newTestConsumer(store)

	msgs := []kafka.Message{
		msg(t, "ok1"),
		{Value: []byte("{not valid json")}, // poison: never decodes
		msg(t, "ok2"),
	}

	if err := c.processBatch(context.Background(), msgs); err != nil {
		t.Fatalf("poison message must not fail the batch, got %v", err)
	}
	if got := len(store.saved); got != 2 {
		t.Errorf("saved %d events, want 2 (poison skipped)", got)
	}
}
