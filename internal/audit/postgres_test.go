package audit

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Antrikshgwal/Vergil/internal/event"
)

// TestSaveIdempotent proves that reprocessing the same message (as happens
// after an at-least-once redelivery) does not create a duplicate row. It is an
// integration test: set VERGIL_TEST_DSN to a reachable Postgres to run it,
// otherwise it is skipped.
//
//	VERGIL_TEST_DSN=postgres://vergil:vergil@localhost:5432/vergil go test ./internal/audit/
func TestSaveIdempotent(t *testing.T) {
	dsn := os.Getenv("VERGIL_TEST_DSN")
	if dsn == "" {
		t.Skip("VERGIL_TEST_DSN not set; skipping postgres integration test")
	}

	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	e := event.DecisionEvent{
		TxnID:          "idem-test-1",
		UserID:         "u1",
		Classification: "ALLOW",
		Score:          0.1,
		Reasons:        []string{},
		DecidedAt:      time.Now().UTC(),
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), "DELETE FROM decisions WHERE txn_id = $1", e.TxnID)
	})

	if err := store.Save(ctx, e); err != nil {
		t.Fatalf("first save: %v", err)
	}
	// Reprocess the same event — must be a no-op, not a duplicate-key error.
	if err := store.Save(ctx, e); err != nil {
		t.Fatalf("second save (reprocess): %v", err)
	}

	var count int
	if err := store.pool.QueryRow(ctx,
		"SELECT count(*) FROM decisions WHERE txn_id = $1", e.TxnID).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected exactly 1 row after reprocess, got %d", count)
	}
}
