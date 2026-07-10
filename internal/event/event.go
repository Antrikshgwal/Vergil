package event

import (
	"context"
	"time"
)

// DecisionEvent is the audit record emitted for every scored transaction. It is
// published asynchronously off the request path and later persisted by the
// pipeline consumer.
type DecisionEvent struct {
	TxnID          string    `json:"txn_id"`
	UserID         string    `json:"user_id"`
	Classification string    `json:"classification"`
	Score          float64   `json:"score"`
	Reasons        []string  `json:"reasons"`
	DecidedAt      time.Time `json:"decided_at"`
}

// Publisher emits DecisionEvents into the async pipeline. Kept an interface so
// the decision service can be wired against an in-memory fake in tests.
type Publisher interface {
	Publish(ctx context.Context, e DecisionEvent) error
	Close() error
}
