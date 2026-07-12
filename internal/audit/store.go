package audit

import (
	"context"

	"github.com/Antrikshgwal/Vergil/internal/event"
)

// AuditStore persists DecisionEvents to durable storage. Kept an interface so
// the pipeline consumer can be tested against an in-memory fake without a real
// database.
type AuditStore interface {
	Save(ctx context.Context, e event.DecisionEvent) error
	Close()
}
