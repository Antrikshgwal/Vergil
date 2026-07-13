package decision

import (
	"context"
	"log/slog"
	"time"

	"github.com/Antrikshgwal/Vergil/internal/event"
	"github.com/Antrikshgwal/Vergil/internal/feature"
	"github.com/Antrikshgwal/Vergil/internal/rules"
)

// Service struct for holding dependencies
type Service struct {
	store feature.Store
	rules []rules.Rule
	pub   event.Publisher
}

// Transaction struct holding the transaction details
type Transaction struct {
	TxnID    string
	UserID   string
	Amount   float64
	Currency string
}

// struct holding the decision
type Decision struct {
	TxnID          string
	Classification string
	Score          float64
	Reason         []string
}

// NewService creates a new instance of the Service struct with the provided
// store, rules, and event publisher. pub may be nil to disable publishing.
func NewService(store feature.Store, rules []rules.Rule, pub event.Publisher) *Service {
	return &Service{
		store: store,
		rules: rules,
		pub:   pub,
	}
}

func (s *Service) Decide(ctx context.Context, txn Transaction) (Decision, error) {
	// Orchestrate the decision-making process
	snap, err := s.store.Snapshot(ctx, txn.UserID, txn.TxnID, txn.Amount)
	if err != nil {
		return Decision{}, err
	}
	feats := rules.Features{
		Velocity:  snap.Velocity,
		Amount:    txn.Amount,
		AmountSum: snap.AmountSum,
		Currency:  txn.Currency,
	}
	score, triggeredRules := rules.ScoreTransaction(feats, s.rules)
	classification := rules.Classify(score)

	d := Decision{
		TxnID:          txn.TxnID,
		Classification: classification,
		Score:          score,
		Reason:         triggeredRules,
	}

	slog.Debug("decision made",
		"txn_id", d.TxnID,
		"user_id", txn.UserID,
		"classification", d.Classification,
		"score", d.Score,
		"reasons", d.Reason,
		"velocity", snap.Velocity,
		"amount", txn.Amount,
		"amount_sum", snap.AmountSum,
	)

	// Emit the audit event off the request path. KafkaPublisher enqueues on an
	// async writer, so Publish does not block on the broker. A publish failure
	// must not fail an already-made decision, so it is logged, not returned.
	if s.pub != nil {
		evt := event.DecisionEvent{
			TxnID:          d.TxnID,
			UserID:         txn.UserID,
			Classification: d.Classification,
			Score:          d.Score,
			Reasons:        d.Reason,
			DecidedAt:      time.Now().UTC(),
		}
		if err := s.pub.Publish(ctx, evt); err != nil {
			slog.Error("decision publish failed", "txn_id", d.TxnID, "err", err)
		}
	}

	return d, nil
}
