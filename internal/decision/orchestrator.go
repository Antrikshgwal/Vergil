package decision

import (
	"context"

	"github.com/Antrikshgwal/Vergil/internal/feature"
	"github.com/Antrikshgwal/Vergil/internal/rules"
)

// Service struct for holding dependencies
type Service struct {
	Store feature.Store
	Rules []rules.Rule
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

// NewService creates a new instance of the Service struct with the provided store and rules.
func NewService(store feature.Store, rules []rules.Rule) *Service {
	return &Service{
		Store: store,
		Rules: rules,
	}
}

func (s *Service) Decide(ctx context.Context, txn Transaction) (Decision, error) {
	// Orchestrate the decision-making process
	velocity, err := s.Store.Velocity(ctx, txn.UserID)
	if err != nil {
		return Decision{}, err
	}
	feats := rules.Features{
		Velocity: velocity,
		Amount:   txn.Amount,
		Currency: txn.Currency,
	}
	score, triggeredRules := rules.ScoreTransaction(feats, s.Rules)
	classification := rules.Classify(score)

	return Decision{
		TxnID:          txn.TxnID,
		Classification: classification,
		Score:          score,
		Reason:         triggeredRules,
	}, nil
}
