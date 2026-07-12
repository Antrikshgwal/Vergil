package decision

import (
	"context"
	"testing"

	"github.com/Antrikshgwal/Vergil/internal/event"
	"github.com/Antrikshgwal/Vergil/internal/rules"
)

type FakeStore struct {
	n         int
	amountSum float64
}

func (fs *FakeStore) Velocity(ctx context.Context, userID, txnID string) (int, error) {
	return fs.n, nil
}

func (fs *FakeStore) AmountSum(ctx context.Context, userID string, amount float64) (float64, error) {
	return fs.amountSum, nil
}

// recordingPublisher captures published events so tests can assert Decide emits
// an audit event without needing a real broker.
type recordingPublisher struct {
	events []event.DecisionEvent
}

func (p *recordingPublisher) Publish(ctx context.Context, e event.DecisionEvent) error {
	p.events = append(p.events, e)
	return nil
}

func (p *recordingPublisher) Close() error { return nil }

func TestDecide(t *testing.T) {
	// Create a fake store with a predefined velocity
	fakeStore := &FakeStore{n: 5}
	// rules
	ruleset := []rules.Rule{
		rules.HighVelocityRule{Threshold: 5, Point: 0.5},
		rules.HighAmountRule{Threshold: 1000, Point: 0.5},
	}
	// test struct
	tests := []struct {
		name         string
		fakeVelocity int
		amount       float64
		wantClass    string
	}{
		{"Low velocity and low amount", 1, 50.0, "ALLOW"},
		{"High velocity and low amount", 10, 50.0, "REVIEW"},
		{"Low velocity and high amount", 1, 50000.0, "REVIEW"},
		{"High velocity and high amount", 10, 50000.0, "BLOCK"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeStore.n = tt.fakeVelocity
			pub := &recordingPublisher{}
			svc := NewService(fakeStore, ruleset, pub)

			got, err := svc.Decide(context.Background(), Transaction{
				TxnID: "t1", UserID: "u1", Amount: tt.amount, Currency: "USD",
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Classification != tt.wantClass {
				t.Errorf("Decide() class = %q, want %q", got.Classification, tt.wantClass)
			}
			if len(pub.events) != 1 {
				t.Fatalf("expected 1 published event, got %d", len(pub.events))
			}
			if pub.events[0].TxnID != got.TxnID || pub.events[0].Classification != got.Classification {
				t.Errorf("published event = {%q, %q}, want {%q, %q}",
					pub.events[0].TxnID, pub.events[0].Classification, got.TxnID, got.Classification)
			}
		})
	}
}
