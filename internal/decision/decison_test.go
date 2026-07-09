package decision

import (
	"context"
	"testing"

	"github.com/Antrikshgwal/Vergil/internal/rules"
)

type FakeStore struct {
	n int
}

func (fs *FakeStore) Velocity(ctx context.Context, userID, txnID string) (int, error) {
	return fs.n, nil
}

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
			svc := NewService(fakeStore, ruleset)

			got, err := svc.Decide(context.Background(), Transaction{
				TxnID: "t1", UserID: "u1", Amount: tt.amount, Currency: "USD",
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Classification != tt.wantClass {
				t.Errorf("Decide() class = %q, want %q", got.Classification, tt.wantClass)
			}
		})
	}
}
