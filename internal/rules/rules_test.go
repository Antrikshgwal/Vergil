package rules_test

import (
	"reflect"
	"testing"

	"github.com/Antrikshgwal/Vergil/internal/rules"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name  string
		score float64
		want  string
	}{
		{"low is allow", 0.1, "ALLOW"},
		{"mid is review", 0.5, "REVIEW"},
		{"high is block", 0.9, "BLOCK"},
		{"exactly block threshold", 0.7, "BLOCK"},
		{"just below block", 0.699, "REVIEW"},
		{"exactly review threshold", 0.4, "REVIEW"},
		{"just below review", 0.399, "ALLOW"},
		{"zero", 0.0, "ALLOW"},
		
		// add boundary cases: exactly at a threshold
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rules.Classify(tt.score); got != tt.want {
				t.Errorf("rules.Classify(%v) = %q, want %q", tt.score, got, tt.want)
			}
		})
	}
}

func TestHighVelocityRule(t *testing.T) {
	rule := rules.HighVelocityRule{Threshold: 5, Point: 0.5}
	tests := []struct {
		name     string
		velocity int
		want     float64
	}{
		{"below threshold", 3, 0},
		{"at threshold is not high", 5, 0},
		{"above threshold fires", 6, 0.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rule.Evaluate(rules.Features{Velocity: tt.velocity}); got != tt.want {
				t.Errorf("HighVelocityRule.Evaluate(velocity=%d) = %v, want %v", tt.velocity, got, tt.want)
			}
		})
	}
}

func TestHighAmountRule(t *testing.T) {
	rule := rules.HighAmountRule{Threshold: 1000, Point: 0.5}
	tests := []struct {
		name   string
		amount float64
		want   float64
	}{
		{"below threshold", 999, 0},
		{"at threshold is not high", 1000, 0},
		{"above threshold fires", 1000.01, 0.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rule.Evaluate(rules.Features{Amount: tt.amount}); got != tt.want {
				t.Errorf("HighAmountRule.Evaluate(amount=%v) = %v, want %v", tt.amount, got, tt.want)
			}
		})
	}
}

func TestHighAmountSumRule(t *testing.T) {
	rule := rules.HighAmountSumRule{Threshold: 5000, Point: 0.5}
	tests := []struct {
		name      string
		amountSum float64
		want      float64
	}{
		{"below threshold", 4999, 0},
		{"at threshold is not high", 5000, 0},
		{"above threshold fires", 5000.01, 0.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rule.Evaluate(rules.Features{AmountSum: tt.amountSum}); got != tt.want {
				t.Errorf("HighAmountSumRule.Evaluate(amountSum=%v) = %v, want %v", tt.amountSum, got, tt.want)
			}
		})
	}
}

func TestUnusualCurrencyRule(t *testing.T) {
	rule := rules.UnusualCurrencyRule{
		Allowed: map[string]bool{"USD": true, "EUR": true},
		Point:   0.3,
	}
	tests := []struct {
		name     string
		currency string
		want     float64
	}{
		{"allowed currency", "USD", 0},
		{"another allowed currency", "EUR", 0},
		{"disallowed currency fires", "XRP", 0.3},
		{"empty currency fires", "", 0.3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rule.Evaluate(rules.Features{Currency: tt.currency}); got != tt.want {
				t.Errorf("UnusualCurrencyRule.Evaluate(currency=%q) = %v, want %v", tt.currency, got, tt.want)
			}
		})
	}
}

func TestScoreTransaction(t *testing.T) {
	tests := []struct {
		name      string
		features  rules.Features
		rules     []rules.Rule
		wantScore float64
		wantRules []string
	}{
		{
			name:      "no rules triggered",
			features:  rules.Features{Velocity: 5, Amount: 100, Currency: "USD"},
			rules:     []rules.Rule{},
			wantScore: 0,
			wantRules: []string{},
		},
		{
			name:      "one rule triggered",
			features:  rules.Features{Velocity: 10, Amount: 100, Currency: "USD"},
			rules:     []rules.Rule{rules.HighVelocityRule{Threshold: 5, Point: 0.5}},
			wantScore: 0.5,
			wantRules: []string{"HighVelocityRule"},
		},
		{
			name:     "multiple rules triggered sum and compose",
			features: rules.Features{Velocity: 10, Amount: 2000, AmountSum: 6000, Currency: "XRP"},
			rules: []rules.Rule{
				rules.HighVelocityRule{Threshold: 5, Point: 0.5},
				rules.HighAmountRule{Threshold: 1000, Point: 0.5},
				rules.HighAmountSumRule{Threshold: 5000, Point: 0.5},
				rules.UnusualCurrencyRule{Allowed: map[string]bool{"USD": true}, Point: 0.3},
			},
			wantScore: 1.8,
			wantRules: []string{"HighVelocityRule", "HighAmountRule", "HighAmountSumRule", "UnusualCurrencyRule"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotScore, gotRules := rules.ScoreTransaction(tt.features, tt.rules)
			if gotScore != tt.wantScore {
				t.Errorf("rules.ScoreTransaction() gotScore = %v, want %v", gotScore, tt.wantScore)
			}
			if !reflect.DeepEqual(gotRules, tt.wantRules) {
				t.Errorf("rules.ScoreTransaction() gotRules = %v, want %v", gotRules, tt.wantRules)
			}
		})
	}
}
