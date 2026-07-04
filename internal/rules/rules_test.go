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
