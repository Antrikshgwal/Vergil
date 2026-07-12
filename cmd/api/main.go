package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/Antrikshgwal/Vergil/internal/decision"
	"github.com/Antrikshgwal/Vergil/internal/event"
	"github.com/Antrikshgwal/Vergil/internal/feature"
	"github.com/Antrikshgwal/Vergil/internal/rules"
)

type transactionRequest struct {
	TxnID    string  `json:"txn_id"`
	UserID   string  `json:"user_id"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

type classificationType string

const (
	ALLOW  classificationType = "ALLOW"
	REVIEW classificationType = "REVIEW"
	BLOCK  classificationType = "BLOCK"
)

type transactionResponse struct {
	TxnID          string             `json:"txn_id"`
	Classification classificationType `json:"classification"`
	Score          float64            `json:"score"`
}

type api struct {
	svc *decision.Service
}

func (a *api) handleTransaction(w http.ResponseWriter, r *http.Request) {
	var req transactionRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.TxnID == "" || req.UserID == "" || req.Amount <= 0 || req.Currency == "" {
		http.Error(w, "Missing or invalid fields in request", http.StatusBadRequest)
		return
	}

	d, err := a.svc.Decide(r.Context(), decision.Transaction{
		TxnID:    req.TxnID,
		UserID:   req.UserID,
		Amount:   req.Amount,
		Currency: req.Currency,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(transactionResponse{
		TxnID:          d.TxnID,
		Classification: classificationType(d.Classification),
		Score:          d.Score,
	})
}

func main() {
	a := &api{}
	mux := http.NewServeMux()
	fs := feature.NewRedisStore("localhost:6379", 60*time.Second)
	ruleset := []rules.Rule{
		rules.HighVelocityRule{Threshold: 5, Point: 0.5},
		rules.HighAmountRule{Threshold: 1000, Point: 0.5},
		rules.HighAmountSumRule{Threshold: 5000, Point: 0.5},
		rules.UnusualCurrencyRule{
			Allowed: map[string]bool{"USD": true, "EUR": true, "GBP": true},
			Point:   0.3,
		},
	}
	pub := event.NewKafkaPublisher([]string{"localhost:9092"}, "decisions")
	svc := decision.NewService(fs, ruleset, pub)
	a = &api{svc: svc}
	mux.HandleFunc("POST /v1/transactions", a.handleTransaction)
	log.Fatal(http.ListenAndServe(":8080", mux))
}
