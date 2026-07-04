package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type transactionRequest struct {
	TxnID    string  `json:"txn_id"`
	UserID   string  `json:"user_id"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

type classificationType string

const (
	ALLOW classificationType = "ALLOW"
	REVIEW classificationType = "REVIEW"
	BLOCK  classificationType = "BLOCK"
)

type transactionResponse struct {
	TxnID          string             `json:"txn_id"`
	Classification classificationType `json:"classification"`
	Score          float64            `json:"score"`
}

func processTransaction(w http.ResponseWriter, r *http.Request){
	var Request transactionRequest
	err := json.NewDecoder(r.Body).Decode(&Request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if Request.TxnID == "" || Request.UserID == "" || Request.Amount <= 0 || Request.Currency == "" {
		http.Error(w, "Missing or invalid fields in request", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(transactionResponse{
		TxnID:          Request.TxnID,
		Classification: ALLOW,
		Score:          0.8,
	})

}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/transactions", processTransaction)
	log.Fatal(http.ListenAndServe(":8080", mux))
}