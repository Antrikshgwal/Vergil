package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof on the default mux for the debug server
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Antrikshgwal/Vergil/internal/decision"
	"github.com/Antrikshgwal/Vergil/internal/event"
	"github.com/Antrikshgwal/Vergil/internal/feature"
	"github.com/Antrikshgwal/Vergil/internal/metrics"
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

// statusRecorder captures the response status so the latency histogram can be
// labelled by it. Defaults to 200 when the handler never calls WriteHeader.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// withMetrics times the request and records it under the given route label.
func withMetrics(route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next(rec, r)
		metrics.RequestDuration.WithLabelValues(route, strconv.Itoa(rec.status)).
			Observe(time.Since(start).Seconds())
	}
}

func (a *api) handleTransaction(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req transactionRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		slog.Warn("reject transaction: bad request body", "remote", r.RemoteAddr, "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.TxnID == "" || req.UserID == "" || req.Amount <= 0 || req.Currency == "" {
		slog.Warn("reject transaction: missing or invalid fields",
			"txn_id", req.TxnID, "user_id", req.UserID, "amount", req.Amount, "currency", req.Currency)
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
		slog.Error("decide failed", "txn_id", req.TxnID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	metrics.DecisionsTotal.WithLabelValues(d.Classification).Inc()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(transactionResponse{
		TxnID:          d.TxnID,
		Classification: classificationType(d.Classification),
		Score:          d.Score,
	})

	slog.Debug("transaction handled",
		"txn_id", d.TxnID,
		"classification", d.Classification,
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

// setupLogger builds the process-wide structured logger. Output is JSON to
// stdout; the level is read from LOG_LEVEL (DEBUG/INFO/WARN/ERROR), default INFO.
func setupLogger() {
	level := slog.LevelInfo
	if lv := os.Getenv("LOG_LEVEL"); lv != "" {
		_ = level.UnmarshalText([]byte(strings.ToUpper(lv)))
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
}

func main() {
	setupLogger()

	const (
		addr      = ":8080"
		debugAddr = "localhost:6060"
		redisAddr = "localhost:6379"
		kafkaAddr = "localhost:9092"
		topic     = "decisions"
	)

	mux := http.NewServeMux()
	fs := feature.NewRedisStore(redisAddr, 60*time.Second)
	ruleset := []rules.Rule{
		rules.HighVelocityRule{Threshold: 5, Point: 0.5},
		rules.HighAmountRule{Threshold: 1000, Point: 0.5},
		rules.HighAmountSumRule{Threshold: 5000, Point: 0.5},
		rules.UnusualCurrencyRule{
			Allowed: map[string]bool{"USD": true, "EUR": true, "GBP": true},
			Point:   0.3,
		},
	}
	pub := event.NewKafkaPublisher([]string{kafkaAddr}, topic)
	svc := decision.NewService(fs, ruleset, pub)
	a := &api{svc: svc}
	mux.HandleFunc("POST /v1/transactions", withMetrics("/v1/transactions", a.handleTransaction))
	mux.Handle("GET /metrics", metrics.Handler())

	srv := &http.Server{Addr: addr, Handler: mux}

	// pprof + the default mux on a separate debug port, kept off the public api.
	go func() {
		if err := http.ListenAndServe(debugAddr, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("debug server exited", "err", err)
		}
	}()

	// Cancel ctx on SIGINT/SIGTERM to begin graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("starting api server",
			"addr", addr, "redis", redisAddr, "kafka", kafkaAddr, "topic", topic, "rules", len(ruleset))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server exited", "err", err)
			stop() // unblock main so the process exits
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown signal received, draining")

	// Correct order: drain in-flight HTTP requests first (handlers enqueue into
	// the publisher), then flush and close the async publisher so buffered
	// events are not lost.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown error", "err", err)
	}
	if err := pub.Close(); err != nil {
		slog.Error("publisher close error", "err", err)
	}
	slog.Info("api stopped")
}
