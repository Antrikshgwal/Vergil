package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Antrikshgwal/Vergil/internal/audit"
	"github.com/Antrikshgwal/Vergil/internal/pipeline"
)

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

// getenv returns the env var or a fallback when it is empty.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// databaseDSN builds the Postgres DSN from environment variables. Host, port,
// user, and database name have local-dev defaults, but the password is required
// and has no default — no credential is baked into the binary. Returns an error
// if PGPASSWORD is unset so the process fails fast with a clear message.
func databaseDSN() (string, error) {
	pass := os.Getenv("PGPASSWORD")
	if pass == "" {
		return "", fmt.Errorf("PGPASSWORD is not set")
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		getenv("PGUSER", "vergil"),
		url.QueryEscape(pass),
		getenv("PGHOST", "localhost"),
		getenv("PGPORT", "5432"),
		getenv("PGDATABASE", "vergil"),
	), nil
}

func main() {
	setupLogger()

	const (
		kafkaAddr = "localhost:9092"
		topic     = "decisions"
		groupID   = "vergil-audit"
		workers   = 4
		batchSize = 100
	)

	dsn, err := databaseDSN()
	if err != nil {
		slog.Error("database config invalid", "err", err)
		os.Exit(1)
	}

	// Cancel the run loop on Ctrl+C / SIGTERM. Full graceful drain is 4.1.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := audit.NewPostgresStore(ctx, dsn)
	if err != nil {
		slog.Error("connect postgres failed", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		slog.Error("migrate failed", "err", err)
		os.Exit(1)
	}

	c := pipeline.NewConsumer([]string{kafkaAddr}, topic, groupID, store, workers, batchSize)
	defer c.Close()

	slog.Info("consumer starting",
		"kafka", kafkaAddr, "topic", topic, "group", groupID, "workers", workers, "batch_size", batchSize)
	if err := c.Run(ctx); err != nil {
		slog.Error("consumer stopped with error", "err", err)
		os.Exit(1)
	}
	slog.Info("consumer stopped")
}
