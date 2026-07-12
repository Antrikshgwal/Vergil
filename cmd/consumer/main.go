package main

import (
	"context"
	"log/slog"
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

func main() {
	setupLogger()

	const (
		kafkaAddr = "localhost:9092"
		topic     = "decisions"
		groupID   = "vergil-audit"
		dsn       = "postgres://vergil:vergil@localhost:5432/vergil"
	)

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

	c := pipeline.NewConsumer([]string{kafkaAddr}, topic, groupID, store)
	defer c.Close()

	slog.Info("consumer starting", "kafka", kafkaAddr, "topic", topic, "group", groupID)
	if err := c.Run(ctx); err != nil {
		slog.Error("consumer stopped with error", "err", err)
		os.Exit(1)
	}
	slog.Info("consumer stopped")
}
