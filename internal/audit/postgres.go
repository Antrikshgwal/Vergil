package audit

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Antrikshgwal/Vergil/internal/event"
)

//go:embed schema.sql
var schemaSQL string

// PostgresStore is a pgxpool-backed AuditStore. The pool is safe for concurrent
// use, so the consumer's worker pool can share a single PostgresStore.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore opens a connection pool for dsn and verifies it with a Ping.
func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

// Migrate applies schema.sql, creating the decisions table if it is absent.
func (s *PostgresStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

func (s *PostgresStore) Save(ctx context.Context, e event.DecisionEvent) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO decisions (txn_id, user_id, classification, score, reasons, decided_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		e.TxnID, e.UserID, e.Classification, e.Score, e.Reasons, e.DecidedAt,
	)
	return err
}

func (s *PostgresStore) Close() {
	s.pool.Close()
}
