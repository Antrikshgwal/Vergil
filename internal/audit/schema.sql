-- Audit trail of every scored transaction. txn_id is the primary key so the
-- pipeline consumer can later upsert idempotently (ON CONFLICT) when a Kafka
-- message is redelivered.
CREATE TABLE IF NOT EXISTS decisions (
    txn_id         TEXT             PRIMARY KEY,
    user_id        TEXT             NOT NULL,
    classification TEXT             NOT NULL,
    score          DOUBLE PRECISION NOT NULL,
    reasons        TEXT[]           NOT NULL DEFAULT '{}',
    decided_at     TIMESTAMPTZ      NOT NULL,
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS decisions_user_id_idx ON decisions (user_id);
CREATE INDEX IF NOT EXISTS decisions_classification_idx ON decisions (classification);
