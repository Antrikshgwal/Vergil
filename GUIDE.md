# GUIDE — How we build this together

You write the code. I guide. Learning is the whole point, so I will **not** hand you finished files. This document defines how we work and tracks where you are.

Read [PLAN.md](PLAN.md) for the architecture and phase map. This file is the *process*.

---

## The protocol

**1. One task at a time.** Each phase in PLAN.md is broken into numbered tasks below. We do them in order. You never have more than one open task.

**2. For each task I give you three things, never the code:**
- **Spec** — what the task must do, its inputs/outputs, its boundary.
- **Hint** — the concept, the stdlib/lib call to look up, the shape of the approach. Enough to unblock, not enough to copy.
- **Acceptance check** — a concrete command or test that proves it works. You run it.

**3. You write it, then show me.** Paste the code or point me at the file. I review:
- Correctness against the acceptance check.
- Whether you wrote the by-hand parts by hand (no library shortcut on the worker pool, shutdown, offsets, backpressure).
- One or two "senior-eye" notes — naming, error handling, a race, a missed edge.
I do **not** rewrite it for you unless you're truly stuck and ask directly. Even then I give the smallest nudge that unblocks.

**4. You must be able to explain it.** Before a task is marked done, you answer the task's **check question** in your own words. If you can't, it's not learned yet — we stay on it. These questions are your interview rehearsal.

**5. When stuck, escalate in steps:** (a) tell me what you tried and the error, (b) I give a sharper hint, (c) only if still stuck do I show a minimal snippet — and then you extend it, not me.

### When I *will* write code
- Boilerplate with zero learning value you've already done before (a Makefile, a compose file's nth service) — and only if you ask.
- A minimal snippet to unblock after steps (a) and (b) above.
Everything on the "write by hand" list in PLAN.md §3 is yours, always.

### Your side of the deal
- Attempt before asking. A wrong attempt teaches more than a hint.
- Commit at the end of each task (small commits — one concept each).
- Keep the check-question answers somewhere; they become your README and your interview script.

---

## Ground rules for the code

- Go 1.22+. `gofmt` clean, `go vet` clean before you show me.
- Interfaces at every component boundary (see PLAN.md §2.1).
- No framework where stdlib does the job.
- Errors wrapped with context (`fmt.Errorf("...: %w", err)`), never swallowed.
- Every pure function (rules, classify, scorer) gets a table-driven test.

---

## Progress tracker

Legend: `[ ]` todo · `[~]` in progress · `[x]` done (acceptance passed + check question answered)

### Phase 0 — Setup
- [ ] 0.1 Go module init, `.gitignore`, empty package dirs per PLAN.md §2.6
- [ ] 0.2 `deploy/docker-compose.yml`: Redis + Postgres + Redpanda, all healthchecked, `docker compose up` clean
- [ ] 0.3 `internal/config`: env parsing, one `Config` struct, sane defaults

### Phase 1 — Request path, runnable
- [x] 1.1 `TxnRequest`/`DecisionResponse` types + JSON decode/validate (in `cmd/api/main.go` for now)
- [x] 1.2 Method-routed `ServeMux`, `POST /v1/transactions` (walking skeleton, hardcoded ALLOW)
- [ ] 1.3 `FeatureStore` interface + Redis fixed-window velocity (`INCR`+`EXPIRE`)
- [ ] 1.4 `rules`: 2 rules + scorer + `classify(score)` → ALLOW/REVIEW/BLOCK
- [ ] 1.5 `decision.Decide` wires feature→score→classify; `curl` returns a real decision

### Phase 2 — Real scoring, tested
- [ ] 2.1 Sliding-window velocity via sorted set (`ZADD`/`ZREMRANGEBYSCORE`/`ZCARD`), atomic + `EXPIRE`
- [ ] 2.2 Fixed-window amount-sum (`INCRBYFLOAT`+`EXPIRE`); note the trade-off in comments
- [ ] 2.3 `Rule` interface + several concrete rules + composable scorer
- [ ] 2.4 Table-driven tests for every rule + classify

### Phase 3 — Async pipeline
- [ ] 3.1 `DecisionEvent` + `EventPublisher` interface + kafka-go async writer
- [ ] 3.2 Publish from `decision.Decide` without blocking request path
- [ ] 3.3 Postgres `schema.sql` + `pgxpool` setup + `AuditStore` interface
- [ ] 3.4 Consumer with `FetchMessage`/`CommitMessages` (manual offsets)
- [ ] 3.5 Bounded worker pool by hand: dispatch channel + N workers + `WaitGroup`
- [ ] 3.6 Commit offset AFTER Postgres write; verify audit rows land

### Phase 4 — Hardening
- [ ] 4.1 Graceful shutdown, correct order (PLAN.md §4 Phase 4), drains in-flight
- [ ] 4.2 Backpressure observable: slow Postgres → channel fills → lag grows
- [ ] 4.3 Idempotent upsert `ON CONFLICT DO NOTHING`; prove reprocess is safe
- [ ] 4.4 Per-worker panic recovery; pool survives, message redelivered

### Phase 5 — Prove + document
- [ ] 5.1 Load test decision latency (vegeta/k6), record p50/p95/p99
- [ ] 5.2 Load test pipeline throughput + lag under burst
- [ ] 5.3 pprof under load; tune one hot path; before/after
- [ ] 5.4 Tune workers / buffer / MaxConns; know why the numbers are the numbers
- [ ] 5.5 `/metrics` Prometheus: latency histogram, class counter, lag, pool utilization
- [ ] 5.6 README with all five trade-offs + your numbers

---

## Check questions (answer before marking the phase done)

- **P1:** Why is the decision synchronous and Kafka after it? What breaks if you invert that?
- **P2:** Sliding vs fixed window — why is count exact but sum hard? What did you trade and why?
- **P3:** Why commit the Kafka offset *after* the Postgres write, not before? What does that guarantee, and what does it cost?
- **P4:** Walk the shutdown order. Why that order? Where would a dropped message come from if you got it wrong?
- **P5:** Your p99 decision latency is X. Why that number? What's the hot path, and what did tuning change?
