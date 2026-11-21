# Decisions Log

Minor, reversible implementation decisions taken during development that complement `docs/architecture.md` and feature plans.

| Date | Feature / Task | Decision | Rationale |
|---|---|---|---|
| 2026-11-20 | 006-T002 / 001 | GSI `gsi_slot_shard` uses `ProjectionType=INCLUDE` with `NonKeyAttributes=["provider", "enabled", "created_at"]`. | Matches DynamoDB API semantics for `KEYS_ONLY + provider, enabled` (001/plan.md) and also supports reconciler filtering by `created_at` (005/plan.md) without extra `GetItem` calls. |
| 2026-11-20 | 006-T001 | `docker-compose.yml` starts with `localstack` and `init`; `mockapi`, `dispatcher`, `worker`, `seed` will be attached in M1/M2. | Agreed with user; keeps `make up` immediately verifiable in M0 without dummy binaries. |
| 2026-11-20 | 001-T006 | `Request.Provider` disallows `#`; `Request` implements `slog.LogValuer` and `fmt.Stringer` omitting `Body` content. | Prevents GSI sort-key (`provider#id`) delimiter ambiguity and enforces the PII no-raw-body logging invariant by construction. |
| 2026-11-20 | 001-T015 | Unslotted requests omit `slot_shard` (sparse GSI); `ListBySlotShard` encodes `LastEvaluatedKey` as URL-safe base64 JSON and skips empty filtered pages. | Keeps unslotted rows out of `gsi_slot_shard` until `SetSlot`, prevents empty-page cursor leaks under `FilterExpression`, and preserves the exact `ListBySlotShard(..., page string)` port contract. |
| 2026-11-21 | 006-T003 / 002-T004 | `Clock` (real + `CLOCK_SPEEDUP` and future `FakeClock`) lives in `internal/platform/clock` rather than `internal/adapters/clock`. | Aligns with `002/tasks.md` T004 and treats wall-clock time as a transversal platform primitive (like `internal/platform/telemetry`) with zero external I/O dependencies. |
