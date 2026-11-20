# Decisions Log

Minor, reversible implementation decisions taken during development that complement `docs/architecture.md` and feature plans.

| Date | Feature / Task | Decision | Rationale |
|---|---|---|---|
| 2026-11-20 | 006-T002 / 001 | GSI `gsi_slot_shard` uses `ProjectionType=INCLUDE` with `NonKeyAttributes=["provider", "enabled", "created_at"]`. | Matches DynamoDB API semantics for `KEYS_ONLY + provider, enabled` (001/plan.md) and also supports reconciler filtering by `created_at` (005/plan.md) without extra `GetItem` calls. |
| 2026-11-20 | 006-T001 | `docker-compose.yml` starts with `localstack` and `init`; `mockapi`, `dispatcher`, `worker`, `seed` will be attached in M1/M2. | Agreed with user; keeps `make up` immediately verifiable in M0 without dummy binaries. |
