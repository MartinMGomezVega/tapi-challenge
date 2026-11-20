# Tasks: Observabilidad y reconciliación

**Input**: [spec.md](./spec.md), [plan.md](./plan.md) · TDD obligatorio.

## Fase 1: Telemetría transversal (US2, P1)
- [ ] T001 [US2] Test (golden): el logger JSON incluye `request_id`, `provider`, `run_date`, `trace_id`
- [ ] T002 [US2] Implementar `platform/telemetry/log.go`
- [ ] T003 [US2] Test (golden): formato EMF de las métricas
- [ ] T004 [US2] Implementar `platform/telemetry/emf.go`
- [ ] T005 [US2] Test: `traceparent` viaja en `MessageAttributes` (dispatcher → worker)
- [ ] T006 [US2] Implementar el tracer OTel + inyección y extracción en los adaptadores SQS
- [ ] T007 [P] [US2] Instrumentar dispatcher y worker con las métricas del spec

## Fase 2: US1 — Reconciliación (P1) 🎯
- [ ] T008 [US1] Test: `Coverage` puro (terminales / esperados; `SKIPPED` cuenta como terminal)
- [ ] T009 [US1] Test: clasificación `missing` / `stuck(>2h)` / `terminal`; exclusión por `created_at`
- [ ] T010 [US1] Test: `Reconcile` re-despacha faltantes y atascados por el puerto `JobQueue` (fake)
- [ ] T011 [US1] Test: dos corridas seguidas → el segundo re-despacho es 0 (idempotencia)
- [ ] T012 [US1] Implementar `app/reconcile`
- [ ] T013 [US1] Implementar `executions.BatchGet` y `ResetStale` (condicional)
- [ ] T014 [US1] Reporte JSON/CSV en S3 + tabla Glue/Athena
- [ ] T015 [US1] `cmd/reconciler` (flags `--date`, `--redispatch`, `--parallelism`)
- [ ] T016 [US1] Test de integración: 10k registros con 50 faltantes y 20 atascados → cobertura 100 % tras el re-despacho (SC-001)
- [ ] T017 [P] [US1] Benchmark con 1M en LocalStack → `docs/evidence/reconciler-bench.md` (SC-002)

## Fase 3: US3 — Alarmas y runbooks (P1)
- [ ] T018 [US3] Terraform: alarmas de la tabla del spec + SNS
- [ ] T019 [P] [US3] Runbooks: `dispatch-lag.md`, `queue-age.md`, `dlq.md`, `coverage-low.md`, `auth-errors.md`, `lambda-quota.md`, `provider-capacity.md`

## Fase 4: US4 — Dashboard (P2)
- [ ] T020 [US4] Terraform: dashboard de CloudWatch
- [ ] T021 [P] Captura del dashboard durante el test de carga → `docs/evidence/`

## Dependencias
Fase 1 puede empezar en paralelo con la 003. La Fase 2 requiere 001–004.
