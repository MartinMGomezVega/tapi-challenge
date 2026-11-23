# Checkpoint 005 — Observabilidad y reconciliación diaria (+ `001-T025`)

**Branch**: `005-observabilidad-y-reconciliacion` (mergeada a `main` con `--no-ff`)
**Estado**: Completada (`005-T001`–`T021` + `001-T025`).

## 1. Tareas y commits relevantes
- **Fase 1 — Telemetría transversal (`005-T001`..`T007`)**:
  - `fd508bf` + GREEN commit: `internal/platform/telemetry/{log,emf,trace}.go` con golden tests (`testdata/log.golden.json`, `testdata/emf.golden.json`), propagación W3C `traceparent` sobre OpenTelemetry (`otel/propagation.TraceContext`), y métricas EMF `WorkerMetrics` (`ExecutionsByStatus`, `ProviderLatency`, `ExecutionFailed`, `BreakerOpen`) y `ReconcilerMetrics` (`DailyCoverage`, `MissingExecutions`, `ProviderOverCapacity`).
  - Instrumentación de `cmd/dispatcher` y `cmd/worker` con `telemetry.NewLogger` y métricas EMF.
- **Fase 2 — Reconciliación diaria + `001-T025` (`005-T008`..`T017`)**:
  - `dc1e562` + GREEN commit: `internal/domain/coverage.go` (`ComputeCoverage`, `SlotStartTime`, `ShouldExpectExecution` excluyendo altas posteriores al slot, `ClassifyExecution` `missing`/`stuck(>2h)`/`terminal`/`in_flight` con `SKIPPED` como terminal).
  - `d40b213` + GREEN commit: `internal/app/reconcile` (recorrido paralelo acotado por `1440 × S` sin `Scan`, `BatchGet` de a 100, re-despacho idempotente, 2.ª corrida con 0 re-despachos, reportes JSON + CSV en S3).
  - `dynamo.ExecutionsRepo.BatchGet` (chunks de 100 con reintento de `UnprocessedKeys`) y `ResetStale` condicional (`NOT terminal AND updated_at <= cutoff`).
  - `cmd/reconciler` (`--date`, `--redispatch`, `--parallelism`, `--check-capacity` ejecutando `app/capacity.Check` para `001-T025`) + `build/reconciler.Dockerfile` + `deploy/athena/reports_table.sql`.
  - `TestReconcile_LocalStack_FaultInjectionTo100PercentCoverage`: E2E en LocalStack con fault injection (missing + stuck > 2h) → re-despacho → 2.ª corrida idempotente (0 re-despachos) → worker completa el 100 % → cobertura **100 %** + JSON/CSV en S3.
  - `BenchmarkReconcile1M` + proyección 1M / 100M en [reconciler-bench.md](reconciler-bench.md).
- **Fase 3 & 4 — Alarmas, 7 Runbooks y Dashboard (`005-T018`..`T021`)**:
  - `deploy/terraform/modules/observability/` (7 alarmas + SNS cifrado KMS + CloudWatch Dashboard + tabla Glue/Athena).
  - 7/7 runbooks en `docs/runbooks/`: `dispatch-lag.md`, `queue-age.md`, `dlq.md`, `coverage-low.md`, `auth-errors.md`, `lambda-quota.md`, `provider-capacity.md`.
  - [observability-dashboard.md](observability-dashboard.md).

## 2. Evidencia de gates y cobertura
- `make test` (`-short -race -cover`) & `make test-full` (`-race`): **PASS**
- `make cover`:
  - `internal/domain`: **91,5 %** (imports 100 % stdlib)
  - `internal/app/assignslots`: **90,3 %**
  - `internal/app/capacity`: **94,4 %**
  - `internal/app/dispatch`: **90,5 %**
  - `internal/app/execute`: **94,7 %**
  - `internal/app/reconcile`: **94,0 %**
  - `internal/app/redrive`: **97,1 %**
  - **Total `domain + app`**: **92,4 %** (gate ≥ 90 %)
- E2E LocalStack (`TestReconcile_LocalStack_FaultInjectionTo100PercentCoverage`): **PASS** (27,40 s)
- `BenchmarkReconcile1M`: **235,2 ms/op** para 1.002.240 registros
- `gofmt`, `go vet -tags=integration`, `golangci-lint run --build-tags=integration`, `staticcheck -tags=integration`: **0 issues**
- `govulncheck ./...`: **0 vulnerabilidades**

## 3. Cumplimiento de Success Criteria (`005/spec.md`)
| SC | Criterio | Evidencia | Estado |
|---|---|---|---|
| SC-001 | `DailyCoverage ≥ 99,9 %` con fault injection; 100 % tras re-despacho | `TestReconcile_LocalStack_FaultInjectionTo100PercentCoverage`: 10 missing + 5 stuck re-despachados → **100,0 %** cobertura | ✅ |
| SC-002 | Reconciler procesa 1M en < 10 min y 100M en < 2 h | `BenchmarkReconcile1M`: 235 ms CPU; proyección I/O DynamoDB: < 30 s (1M) y ~15 min worst-case (100M) en [reconciler-bench.md](reconciler-bench.md) | ✅ |
| SC-003 | Cualquier ejecución rastreable punta a punta con `trace_id` | W3C `traceparent` OTel generado/propagado en SQS `MessageAttributes` y HTTP `traceparent`, verificado en `TestTraceParent_RoundTrip` y golden log `trace_id` | ✅ |

## 4. Desvíos y tests modificados (Regla B)
- **Desvíos menores**: Ninguno.
- **Tests existentes modificados (Regla B)**: Ninguno.
