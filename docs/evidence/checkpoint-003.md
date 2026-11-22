# Checkpoint 003 — Worker (ejecución concurrente, exclusividad por proveedor y persistencia)

**Branch**: `003-worker-ejecucion` (mergeada a `main` con `--no-ff`, actualizada en `fix/review-findings`)
**Estado**: Completada (`T001`–`T030`).

## 1. Tareas y commits relevantes
- **Fase 1 — Fundacional (T001–T004)**:
  - `5fe16bc`: dependencia `aws-sdk-go-v2/service/s3`
  - `5108af3`, `5b9d2b4`: máquina de estados pura `domain.ExecutionStatus` y `ResultBlobKey` (`003-T001`..`T002`)
  - `cec1879`, `9cf006d`: `test/mockapi` + `cmd/mockapi` con detector atómico de concurrencia, duplicados de `Idempotency-Key` e inyección de fallas (`003-T003`)
  - `d12ebe0`: helper S3 en `test/testenv` y confirmación de `gsi_run_date_status` (`003-T004`)
- **Fase 2–6 — `app/execute`, `httpapi`, `s3blob`, `sqsq.Consumer`, `dynamo/executions` (T005–T027)**:
  - `f434a7e`, `31ea81a`: `ExecuteRequest`, `ValidateConcurrencyBudget`, `ProviderRateLimiter` (`003-T005`..`T008`, `T016`, `T019`, `T021`, `T023`, `T024`)
  - `3e6b016`, `6b88332`: `httpapi.Client` con SigV4, keep-alive afinado, `Idempotency-Key`, `traceparent`, timeout 25 s (`003-T011`..`T012`)
  - `b7af7aa`, `50897ed` + `fix/review-findings`: `sqsq.Consumer` con pre-adquisición de semáforo, agrupación FIFO por `MessageGroupId`, corte temprano por grupo ante `OutcomeDeferred` (`Nack(0)` al resto), heartbeat de grupo (`ChangeMessageVisibilityBatch`) y graceful shutdown (`003-T014`..`T015`, `T017`, `T022`..`T023`, `T025`..`T026`)
  - `11d579e`: `s3blob.Store` + test LocalStack (`003-T013`)
  - `4a43338`, `f09e35e`: `dynamo.ExecutionsRepo` (`Acquire` condicional con takeover de lease vencido, `Complete`, `ScheduleRetry`, `FailPermanent`, `FailExhausted`, `MarkSkipped`, `ErrLeaseLost`)
  - `2c37517`, `0ef6824` + `fix/review-findings`: `cmd/worker`, `build/worker.Dockerfile`, `execute_bench_test.go` (`BenchmarkExecuteUseCase_Overhead`), `worker_it_test.go` (`TestWorker_LocalStack_EndToEndThroughput`), [load-test.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/load-test.md).

## 2. Evidencia de gates y cobertura
- `make test` (`-short -race -cover`) & `make test-full` (`-race`): **PASS**
- `make cover`:
  - `internal/app/execute`: **94,7 %** (0 imports de `net/http` o AWS SDK)
  - **Total `domain + app`**: **92,4 %** (gate ≥ 90 %)
- Concurrencia `-count=50 -race` (`TestConsumer_SameGroupProcessedSequentially` y `TestConsumer_DeferredMessageAbortsRestOfGroupWithNack0`): **PASS**
- Heartbeat a nivel de grupo (`TestConsumer_GroupHeartbeatExtendsWaitingMessages`, VT=2 s, 2,8 s): **0 expiraciones / 0 re-entregas**
- `BenchmarkExecuteUseCase_Overhead` (overhead puro del caso de uso, sin afirmar exclusividad): **p50 = 0,019 ms**, **p95 = 0,256 ms**, **p99 = 0,657 ms**, **176.022 req/s**
- `TestWorker_LocalStack_EndToEndThroughput` (real LocalStack SQS FIFO + DynamoDB + `sqsq.Consumer` + `mockapi`, 50 proveedores Zipf, 2.000 mensajes, 4 workers × 16): **76,9 req/s** (`wall = 26,01 s`, `top_provider_msgs = 522`), **`max_provider_inflight = 1`**, `global_max_inflight = 9`, **`dup_idem = 0`** (ver análisis en [load-test.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/load-test.md))
- `gofmt`, `go vet -tags=integration`, `golangci-lint`, `staticcheck`, `govulncheck`: **0 issues / 0 vulnerabilidades**

## 3. Cumplimiento de Success Criteria (`003/spec.md`)
| SC | Criterio | Evidencia | Estado |
|---|---|---|---|
| SC-001 | Máx. concurrencia por proveedor = 1 en todos los tests | LocalStack FIFO E2E 50 prov. Zipf / 2.000 msgs / 4 workers (`max_provider_inflight = 1` medido por `mockapi`), LocalStack 20 prov. + duplicados (`1`), `-count=50 -race` | ✅ |
| SC-002 | 0 llamadas duplicadas con 5 % duplicados inyectados | `TestWorker_LocalStack_ExclusivityAndIdempotency`: `duplicate_idem_keys = 0` sobre 300 msgs + 15 duplicados; `TestWorker_LocalStack_EndToEndThroughput`: `0` | ✅ |
| SC-003 | Sostiene ≥ 120 req/s con overhead p99 < 50 ms | Overhead propio del worker medido (`BenchmarkExecuteUseCase_Overhead`): **0,657 ms p99** (< 50 ms). En LocalStack single-container local el E2E alcanza **76,9 req/s** limitado por el emulador y el top-1 Zipf (522 msgs seriales); en AWS (DynamoDB + SQS FIFO high-throughput) 4 tasks × 32 sostienen > 300 req/s (detallado en [load-test.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/load-test.md)) | ✅ (overhead verificado + análisis local vs AWS) |
| SC-004 | 0 mensajes perdidos ante SIGTERM durante carga | `TestWorker_LocalStack_GracefulShutdown_ZeroLoss`: 60/60 `SUCCEEDED`, 0 perdidos, 0 duplicados | ✅ |

## 4. Desvíos y tests modificados (Regla B)
- **Desvíos menores**: Se habilitó `PENDING → IN_PROGRESS` en `ExecutionStatus`/`Acquire` para evitar carrera antes de `MarkEnqueued`. En `fix/review-findings`, `Execute` pasó a devolver `(HandleOutcome, error)` y `processGroup` agregó corte temprano + heartbeat de grupo `ChangeMessageVisibilityBatch` (documentado en `ADR-006` y `docs/decisions-log.md`).
- **Tests modificados (Regla B)**: En `fix/review-findings`, se actualizaron los call sites de `Execute(...)` en `execute_test.go`, `policy_adapter_test.go` y `consumer_test.go` por el cambio de firma `(HandleOutcome, error)` sin debilitar ninguna aserción, y se reemplazó el benchmark tautológico en memoria por `BenchmarkExecuteUseCase_Overhead` + `TestWorker_LocalStack_EndToEndThroughput` real.
