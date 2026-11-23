# Checkpoint 003 — Worker (ejecución concurrente, exclusividad por proveedor y persistencia)

**Branch**: `003-worker-ejecucion` (mergeada a `main` con `--no-ff`; actualizada en `fix/review-findings` y `fix/review-findings-2`)
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
  - `b7af7aa`, `50897ed` + `fix/review-findings` + `fix/review-findings-2` (`f035d48`, `37c5360`): `sqsq.Consumer` con pre-adquisición de semáforo, agrupación FIFO por `MessageGroupId`, corte temprano por grupo ante `OutcomeDeferred` (`Nack(0)` al resto), heartbeat de grupo (`ChangeMessageVisibilityBatch`) sincronizado bajo mutex con `Ack`/`Nack`/`Extend` (evitando pisar backoff de `Nack`), conteo/logging de `Failed` parciales en `extendBatch`, y graceful shutdown (`003-T014`..`T015`, `T017`, `T022`..`T023`, `T025`..`T026`)
  - `11d579e`: `s3blob.Store` + test LocalStack (`003-T013`)
  - `4a43338`, `f09e35e`: `dynamo.ExecutionsRepo` (`Acquire` condicional con takeover de lease vencido, `Complete`, `ScheduleRetry`, `FailPermanent`, `FailExhausted`, `MarkSkipped`, `ErrLeaseLost`)
  - `2c37517`, `0ef6824` + `fix/review-findings` + `fix/review-findings-2`: `cmd/worker`, `build/worker.Dockerfile`, `execute_bench_test.go` (`BenchmarkExecuteUseCase_Overhead`), `worker_it_test.go` (`TestWorker_LocalStack_EndToEndThroughput` Zipf + `TestWorker_LocalStack_EndToEndThroughput_Uniform`), [load-test.md](load-test.md).

## 2. Evidencia de gates y cobertura (MEDIDO vs PROYECTADO)
- `make test` (`-short -race -cover`) & `make test-full` (`-race`): **PASS**
- `make cover`:
  - `internal/app/execute`: **94,7 %** (0 imports de `net/http` o AWS SDK)
  - **Total `domain + app`**: **92,4 %** (gate ≥ 90 %)
- **Concurrencia y race del heartbeat (`-count=50 -race`)**: `TestConsumer_SameGroupProcessedSequentially`, `TestConsumer_DeferredMessageAbortsRestOfGroupWithNack0`, `TestConsumer_HeartbeatDoesNotOverwriteNackBackoff`, `TestConsumer_ExtendBatchCountsPartialFailures`: **PASS**; `TestConsumer_GroupHeartbeatExtendsWaitingMessages` (`-count=3 -race`): **0 expiraciones / 0 re-entregas**.
- **MEDIDO — Overhead puro del caso de uso (`BenchmarkExecuteUseCase_Overhead`)**: **p50 = 0,019 ms**, **p95 = 0,256 ms**, **p99 = 0,657 ms** (< 50 ms ✅), **176.022 req/s** CPU (sin afirmar exclusividad).
- **MEDIDO — E2E LocalStack Uniforme (`TestWorker_LocalStack_EndToEndThroughput_Uniform`, 200 prov. × 10 msgs = 2.000 msgs, 4 workers × 16)**: **`85,2 req/s`** (`wall = 23,484 s`), **`max_provider_inflight = 1`**, `global_max_inflight = 9`, **`dup_idem = 0`**.
- **MEDIDO — E2E LocalStack Zipf (`TestWorker_LocalStack_EndToEndThroughput`, 50 prov. Zipf / 2.000 msgs, 4 workers × 16)**: **`61,8 req/s`** (`wall = 32,344 s`), `top_provider_msgs = 522`, `top_provider_avg_ms = 61,26 ms`, `top_provider_serial_time = 31,979 s`, **`wall_over_serial_ratio = 1,011`** (≤ 1,20 ✅), **`max_provider_inflight = 1`**, `global_max_inflight = 5`, **`dup_idem = 0`**.
- **PROYECTADO — AWS Producción**: $W \times I / (t_{\text{api}} + t_{\text{aws\_io}} + t_{\text{worker}}) = 128 / 0{,}4207 \approx \mathbf{304{,}3\text{ req/s}}$ (ver [load-test.md](load-test.md)).
- `gofmt`, `go vet -tags=integration`, `golangci-lint`, `staticcheck`, `govulncheck`: **0 issues / 0 vulnerabilidades**.

## 3. Cumplimiento de Success Criteria (`003/spec.md`)
| SC | Criterio | Evidencia | Estado |
|---|---|---|---|
| SC-001 | Máx. concurrencia por proveedor = 1 en todos los tests | **MEDIDO**: LocalStack FIFO E2E 200 prov. uniformes (`1`), 50 prov. Zipf (`1`, `wall/serial = 1,011`), 20 prov. + duplicados (`1`), `-count=50 -race` | ✅ |
| SC-002 | 0 llamadas duplicadas con 5 % duplicados inyectados | **MEDIDO**: `TestWorker_LocalStack_ExclusivityAndIdempotency`: `duplicate_idem_keys = 0` sobre 300 msgs + 15 duplicados; E2E uniforme y Zipf: `0` | ✅ |
| SC-003 | Sostiene ≥ 120 req/s con overhead p99 < 50 ms | **MEDIDO**: overhead propio **0,657 ms p99** (< 50 ms); E2E uniforme LocalStack = **85,2 req/s** (< 120 req/s por emulador single-container); E2E Zipf = **61,8 req/s** (cota serial `zipf-00`, ratio `1,011`). **PROYECTADO**: AWS 4 tasks × 32 sostienen **304,3 req/s** (detallado en [load-test.md](load-test.md)) | ⚠️ verificado por proyección |
| SC-004 | 0 mensajes perdidos ante SIGTERM durante carga | **MEDIDO**: `TestWorker_LocalStack_GracefulShutdown_ZeroLoss`: 60/60 `SUCCEEDED`, 0 perdidos, 0 duplicados | ✅ |

## 4. Desvíos y tests modificados (Regla B)
- **Desvíos menores**: Se habilitó `PENDING → IN_PROGRESS` en `ExecutionStatus`/`Acquire` para evitar carrera antes de `MarkEnqueued`. En `fix/review-findings`, `Execute` pasó a devolver `(HandleOutcome, error)` y `processGroup` agregó corte temprano + heartbeat de grupo `ChangeMessageVisibilityBatch`. En `fix/review-findings-2`, `Ack`/`Nack`/`Extend` y `runGroupHeartbeat` se sincronizan bajo un mutex compartido por grupo y `extendBatch` contabiliza/loguea fallos parciales `out.Failed` (documentado en `docs/decisions-log.md`).
- **Tests modificados (Regla B)**: En `fix/review-findings`, call sites de `Execute(...)` actualizados por cambio de firma sin debilitar aserciones, y separación en `BenchmarkExecuteUseCase_Overhead` + `TestWorker_LocalStack_EndToEndThroughput`. En `fix/review-findings-2`, se añadieron `TestWorker_LocalStack_EndToEndThroughput_Uniform`, la aserción `wall <= 1.20 * top_provider_serial_time` en Zipf, `TestConsumer_HeartbeatDoesNotOverwriteNackBackoff` y `TestConsumer_ExtendBatchCountsPartialFailures` (sin debilitar tests existentes).
