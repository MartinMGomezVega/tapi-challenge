# Checkpoint 003 — Worker (ejecución concurrente, exclusividad por proveedor y persistencia)

**Branch**: `003-worker-ejecucion` (mergeada a `main` con `--no-ff`)
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
  - `b7af7aa`, `50897ed`: `sqsq.Consumer` con pre-adquisición de semáforo, agrupación FIFO por `MessageGroupId`, heartbeat `VT/2` y graceful shutdown (`003-T014`..`T015`, `T017`, `T022`..`T023`, `T025`..`T026`)
  - `11d579e`: `s3blob.Store` + test LocalStack (`003-T013`)
  - `4a43338` + commit `003-T010`: `dynamo.ExecutionsRepo` (`Acquire` condicional con takeover de lease vencido, `Complete`, `ScheduleRetry`, `FailPermanent`, `FailExhausted`, `MarkSkipped`, `ErrLeaseLost`)
  - Commits `003-T018`/`T020`/`T027`/`T028`/`T029`/`T030`: `cmd/worker`, `build/worker.Dockerfile`, `worker_it_test.go`, `docs/evidence/load-test.md`.

## 2. Evidencia de gates y cobertura
- `make test` (`-short -race -cover`): **PASS**
- `make test-full` (`-race`): **PASS**
- `make cover`:
  - `internal/domain`: **93,3 %** (imports 100 % stdlib)
  - `internal/app/assignslots`: **90,3 %**
  - `internal/app/capacity`: **94,4 %**
  - `internal/app/dispatch`: **90,5 %**
  - `internal/app/execute`: **96,6 %** (0 imports de `net/http` o AWS SDK)
  - **Total `domain + app`**: **92,9 %** (gate ≥ 90 %)
- Concurrencia `-count=50 -race` (`TestConsumer_SameGroupProcessedSequentially`): **PASS** (2,2 s)
- Integración LocalStack (`dynamo`, `s3blob`, `execute` E2E + load benchmark Zipf 200 proveedores): **PASS** (ver [load-test.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/load-test.md))
- `gofmt`, `go vet -tags=integration`, `golangci-lint run --build-tags=integration`, `staticcheck -tags=integration`: **0 issues**
- `govulncheck ./...`: **0 vulnerabilidades**

## 3. Cumplimiento de Success Criteria (`003/spec.md`)
| SC | Criterio | Evidencia | Estado |
|---|---|---|---|
| SC-001 | Máx. concurrencia por proveedor = 1 en todos los tests | LocalStack FIFO 4 workers (`max_provider_inflight=1`), Zipf 200 prov. (`1`), `-count=50 -race` | ✅ |
| SC-002 | 0 llamadas duplicadas con 5 % duplicados inyectados | `TestWorker_LocalStack_ExclusivityAndIdempotency`: `duplicate_idem_keys=0` sobre 300 msgs + 15 duplicados inyectados | ✅ |
| SC-003 | Sostiene ≥ 120 req/s con p99 overhead < 50 ms | `TestWorker_LoadBenchmark_Zipf200Providers`: **635,3 req/s**, p99 overhead **9,92 ms** | ✅ |
| SC-004 | 0 mensajes perdidos ante SIGTERM durante carga | `TestWorker_LocalStack_GracefulShutdown_ZeroLoss`: 60/60 `SUCCEEDED`, 0 perdidos, 0 duplicados | ✅ |

## 4. Desvíos y tests modificados (Regla B)
- **Desvíos menores**: Se habilitó `PENDING → IN_PROGRESS` además de `ENQUEUED|RETRYING → IN_PROGRESS` en `ExecutionStatus` y `Acquire` para evitar carreras cuando el worker consume un mensaje antes de que el dispatcher complete `MarkEnqueued`.
- **Tests existentes modificados (Regla B)**: Ninguno (sólo se agregaron nuevos casos de test para superar el 90 % de cobertura en `internal/app/execute`).
