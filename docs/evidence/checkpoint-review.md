# Checkpoint Review — Correcciones de auditoría (`fix/review-findings`)

**Branch**: `fix/review-findings` (mergeada a `main` con `--no-ff`)
**Estado**: Completada (los 4 hallazgos cerrados con Spec-first + TDD RED → GREEN → REFACTOR).

## 1. Hallazgos resueltos y commits
1. **Spec-first (`831b72a`)**: actualizados `docs/architecture.md` (ADR-006), `specs/003-worker-ejecucion/plan.md`, `specs/004-errores-y-reintentos/plan.md` y `docs/decisions-log.md` con corte intra-batch ante diferimiento, heartbeat de grupo `ChangeMessageVisibilityBatch` y separación benchmark puro vs E2E LocalStack.
2. **Hallazgo 2 — Backoff intra-batch (`28da8b0`, `23e1bec`, `8887bfb`, `a7c37e6`)**:
   - **RED**: `TestExecute_HandleOutcome_DoneVsDeferred` y `TestConsumer_DeferredMessageAbortsRestOfGroupWithNack0` (batch de 5 mensajes del mismo grupo donde el 2.º devuelve `OutcomeDeferred` → fallaba ejecutando los 5).
   - **GREEN**: `execute.UseCase.Execute` y `sqsq.Handler.Execute` devuelven `(execute.HandleOutcome, error)` (`OutcomeDone | OutcomeDeferred`). Ante `OutcomeDeferred` (`RetryLater`, `FailExhausted`, breaker abierto, `LeasedByOther` o error de persistencia), `sqsq.Consumer.processGroup` frena el procesamiento de ese grupo y libera los mensajes restantes (`m3..m5`) con `Nack(0)`. Verificado con `-count=50 -race`.
3. **Hallazgo 3 — Heartbeat de mensajes en espera (`23e1bec`, `a7c37e6`)**:
   - **RED**: `TestConsumer_GroupHeartbeatExtendsWaitingMessages` (VT=2 s, 4 mensajes × 700 ms = 2,8 s → fallaba con re-entrega de `rh-w3` y `rh-w4` por vencimiento de VT mientras esperaban turno).
   - **GREEN**: `sqsq.Consumer.runGroupHeartbeat` ejecuta `ChangeMessageVisibilityBatch` cada `VT/2` sobre `items[activeIdx:]` (mensaje activo + todos los mensajes en espera del mismo grupo). Resultado: **0 expiraciones / 0 re-entregas**.
4. **Hallazgo 1 — Separación honesta de overhead vs E2E real**:
   - Renombrado a `BenchmarkExecuteUseCase_Overhead` (`internal/app/execute/execute_bench_test.go`), reportando únicamente overhead y throughput puro en memoria sin afirmar exclusividad: **p50 = 0,019 ms**, **p95 = 0,256 ms**, **p99 = 0,657 ms**, **176.022 req/s**.
   - Agregado `TestWorker_LocalStack_EndToEndThroughput` (`internal/app/execute/worker_it_test.go`): LocalStack real SQS FIFO + `sqsq.Consumer` + `dynamo.ExecutionsRepo` + `mockapi`, **50 proveedores Zipf**, **2.000 mensajes**, **4 workers × 16 inflight**:
     - `wall = 26,01 s`, **throughput E2E real = 76,9 req/s** (`top_provider_msgs = 522`)
     - **`max_provider_inflight = 1`** (medido por `mockapi`, sin serialización en el test)
     - `global_max_inflight = 9`, **`dup_idem = 0`**
   - Actualizados [load-test.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/load-test.md), [checkpoint-003.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/checkpoint-003.md) y [final-report.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/final-report.md) explicando cuantitativamente el techo de LocalStack/laptop vs AWS.
5. **Hallazgo 4 — Transparencia de Checkov skipped**:
   - Eliminado el flag redundante `skip_check` en `.github/workflows/ci.yml` (sólo anotaciones inline justificadas).
   - Documentado explícitamente en [checkpoint-006.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/checkpoint-006.md) y [final-report.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/final-report.md): **Checkov 306 passed / 0 failed / 6 skipped (justificados)** con la lista completa y razón de cada uno (`CKV_AWS_144`, `CKV_AWS_18`, `CKV2_AWS_62`, `CKV_AWS_109`, `CKV_AWS_111`, `CKV_AWS_356`).

## 2. Gates verificados tras las correcciones
- `make test` (`-short -race -cover`), `make test-full` (`-race`), `make cover`: **PASS — 92,4 %** (`domain + app`)
- `-count=50 -race` en `sqsq.Consumer` (secuencialidad + abort on defer): **PASS**
- `TestWorker_LocalStack_EndToEndThroughput` (50 Zipf / 2.000 msgs / 4 workers): **PASS** (`76,9 req/s`, `max_provider_inflight=1`, `dup_idem=0`)
- `TestWorker_LocalStack_RetriesExhaustionIsolationAndRedrive`: **PASS**
- `gofmt`, `go vet -tags=integration`, `golangci-lint`, `staticcheck`, `govulncheck`, `actionlint`, `terraform fmt -check`, `tflint`, Checkov: **PASS / 0 issues / 0 vulnerabilidades / 0 failed**.

## 3. Regla B — Tests modificados
- Call sites de `Execute(...)` en `execute_test.go`, `policy_adapter_test.go` y `consumer_test.go`: actualizados por cambio de firma `error` → `(HandleOutcome, error)`, sin relajar aserciones.
- `TestWorker_LoadBenchmark_Zipf200Providers` → renombrado y separado en `BenchmarkExecuteUseCase_Overhead` + `TestWorker_LocalStack_EndToEndThroughput` por pedido explícito de revisión (registrado en `docs/decisions-log.md`).

---

# Ronda 2 — Ajustes finales (`fix/review-findings-2`)

**Branch**: `fix/review-findings-2` (mergeada a `main` con `--no-ff`)
**Estado**: Completada (los 2 ajustes cerrados con TDD RED → GREEN + auditoría honesta MEDIDO vs PROYECTADO).

## 1. Ajustes resueltos y commits
1. **Ajuste 2 — Race del heartbeat vs `Nack(backoff)` y fallos parciales de `extendBatch` (`f035d48`, `37c5360`)**:
   - **RED (`f035d48`)**:
     - `TestConsumer_HeartbeatDoesNotOverwriteNackBackoff`: el handler difiere el mensaje con `Nack(30s)` dentro de `Execute` y un tick de heartbeat ocurre inmediatamente después → fallaba con `final visibility for deferred rh-d1 = [30 1], want last value 30`.
     - `TestConsumer_ExtendBatchCountsPartialFailures`: `ChangeMessageVisibilityBatch` devuelve 1 entrada `Failed` → fallaba con `HeartbeatFailures() = 0, want >= 1`.
   - **GREEN (`37c5360`)**:
     - `groupHeartbeatState` con mutex `mu` y mapa `settled` registrado por `ReceiptHandle` en `Consumer.groups`. `Ack`/`Nack`/`Extend` toman `gs.mu`, marcan `settled[rh] = true` y ejecutan la llamada SQS bajo el mismo mutex que `runGroupHeartbeat` (el cual filtra `!settled[m.ReceiptHandle]`), eliminando la carrera por construcción.
     - `extendBatch` inspecciona `out.Failed`, incrementa `heartbeatFailures` (`atomic.Int64`, expuesto vía `HeartbeatFailures()`) y emite `slog.Warn` estructurado por cada fallo parcial.
     - Verificado con `-count=50 -race` (**PASS**).
2. **Ajuste 1 — SC-003, E2E Uniforme 200 proveedores, cota serial Zipf y auditoría MEDIDO vs PROYECTADO**:
   - **`TestWorker_LocalStack_EndToEndThroughput_Uniform`** (200 proveedores uniformes × 10 msgs = 2.000 msgs, 4 workers × 16 inflight en LocalStack real):
     - `wall = 23,484 s`, **throughput real = `85,2 req/s`** (< 120 req/s → `003/SC-003` pasa a **`⚠️ verificado por proyección`**).
     - **`max_provider_inflight = 1`**, `global_max_inflight = 9`, **`dup_idem = 0`**.
   - **`TestWorker_LocalStack_EndToEndThroughput` (Zipf 50 proveedores, 2.000 msgs)**:
     - Reporta `top_provider_msgs = 522`, `top_provider_avg_ms = 61,26 ms`, `top_provider_serial_time = 31,979 s`, `wall = 32,344 s`, **`wall_over_serial_ratio = 1,011`** (+1,1 %, verificando `wall <= 1.20 * top_provider_serial_time` ✅), `throughput = 61,8 req/s`, `max_provider_inflight = 1`, `global_max_inflight = 5`, `dup_idem = 0`.
   - **Documentación actualizada (`load-test.md`, `checkpoint-003.md`, `final-report.md`, `checkpoint-{001,002,005,006}.md`)**: separación estricta **MEDIDO** vs **PROYECTADO** (con fórmula explícita AWS `128 / 0,4207 = 304,3 req/s`) y reclasificación honesta de todos los SC verificados por cálculo/proyección/estático (`001/SC-003`, `001/SC-004`, `002/SC-002`, `003/SC-003`, `005/SC-002`, `006/SC-002`, `006/SC-003`, `006/SC-004`).

## 2. Gates verificados tras Ronda 2
- `make test` (`-short -race -cover`), `make test-full` (`-race`), `make cover`: **PASS — 92,4 %** (`domain + app`).
- `go test -race -count=50` en `internal/adapters/sqsq` (heartbeat/Nack race, partial failures, abort on defer, sequentiality) + `-count=3` en heartbeat multi-segundo: **PASS**.
- LocalStack E2E: `TestWorker_LocalStack_EndToEndThroughput` (Zipf, `ratio=1.011`) + `TestWorker_LocalStack_EndToEndThroughput_Uniform` (`85.2 req/s`, `max_provider_inflight=1`, `dup_idem=0`) + `make it`: **PASS**.
- `gofmt`, `go vet -tags=integration`, `golangci-lint`, `staticcheck`, `govulncheck`, `actionlint`, `terraform fmt -check`: **PASS / 0 issues / 0 vulnerabilidades**.

