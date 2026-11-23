# Reporte Final — Tapi Scheduled Provider Queries

**Fecha**: 2026-11-23 · **Branch**: `main` (6 features + `fix/review-findings` + `fix/review-findings-2` mergeadas con `--no-ff`, sin `git push` ni `terraform apply` externo)
**Metodología**: SDD (`spec.md` → `plan.md` → `tasks.md`) + TDD estricto (`RED → GREEN → REFACTOR`) + Clean Architecture en Go 1.26.

---

## 1. Matriz de trazabilidad (`docs/architecture.md §1`)

| # | Requisito / consideración del enunciado | FRs cubiertos | Tests ejecutados | Evidencia verificada (MEDIDO vs PROYECTADO) | Estado |
|---|---|---|---|---|---|
| **R1** | Consulta **diaria** para **cada** registro, de forma **automática** | `001/FR-001`, `FR-005b`; `002/FR-001..006`; `005/FR-001..004` | `TestTick_FullDay1440Slots_14400Records_DispatchedExactlyOnce`, `TestDispatcher_LocalStack_Simulation_And_ImmediateDispatch`, `TestReconcile_LocalStack_FaultInjectionTo100PercentCoverage` | **MEDIDO**: 14.400 registros en 1.440 ticks despachados exactamente 1 vez; rebalanceo mismo día despachado vía `ImmediateDispatcher`; fault injection (10 missing + 5 stuck) → **100,0 % cobertura** tras re-despacho; `make demo` live E2E **PASS** | ✅ |
| **R2** | Consultas **distribuidas a lo largo del día** | `001/FR-002..003`, `FR-004b`; `002/FR-001` | `TestSlotAssigner_GlobalUniformityScaleProperty1M`, `TestSlotAssigner_PerProviderUniformity`, `BenchmarkAssign1M` | **MEDIDO**: desvío máx. 1M uniforme **0,352 %**; 1M Zipf **1,232 %** (límite ≤ 10 %); exceso por proveedor sobre `⌈k/1440⌉` = **0**; `BenchmarkAssign1M` = **29,5 ms/op** (0 allocs) | ✅ |
| **R3** | **Guardar el resultado** en una base de datos | `003/FR-002`, `FR-004` | `TestExecute_SuccessInlineResult`, `TestExecute_OversizedResponseToS3BeforeComplete`, `TestExecutionsRepo_Worker_Integration`, `TestStore_PutGet_LocalStack` | **MEDIDO**: transiciones condicionales atómicas en DynamoDB `executions` (TTL 90d, `gsi_run_date_status`); payloads > 350 KiB escritos a S3 (`results/run_date=D/provider=P/id.json`) **antes** de `Complete` | ✅ |
| **R4** | Errores **reintentables y no reintentables** | `004/FR-001..007`; `005/FR-005..006` | `TestClassifier_TruthTable`, `TestParseRetryAfter`, `TestRetryPolicy_BackoffProperties`, `TestProviderBreaker_LifecycleWithFakeClock`, `TestConsumer_DeferredMessageAbortsRestOfGroupWithNack0`, `TestConsumer_HeartbeatDoesNotOverwriteNackBackoff`, `TestWorker_LocalStack_RetriesExhaustionIsolationAndRedrive` | **MEDIDO**: 100 % tabla US1 cubierta; backoff `[30s, 15m]` ±20 % respetando `Retry-After`; corte intra-batch de grupo ante `OutcomeDeferred` (`Nack(0)` al resto) y mutex heartbeat/`Nack` evitando pisar backoff; `bad503` → `FAILED_EXHAUSTED` + DLQ (`maxReceiveCount=6`) sin degradar sanos; `bad400` → **0 reintentos**; `ProviderBreaker` 20/50 %/60 s; `cmd/redrive` validado en LocalStack | ✅ |
| **R5** | **Evitar concurrencia a un mismo proveedor** | `002/FR-003`; `003/FR-001..002`, `FR-005..006`; `001/FR-006` | `TestConsumer_SameGroupProcessedSequentially` (`-count=50 -race`), `TestConsumer_GroupHeartbeatExtendsWaitingMessages`, `TestWorker_LocalStack_ExclusivityAndIdempotency`, `TestWorker_LocalStack_EndToEndThroughput_Uniform`, `TestWorker_LocalStack_EndToEndThroughput`, `TestCapacityCheck` | **MEDIDO**: SQS FIFO `MessageGroupId=provider` + agrupación secuencial intra-batch + heartbeat de grupo sincronizado + lease DynamoDB: **`max_provider_inflight = 1`** medido externamente por `mockapi` en LocalStack E2E uniforme (200 prov. / 2.000 msgs), Zipf (50 prov., `wall/serial = 1,011`), 20 prov. + duplicados y `make demo`; `app/capacity` integrado en `cmd/reconciler` (`001-T025`) | ✅ |
| **C1** | **1M de filas, escalando N veces** | `001/FR-004`, `FR-004b`; `002/FR-001`; `005/FR-002`; `006/FR-003` | `BenchmarkStreamingBackfill1M` vs `4M`, `TestBackfill_LocalStack_Integration` (20k vs 80k), `BenchmarkReconcile1M`, `BenchmarkExecuteUseCase_Overhead`, `TestWorker_LocalStack_EndToEndThroughput_Uniform`, `TestWorker_LocalStack_EndToEndThroughput` | **MEDIDO**: cero `Scan`; heap backfill constante (`1.466 KiB` @ 1M vs `1.433 KiB` @ 4M; LocalStack `2.655 KiB` @ 20k vs `3.067 KiB` @ 80k); overhead worker **0,657 ms p99** (`176k req/s` CPU); E2E uniforme LocalStack **85,2 req/s** (`wall = 23,484 s`); E2E Zipf LocalStack **61,8 req/s** (`wall/serial = 1,011`); reconciler **235,2 ms/1M** CPU. **PROYECTADO AWS**: **304,3 req/s** (`128 / 0,4207 s`), backfill 1M ~27,8 min @ 600 WCU/s, reconciler 1M < 30 s / 100M ~15 min | ⚠️ escala AWS proyectada (ver §2) |
| **C2** | **Misma región y misma cuenta de AWS** | `003/FR-003`; `006/FR-001..002`, US4 | `TestClient_Call_SigV4HeadersAndTimeout`, `deploy/terraform/modules/{network,security}`, `terraform validate`, Checkov | **MEDIDO/ESTÁTICO**: VPC 3 AZs privadas **sin NAT** (2 Gateway + 8 Interface VPC Endpoints), SigV4 + Resource Policy `aws:SourceVpce`, KMS CMK, PITR 35d, sin cross-account | ✅ |
| **C3** | La API interna **ejecuta dos lambdas** | `003/US4`, `FR-003`; `004/US1`; `006/T020` | `TestValidateConcurrencyBudget`, `TestClient_Call_SigV4HeadersAndTimeout`, `deploy/terraform/stack/tests/stack.tftest.hcl` | **MEDIDO/ESTÁTICO**: fail-fast `k × t ≤ LAMBDA_BUDGET / 2`; timeout HTTP 25 s (< APIGW 29 s); `LAMBDA_BUDGET` derivado de Service Quotas `L-B99A9384` → `worker_max_tasks` y alarma `ConcurrentExecutions > 80 %` | ✅ |
| **T1** | **Todo el stack de AWS** | `006/FR-001..006`; `005/T018..020` | `terraform fmt/validate/test`, `tflint`, `checkov`, `actionlint` | **ESTÁTICO/MOCKS (Regla E)**: 6 módulos Terraform (`network`, `security`, `data`, `messaging`, `compute`, `observability`) + 3 envs (`dev`, `staging`, `prod`), CI/CD OIDC en GitHub Actions, 7 runbooks, costos y FMEA | ✅ (estático/mocks) |

---

## 2. Estado consolidado de todos los Success Criteria (SC) — MEDIDO vs PROYECTADO

> Regla de rigor: sólo se marcan con **✅** los criterios verificados por medición directa al nivel especificado en el criterio. Todo criterio sustentado en cálculo, extrapolación de escala AWS o validación estática/mocks sin ejecución live en AWS/GH Actions se marca explícitamente como **⚠️ verificado por proyección** o **⚠️ verificado estático/mocks**.

| Feature | SC | Criterio | Evidencia (MEDIDO vs PROYECTADO) | Estado |
|---|---|---|---|---|
| **001** | SC-001 | 100 % registros habilitados con slot válido | **MEDIDO**: `TestBackfill_LocalStack_Integration` + `cmd/seed` + `cmd/slotter` | ✅ |
| **001** | SC-002 | Desvío máx. por minuto ≤ 10 % con 1M | **MEDIDO**: 1M uniforme **0,352 %**; 1M Zipf **1,232 %**; 100 corridas rapid 1M PASS | ✅ |
| **001** | SC-003 | Backfill 1M en < 30 min (4 workers) | **MEDIDO**: CPU `BenchmarkStreamingBackfill1M` = **46,4 ms/1M** y heap constante en LocalStack (`20k vs 80k`). **PROYECTADO**: `1.000.000 / 600 WCU/s ≈ 27,8 min` (on-demand escala hasta 40k WCU) | ⚠️ verificado por proyección |
| **001** | SC-004 | Query de slot < 2 s (1M) / < 10 s (100M) | **MEDIDO**: `TestRequestsRepo_Integration` en LocalStack (**0,22 s** en dataset chico). **PROYECTADO**: GSI `gsi_slot_shard` paralelo (`S=4` @ 1M ≈ 175 items/shard; `S=32` @ 100M ≈ 2.170 items/shard) | ⚠️ verificado por proyección |
| **002** | SC-001 | Simulación 1 día: 100 % despachado exactamente 1 vez | **MEDIDO**: 1.440 ticks / 14.400 registros exactos + E2E LocalStack + `make demo` | ✅ |
| **002** | SC-002 | Despacho de 1 slot (1M total, ~700 reg.) p99 < 5 s | **MEDIDO**: 14.400 reg. / 1.440 slots in-memory (< 2 s) y 120 reg. en LocalStack (< 0,5 s). **PROYECTADO**: 4 shards paralelos + `SendMessageBatch` de a 10 en AWS (~70 batches → < 1 s) | ⚠️ verificado por proyección |
| **002** | SC-003 | Failover de líder ≤ 40 s sin pérdida/duplicados | **MEDIDO**: lease 30 s / renew 10 s + `TransactWriteItems` fencing (`-race` en LocalStack) | ✅ |
| **002** | SC-004 | `DispatchLagMinutes = 0` en estado estable | **MEDIDO**: `TestTick_CatchUp` + EMF metrics | ✅ |
| **003** | SC-001 | Máx. concurrencia por proveedor = 1 en todos los tests | **MEDIDO**: **`max_provider_inflight = 1`** en LocalStack E2E uniforme (200 prov. / 2.000 msgs), Zipf (50 prov. / 2.000 msgs), 20 prov. + duplicados, `make demo` y `-count=50 -race` | ✅ |
| **003** | SC-002 | 0 llamadas duplicadas con 5 % duplicados inyectados | **MEDIDO**: **`duplicate_idem_keys = 0`** en LocalStack (300 + 15 duplicados con DedupId distinto) y en E2E uniforme y Zipf (2.000 msgs c/u) | ✅ |
| **003** | SC-003 | Sostiene ≥ 120 req/s con overhead p99 < 50 ms | **MEDIDO**: overhead propio (`BenchmarkExecuteUseCase_Overhead`) **p99 = 0,657 ms** (< 50 ms ✅), **176.022 req/s** CPU; E2E uniforme LocalStack (200×10) = **85,2 req/s** (`wall = 23,484 s`); E2E Zipf LocalStack (50 prov.) = **61,8 req/s** (`wall/serial = 1,011`). **PROYECTADO AWS**: `128 / (0,400 + 0,020 + 0,000657) =` **304,3 req/s** (ver [load-test.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/load-test.md)) | ⚠️ verificado por proyección |
| **003** | SC-004 | 0 mensajes perdidos ante SIGTERM bajo carga | **MEDIDO**: `TestWorker_LocalStack_GracefulShutdown_ZeroLoss`: **60/60 `SUCCEEDED`** | ✅ |
| **004** | SC-001 | 100 % tabla US1 cubierta table-driven | **MEDIDO**: `TestClassifier_TruthTable` (20 casos + overrides) + `TestParseRetryAfter` | ✅ |
| **004** | SC-002 | Proveedor con 100 % 503 no degrada a los demás | **MEDIDO**: `TestWorker_LocalStack_RetriesExhaustionIsolationAndRedrive` + `TestConsumer_DeferredMessageAbortsRestOfGroupWithNack0` | ✅ |
| **004** | SC-003 | 0 ejecuciones no terminales 24 h después de `run_date` | **MEDIDO** (mecanismo): `maxReceiveCount=6` → `FAILED_EXHAUSTED` + `reconciler` en LocalStack; ventana productiva de 24 h cubierta por EventBridge 00:30 | ✅ |
| **004** | SC-004 | Error no reintentable genera 0 reintentos | **MEDIDO**: `bad400` en LocalStack: `calls == 1`, `FAILED_PERMANENT`, DLQ vacía | ✅ |
| **005** | SC-001 | `DailyCoverage ≥ 99,9 %` con fault injection; 100 % tras re-despacho | **MEDIDO**: `TestReconcile_LocalStack_FaultInjectionTo100PercentCoverage`: **100,0 %** | ✅ |
| **005** | SC-002 | Reconciler 1M < 10 min y 100M < 2 h | **MEDIDO**: `BenchmarkReconcile1M` = **235,2 ms** CPU. **PROYECTADO**: DynamoDB I/O < 30 s (1M) / ~15 min (100M) en [reconciler-bench.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/reconciler-bench.md) | ⚠️ verificado por proyección |
| **005** | SC-003 | Ejecución rastreable punta a punta por `trace_id` | **MEDIDO**: OTel W3C `traceparent` + SQS `MessageAttributes` + golden log `trace_id` | ✅ |
| **006** | SC-001 | `make up && make demo` en < 5 min con Docker | **MEDIDO**: ejecutado live en ~1 min: 100 % ejecutado, `max_inflight=1`, `dup=0` | ✅ |
| **006** | SC-002 | Pipeline de PR < 10 min | **ESTÁTICO/PROYECTADO**: `ci.yml` en 4 jobs paralelos, validado con `actionlint` (0 issues); sin corrida real en GitHub Actions | ⚠️ verificado por proyección |
| **006** | SC-003 | `checkov` sin HIGH; Trivy sin CRITICAL | **MEDIDO localmente**: Checkov **306 passed / 0 failed / 6 skipped (justificados)** + `govulncheck` **0 vulns**; Trivy sobre `distroless:nonroot` configurado en `ci.yml` (sin corrida en GH Actions) | ⚠️ parcial (Checkov + govulncheck medidos; Trivy en CI) |
| **006** | SC-004 | De cero a `dev` con 1 `terraform apply` + 1 pipeline | **ESTÁTICO/MOCKS (Regla E)**: `terraform fmt/validate/test` + `tflint` + `checkov` sin `apply` externo | ⚠️ verificado estático/mocks (Regla E) |

---

## 3. Gates de calidad, seguridad e IaC
- **Cobertura `domain + app` (`make cover`)**: **92,4 %** (gate ≥ 90 %):
  - `internal/domain`: **91,5 %** (imports 100 % stdlib — cero AWS SDK, cero `net/http`)
  - `internal/app/assignslots`: **90,3 %** · `capacity`: **94,4 %** · `dispatch`: **90,5 %** · `execute`: **94,7 %** · `reconcile`: **94,0 %** · `redrive`: **97,1 %** · `clock`: **100,0 %**
- **Go quality & security**: `make test`, `make test-full` (`-race`), `-count=50 -race` en `sqsq`, `gofmt`, `go vet -tags=integration`, `golangci-lint`, `staticcheck`, `govulncheck`: **PASS / 0 issues / 0 vulnerabilidades**.
- **IaC & CI/CD (Regla E)**: `actionlint` **0 issues**, `terraform fmt -check` **0 issues**, `terraform validate` **6/6 PASS**, `terraform test` con mocks **3/3 PASS**, `tflint` **0 issues**, **Checkov 306 passed / 0 failed / 6 skipped (justificados)**:
  1. `CKV_AWS_144` (`modules/data/main.tf` S3 cross-region replication): fuera de alcance por restricción C2 (single-region).
  2. `CKV_AWS_18` (`modules/data/main.tf` S3 access logging): diferido al bucket centralizado de auditoría organizacional.
  3. `CKV2_AWS_62` (`modules/data/main.tf` S3 event notifications): innecesario para blobs/reportes consultados vía Athena.
  4. `CKV_AWS_109` (`modules/security/main.tf` KMS key policy): AWS KMS exige `Resource = "*"` (acotado a esa única clave).
  5. `CKV_AWS_111` (`modules/security/main.tf` KMS key policy): mismo requisito AWS KMS; servicios AWS acotados por `aws:SourceAccount`.
  6. `CKV_AWS_356` (`modules/security/main.tf` KMS key policy): mismo requisito AWS KMS (`Resource = "*"` = esta CMK).

---

## 4. Desvíos consolidados y tests modificados (Regla B)
1. **Rediseño spec-first de `001` (`dff6551`, `5e2e08d`)**: streaming por proveedor sobre `gsi_provider` (`O(k_max)` memoria), histograma en `coordination` y seguridad mismo día `FR-005b` (`ImmediateDispatcher`).
2. **Transición `PENDING → IN_PROGRESS` en `003`**: previene carrera si el worker consume antes de `MarkEnqueued`.
3. **Correcciones de revisión (`fix/review-findings` y `fix/review-findings-2`, ADR-006)**:
   - `Handler.Execute` / `execute.UseCase.Execute` devuelven `(HandleOutcome, error)`; `processGroup` corta el batch del grupo ante `OutcomeDeferred` liberando el resto con `Nack(0)`.
   - Heartbeat a nivel de grupo con `ChangeMessageVisibilityBatch` sincronizado bajo un mutex por grupo con `Ack`/`Nack`/`Extend` (evita sobreescribir el backoff de `Nack` con `VT`) y conteo/logging de entradas `Failed` parciales en `extendBatch`.
   - Separación explícita **MEDIDO vs PROYECTADO** entre `BenchmarkExecuteUseCase_Overhead` (overhead CPU), `TestWorker_LocalStack_EndToEndThroughput_Uniform` (200 prov. × 10 = `85,2 req/s`), `TestWorker_LocalStack_EndToEndThroughput` (50 Zipf, `wall/serial = 1,011`), y proyección AWS (`304,3 req/s`), reclasificando todos los SC verificados por cálculo/proyección como `⚠️ verificado por proyección`.
4. **Regla E en `006`**: validación IaC y CI/CD sin efectos externos (sin `terraform apply` ni `git push`).
5. **Tests existentes modificados (Regla B)**:
   - `001` (`fd71df6`): `numProviders ∈ [10, 60]` + `TestSlotAssigner_GlobalUniformityScaleProperty1M` + `testing.Short()` (`docs/decisions-log.md`).
   - `001` (Regla G, `backfill_it_test.go`): escalado test de memoria LocalStack a `20k vs 80k` (`k_max=5k`) en `make it-full`.
   - `fix/review-findings`: call sites de `Execute(...)` actualizados a `(HandleOutcome, error)` sin debilitar aserciones; benchmark en memoria renombrado a `BenchmarkExecuteUseCase_Overhead` y reemplazado por `TestWorker_LocalStack_EndToEndThroughput` real (`docs/decisions-log.md`).
   - `fix/review-findings-2`: agregada aserción más estricta `wall <= 1.20 * top_provider_serial_time` en el test Zipf + tests nuevos (`Uniform`, `HeartbeatDoesNotOverwriteNackBackoff`, `ExtendBatchCountsPartialFailures`); cero aserciones debilitadas.

---

## 5. Índice de evidencia y checkpoints
- [checkpoint-001.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/checkpoint-001.md) · [checkpoint-002.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/checkpoint-002.md) · [checkpoint-003.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/checkpoint-003.md) · [checkpoint-004.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/checkpoint-004.md) · [checkpoint-005.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/checkpoint-005.md) · [checkpoint-006.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/checkpoint-006.md) · [checkpoint-review.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/checkpoint-review.md)
- [load-test.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/load-test.md) · [reconciler-bench.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/reconciler-bench.md) · [observability-dashboard.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/observability-dashboard.md)
- [architecture-diagram.svg](file:///Users/martingomezvega/challenge/tapi-challenge/docs/architecture-diagram.svg) · [costs.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/costs.md) · [failure-modes.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/failure-modes.md)
- Runbooks (`7/7`): `docs/runbooks/{dispatch-lag,queue-age,dlq,coverage-low,auth-errors,lambda-quota,provider-capacity}.md`
