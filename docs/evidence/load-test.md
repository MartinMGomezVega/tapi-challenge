# Worker Load & Exclusivity Report (`003-worker-ejecucion`)

**Fecha**: 2026-11-21 · **Entorno**: Apple Silicon (Docker Desktop + LocalStack 4.0.3, Go 1.26.11, `-race`)

## 1. Exclusividad por proveedor e idempotencia E2E (`003-T018`, `003-T020`)
- **Test**: `TestWorker_LocalStack_ExclusivityAndIdempotency` (`internal/app/execute/worker_it_test.go`)
- **Infraestructura real**: LocalStack SQS FIFO (`DeduplicationScope=messageGroup`, `FifoThroughputLimit=perMessageGroupId`), DynamoDB (`requests`, `executions` con `gsi_run_date_status`), S3 (`worker-results-it`), HTTP `mockapi` con cliente SigV4 (`httpapi.Client`).
- **Escenario**:
  - 20 proveedores, 4 workers concurrentes (`sqsq.Consumer` × 2 receivers cada uno) consumiendo de la misma cola FIFO.
  - 5 % de mensajes duplicados inyectados con `MessageDeduplicationId` distinto (forzando la prueba de la capa 2 DynamoDB `Acquire`).
  - Adicionalmente `TestConsumer_SameGroupProcessedSequentially` ejecutado con `-count=50 -race` verificando serialización estricta intra-batch por `MessageGroupId`.
- **Resultados medidos**:
  - `total_calls`: **300 / 300** (100 % en `SUCCEEDED`)
  - `max_provider_inflight`: **1** (SC-001 / Invariante IV.1 ✅)
  - `global_max_inflight`: **6** (paralelismo real entre proveedores distintos ✅)
  - `duplicate_idem_keys`: **0** ante 5 % de duplicados inyectados (SC-002 ✅)

## 2. Graceful shutdown bajo carga (`003-T027`, SC-004)
- **Test**: `TestWorker_LocalStack_GracefulShutdown_ZeroLoss` (`internal/app/execute/worker_it_test.go`)
- **Escenario**: 60 mensajes en 6 proveedores procesados por 2 workers en LocalStack; `worker-1` recibe cancelación equivalente a SIGTERM a mitad de la carga (`TotalCalls >= 10`), termina el trabajo en vuelo y libera lo pendiente con `ChangeMessageVisibility(0)` mientras `worker-2` completa el resto.
- **Resultados medidos**:
  - `total_calls`: **60 / 60** (100 % `SUCCEEDED`, **0 mensajes perdidos** — SC-004 ✅)
  - `max_provider_inflight`: **1** ✅
  - `duplicate_idem_keys`: **0** ✅

## 3. Carga sostenida con 200 proveedores Zipf (`003-T030`, SC-001…SC-003)
- **Test**: `TestWorker_LoadBenchmark_Zipf200Providers` (`go test -tags=integration -race -v -run TestWorker_LoadBenchmark_Zipf200Providers ./internal/app/execute/...`)
- **Escenario**: 200 proveedores con distribución Zipf ($\alpha = 1.1$), 2.400 llamadas HTTP reales con firma AWS SigV4 (`httpapi.Client` + `mockapi` con latencia simulada de 5 ms), semáforo global de 64 slots, serialización estricta por proveedor.
- **Resultados medidos**:
  | Métrica | Valor medido | Objetivo SC | Estado |
  |---|---|---|---|
  | Throughput sostenido | **635,3 req/s** | ≥ 120 req/s (SC-003) | ✅ (5,3× margen) |
  | Overhead propio del worker p50 | **1,26 ms** | — | ✅ |
  | Overhead propio del worker p95 | **4,33 ms** | — | ✅ |
  | Overhead propio del worker p99 | **9,92 ms** | < 50 ms (SC-003) | ✅ (5× margen) |
  | `max_provider_inflight` | **1** | = 1 (SC-001) | ✅ |
  | `duplicate_idem_keys` | **0** | = 0 (SC-002) | ✅ |
