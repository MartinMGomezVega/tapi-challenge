# Worker Load, Overhead & Exclusivity Report (`003-worker-ejecucion`)

**Fecha**: 2026-11-23 · **Entorno local**: Apple M1 Pro (Docker Desktop + LocalStack 4.0.3, Go 1.26.11, `-race`)

> Este reporte separa estrictamente las **mediciones locales reales (MEDIDO)** de la **proyección de capacidad en AWS (PROYECTADO)**.

---

## Parte A — MEDIDO

### 1. Overhead puro del caso de uso (`BenchmarkExecuteUseCase_Overhead`)
- **Comando**: `go test -bench=BenchmarkExecuteUseCase_Overhead -benchmem -v -run='^$' ./internal/app/execute/...`
- **Alcance**: mide únicamente el overhead propio de `execute.UseCase` + firma AWS SigV4 (`httpapi.Client`) + `DomainRetryPolicy` + JSON response handling (store en memoria y acker no-op). **No prueba ni afirma exclusividad por proveedor.**
- **Resultados medidos** (`196.894` iteraciones):
  - Latencia propia p50: **0,019 ms**
  - Latencia propia p95: **0,256 ms**
  - Latencia propia p99: **0,657 ms** (objetivo SC-003 overhead: **< 50 ms** ✅ — 76× margen)
  - Throughput CPU puro del caso de uso: **176.022 req/s** (`5.681 ns/op`, `9.704 B/op`)

### 2. E2E Uniforme en LocalStack — 200 proveedores × 10 mensajes (`TestWorker_LocalStack_EndToEndThroughput_Uniform`)
- **Comando**: `go test -tags=integration -race -v -run TestWorker_LocalStack_EndToEndThroughput_Uniform ./internal/app/execute/...`
- **Infraestructura real**: LocalStack SQS FIFO (`DeduplicationScope=messageGroup`, `FifoThroughputLimit=perMessageGroupId`), DynamoDB (`requests` + `executions`), S3, **4 workers concurrentes** (`sqsq.Consumer`, 2 receivers × 16 inflight = 64 cupos globales) con `DomainRetryPolicy` + SigV4 + `httptest.NewServer(mockapi)` (`DefaultLatency = 2 ms`). **Ninguna serialización en el test.**
- **Carga**: **200 proveedores uniformes × 10 mensajes = 2.000 mensajes** (encolados de forma intercalada para exponer los 200 `MessageGroupId` en paralelo desde `t = 0`). Elimina la cola pesada Zipf para medir el throughput paralelo E2E real del stack en LocalStack.
- **Resultados medidos**:
  | Métrica | Valor medido | Objetivo SC | Estado |
  |---|---:|---|---|
  | Mensajes completados | **2.000 / 2.000** (`wall = 23,484 s`) | 100 % | ✅ |
  | `max_provider_inflight` (medido por `mockapi`) | **1** | = 1 (SC-001) | ✅ |
  | `global_max_inflight` (paralelismo real entre proveedores) | **9** | > 1 | ✅ |
  | `duplicate_idem_keys` (medido por `mockapi`) | **0** | = 0 (SC-002) | ✅ |
  | **Throughput E2E uniforme medido (LocalStack)** | **85,2 req/s** | ≥ 120 req/s (SC-003) | ⚠️ (< 120 req/s en LocalStack; ver Parte B) |

- **Lectura del techo local (`85,2 req/s`)**: sin sesgo Zipf, cada mensaje ejecuta 5 round-trips reales contra el contenedor único de LocalStack (SQS `ReceiveMessage` + DynamoDB `Acquire` + DynamoDB `GetItem` + HTTP SigV4 + DynamoDB `Complete` + SQS `DeleteMessage`), saturando el proceso Python/DynamoDB Local en laptop (~11,7 ms efectivos por mensaje a nivel agregado). Como el E2E uniforme medido no alcanza 120 req/s, **SC-003 se reporta como `⚠️ verificado por proyección`**.

### 3. E2E Zipf en LocalStack — 50 proveedores ($\alpha = 1,1$) (`TestWorker_LocalStack_EndToEndThroughput`)
- **Comando**: `go test -tags=integration -race -v -run '^TestWorker_LocalStack_EndToEndThroughput$' ./internal/app/execute/...`
- **Carga**: **50 proveedores Zipf** ($\alpha = 1,1$), **2.000 mensajes** totales (`zipf-00` recibe **522 mensajes**, 26,1 % del total), mismos 4 workers × 16 inflight.
- **Resultados medidos**:
  | Métrica | Valor medido | Cota / Objetivo | Estado |
  |---|---:|---|---|
  | Mensajes completados | **2.000 / 2.000** | 100 % | ✅ |
  | `top_provider_msgs` (`zipf-00`) | **522** | — | — |
  | `top_provider_avg_ms` (promedio real por msg en `zipf-00`) | **61,26 ms** | — | — |
  | `top_provider_serial_time` (cota teórica $\text{msgs}_{\text{top}} \times \overline{t}_{\text{top}}$) | **31,979 s** | — | — |
  | `wall` total | **32,344 s** | $\le 1{,}20 \times \text{top\_provider\_serial\_time}$ (`38,375 s`) | ✅ (**ratio `1,011`** = +1,1 %) |
  | Throughput agregado bajo Zipf extremo | **61,8 req/s** | Acotado por cadena serial de `zipf-00` | Demuestra invariante IV.1 |
  | `max_provider_inflight` (medido por `mockapi`) | **1** | = 1 (SC-001) | ✅ |
  | `global_max_inflight` | **5** | > 1 | ✅ |
  | `duplicate_idem_keys` | **0** | = 0 (SC-002) | ✅ |

- **Interpretación**: el `wall` (`32,344 s`) difiere apenas **+1,1 %** de la cadena estrictamente serial del proveedor #1 (`31,979 s`). Este test valida el **invariante de exclusividad (IV.1)** bajo sesgo extremo y demuestra que los otros 49 proveedores drenan en paralelo detrás de `zipf-00` sin serialización a nivel sistema; no mide capacidad paralela pico del clúster.

### 4. Exclusividad con 5 % duplicados (`003-T018/T020`), Graceful Shutdown (`003-T027`) y Heartbeat/Nack
- **`TestWorker_LocalStack_ExclusivityAndIdempotency`**: 20 proveedores, 300 mensajes + 15 duplicados inyectados con `MessageDeduplicationId` distinto (forzando capa 2 DynamoDB), 4 workers en LocalStack FIFO → `total_calls = 300/300`, `max_provider_inflight = 1`, `global_max_inflight = 6`, `duplicate_idem_keys = 0` (SC-001 ✅, SC-002 ✅).
- **`TestWorker_LocalStack_GracefulShutdown_ZeroLoss`**: 60 mensajes, 6 proveedores, 2 workers en LocalStack; `worker-1` recibe cancelación equivalente a SIGTERM bajo carga → `60/60 SUCCEEDED`, 0 perdidos, `max_provider_inflight = 1`, `duplicate_idem_keys = 0` (SC-004 ✅).
- **Concurrencia, aislamiento intra-batch y race de heartbeat (`sqsq.Consumer`)**:
  - `TestConsumer_SameGroupProcessedSequentially`, `TestConsumer_DeferredMessageAbortsRestOfGroupWithNack0`, `TestConsumer_HeartbeatDoesNotOverwriteNackBackoff`, `TestConsumer_ExtendBatchCountsPartialFailures` (`-count=50 -race`): **PASS**.
  - `TestConsumer_GroupHeartbeatExtendsWaitingMessages` (VT=2 s, 4 mensajes × 700 ms = 2,8 s, `-count=3 -race`): **0 expiraciones / 0 re-entregas**.

---

## Parte B — PROYECTADO (AWS Producción)

En AWS (ECS Fargate ARM64 + SQS FIFO high-throughput `3.000 msg/s` por partición + DynamoDB On-Demand + VPC Endpoints en la misma región), la capacidad sostenida sigue la ley de Little para `W` tasks con `I` ejecuciones concurrentes por task:

$$\text{Throughput}_{\text{AWS}} = \frac{W \times I}{t_{\text{api}} + t_{\text{aws\_io}} + t_{\text{worker}}}$$

Con los parámetros de diseño (`docs/architecture.md §5`):
- $W = 4$ tasks Fargate ARM64, $I = 32$ inflight/task $\Rightarrow W \times I = 128$ en vuelo globales (cumple $128 \le \text{LAMBDA\_BUDGET}/2 = 500$ con cuota de 1.000).
- $t_{\text{api}} = 400\text{ ms}$ (latencia p95 asumida de la API interna con 2 Lambdas encadenadas).
- $t_{\text{aws\_io}} = 20\text{ ms}$ (DynamoDB `Acquire` + `GetItem` + `Complete` + SQS `DeleteMessage` vía VPC Endpoints en la misma AZ/región).
- $t_{\text{worker}} = 0{,}657\text{ ms}$ (overhead p99 **medido** en `BenchmarkExecuteUseCase_Overhead`).

$$\text{Throughput}_{\text{AWS}} = \frac{4 \times 32}{0{,}400 + 0{,}020 + 0{,}000657} = \frac{128}{0{,}420657} \approx \mathbf{304{,}3\text{ req/s}}$$

- **Margen sobre objetivo 10M registros/día (120 req/s)**: **2,5×** (`304,3 req/s` vs `120 req/s`).
- **Precondición de paralelismo**: al menos 128 proveedores activos simultáneamente en el slot (y ningún proveedor individual excediendo su techo serial $k_{\max} = \lfloor 86{,}400 / t_{\text{api}} \rfloor$, monitoreado diariamente por `app/capacity` en `001/US3`).
- **Estado de `003/SC-003`**: **⚠️ verificado por proyección** (overhead p99 = `0,657 ms < 50 ms` medido; throughput ≥ 120 req/s proyectado en AWS dado que el E2E uniforme en LocalStack llega a `85,2 req/s`).
