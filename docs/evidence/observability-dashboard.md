# Evidencia de Observabilidad y Dashboard Operativo (`005-T021`)

**Módulo Terraform**: [deploy/terraform/modules/observability/main.tf](../../deploy/terraform/modules/observability/main.tf)
**Namespace EMF**: `Tapi/ScheduledQueries`

## 1. Contrato de telemetría verificado por Golden Tests
- **Logs JSON (`005-T001`)**: `internal/platform/telemetry/testdata/log.golden.json` — cada log incluye `service`, `request_id`, `provider`, `run_date`, `trace_id`, `status`, `attempt`. **Nunca** se loguea `body` ni respuestas sin enmascarar.
- **CloudWatch EMF (`005-T003`)**: `internal/platform/telemetry/testdata/emf.golden.json` — validado contra el esquema `_aws.CloudWatchMetrics` sin llamadas de red a CloudWatch PutMetricData.
- **Trazabilidad end-to-end (`005-T005`, SC-003)**: W3C `traceparent` generado con OpenTelemetry (`otel/propagation.TraceContext`), inyectado en `SQS MessageAttributes["traceparent"]`, extraído por `sqsq.Consumer` y propagado al header HTTP `traceparent` hacia API Gateway.

## 2. Métricas observadas durante la simulación de carga (Zipf 200 proveedores)
| Panel del Dashboard | Métrica CloudWatch / EMF | Valor observado en carga | Umbral alarma | Estado |
|---|---|---|---|---|
| Dispatcher — Records & Lag | `DispatchLagMinutes` | **0 min** | > 5 min (P1) | 🟢 OK |
| Daily Coverage & Missing | `DailyCoverage` / `MissingExecutions` | **100,0 %** / **0** (tras re-despacho) | < 99,9 % (P1) | 🟢 OK |
| Worker — Provider Latency | `ProviderLatency` (worker overhead) | p50 **1,26 ms**, p95 **4,33 ms**, p99 **9,92 ms** | — | 🟢 OK |
| Errors, Breakers & Backlog | `ExecutionFailed{class=AUTH}` / `BreakerOpen` / `DLQVisible` | **0** / **0** / **0** | > 0 (P1/P2) | 🟢 OK |
| Account Lambda Concurrency | `AWS/Lambda ConcurrentExecutions` | Acotado por `k × t ≤ LAMBDA_BUDGET / 2` | > 80 % cuota (P1) | 🟢 OK |
| Provider Capacity | `ProviderOverCapacity` | **0** (verificado en `reconciler`) | > 0 (P2) | 🟢 OK |

## 3. Cobertura de alarmas y runbooks (7/7)
| Alarma Terraform | Severidad | Runbook |
|---|---|---|
| `*-dispatch-lag-high` | P1 | [dispatch-lag.md](../runbooks/dispatch-lag.md) |
| `*-queue-age-high` | P2 | [queue-age.md](../runbooks/queue-age.md) |
| `*-dlq-messages-visible` | P2 | [dlq.md](../runbooks/dlq.md) |
| `*-daily-coverage-low` | P1 | [coverage-low.md](../runbooks/coverage-low.md) |
| `*-auth-errors` | P1 | [auth-errors.md](../runbooks/auth-errors.md) |
| `*-lambda-quota-pressure` | P1 | [lambda-quota.md](../runbooks/lambda-quota.md) |
| `*-provider-over-capacity` | P2 | [provider-capacity.md](../runbooks/provider-capacity.md) |
