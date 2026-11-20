# Feature Specification: Observabilidad y reconciliación diaria

**Feature Branch**: `005-observabilidad-y-reconciliacion`
**Creado**: 2026-11-20 · **Estado**: Draft
**Trazabilidad**: R1 (garantizar "cada registro, cada día"), R4 (visibilidad de los fallos), C1 (operar a escala)
**Depende de**: 001–004

## Contexto
El enunciado exige una consulta diaria **para cada registro**. Con 1M+ de registros no alcanza con confiar en el flujo feliz: hace falta **probar** cada día que el 100 % se ejecutó, y detectar y corregir los faltantes.

## User Scenarios & Testing

### User Story 1 — Reconciliación diaria (P1) 🎯
**Test independiente**: un día simulado con 10k registros donde se borran 50 ejecuciones y se dejan 20 en `ENQUEUED`; el reconciler las detecta y re-despacha.

**Acceptance Scenarios**:
1. **Given** el día `D` terminado (00:30 de `D+1`), **When** corre el reconciler, **Then** compara los registros habilitados con `executions(run_date=D)` y produce `coverage = terminales / esperados`.
2. **Given** registros sin ejecución o en estado no terminal con más de 2 h de antigüedad, **When** se detectan, **Then** se re-despachan (si `RECONCILE_REDISPATCH=true`) con el `run_date=D` original.
3. **Given** el reconciler termina, **When** reporta, **Then** escribe `s3://…/reports/run_date=D/summary.json` (totales por estado y por proveedor, top errores) y publica las métricas `DailyCoverage` y `MissingExecutions`.
4. **Given** `coverage < 99,9 %` después de los re-despachos, **When** se evalúa, **Then** se dispara la alarma P1.

### User Story 2 — Métricas, logs y trazas (P1)
**Acceptance Scenarios**:
1. Todo log es JSON con `request_id`, `provider`, `run_date`, `trace_id`, `status`, `attempt`.
2. Una traza conecta dispatcher → SQS → worker → API (propagación de `traceparent` en los atributos del mensaje).
3. Métricas EMF: `ExecutionsByStatus`, `ProviderLatency` (p50/p95/p99), `QueueAge`, `DispatchLagMinutes`, `InflightCalls`, `BreakerOpen`, `DailyCoverage`.

### User Story 3 — Alarmas con runbook (P1)
| Alarma | Condición | Severidad |
|---|---|---|
| DispatchLag | `DispatchLagMinutes > 5` por 5 min | P1 |
| QueueAgeHigh | `ApproximateAgeOfOldestMessage > 30 min` | P2 |
| DLQNotEmpty | DLQ visible > 0 | P2 |
| CoverageLow | `DailyCoverage < 99,9 %` | P1 |
| AuthErrors | `ExecutionFailed{class=AUTH} > 0` | P1 |
| LambdaQuotaPressure | `ConcurrentExecutions` de la cuenta > 80 % de la cuota | P1 |
| ProviderOverCapacity | feature 001 US3 | P2 |

### User Story 4 — Dashboard operativo (P2)
Dashboard de CloudWatch: throughput, cobertura del día en curso (proyectada), latencia por proveedor, errores por clase y estado de los breakers.

### Edge Cases
- Un registro dado de alta durante el día `D` después de su slot → no se espera ejecución en `D` (se excluye por `created_at`).
- Un registro deshabilitado durante `D` → `SKIPPED` cuenta como terminal.
- Con 100M de registros el reconciler debe terminar en < 2 h: se recorre por slot y shard en paralelo (sin Scan completo) con `BatchGetItem` sobre `executions`.

## Requirements
- **FR-001**: El reconciler es un task programado (EventBridge Scheduler → ECS RunTask) a las 00:30 en `RUN_DAY_TZ`.
- **FR-002**: Recorre el GSI `slot_shard` (1440 × S queries en paralelo, con límite) y hace `BatchGetItem` de las ejecuciones.
- **FR-003**: Re-despacho idempotente (mismo DedupId; `CreatePending` condicional o transición `stale → ENQUEUED`).
- **FR-004**: Reporte diario en S3 (JSON + CSV), consultable con Athena.
- **FR-005**: Logging con `slog` y handler JSON; OTel SDK con exporter a X-Ray (ADOT sidecar).
- **FR-006**: Cada alarma tiene un runbook en `docs/runbooks/`.

## Success Criteria
- **SC-001**: `DailyCoverage ≥ 99,9 %` en la simulación con fault injection; 100 % después del re-despacho.
- **SC-002**: el reconciler procesa 1M en < 10 min y 100M en < 2 h (proyección por benchmark).
- **SC-003**: cualquier ejecución se rastrea de punta a punta con un `trace_id`.
