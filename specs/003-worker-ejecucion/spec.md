# Feature Specification: Worker — ejecución concurrente, exclusividad por proveedor y persistencia de resultados

**Feature Branch**: `003-worker-ejecucion`
**Creado**: 2026-11-20 · **Estado**: Draft
**Trazabilidad**: R3 (guardar resultado), R5 (sin concurrencia por proveedor), C2 (misma cuenta y región), C3 (API con dos lambdas)
**Depende de**: 001, 002

## Contexto
Los workers consumen la cola FIFO, llaman a la API interna y persisten el resultado. Deben maximizar el paralelismo **entre** proveedores, garantizar **cero** concurrencia **dentro** de un proveedor y no saturar la cuota de concurrencia de Lambda de la cuenta, que comparten con las dos lambdas de la API.

## User Scenarios & Testing

### User Story 1 — Ejecutar y guardar el resultado (P1) 🎯
**Test independiente**: encolar 1.000 mensajes contra un mock de la API; verificar 1.000 `executions` en `SUCCEEDED` con su resultado.

**Acceptance Scenarios**:
1. **Given** un mensaje `ENQUEUED`, **When** el worker lo toma, **Then** pasa a `IN_PROGRESS` con un lease (`lease_owner`, `lease_expires_at`) mediante una escritura condicional.
2. **Given** la API responde 2xx, **When** termina, **Then** se guarda `SUCCEEDED` con `http_status`, `response_body` (o `response_s3_key` si supera 350 KB), `latency_ms`, `attempts`, `completed_at`, y se borra el mensaje.
3. **Given** el worker lee el registro, **When** llama a la API, **Then** usa `endpoint`, `method` y `body` actuales del catálogo, el header `Idempotency-Key: request_id#run_date` y firma SigV4.

### User Story 2 — Exclusividad por proveedor (P1) 🎯
**Test independiente**: 20 proveedores × 500 mensajes, 4 workers × 16 goroutines; el mock de la API mide las llamadas en vuelo por proveedor.

**Acceptance Scenarios**:
1. **Given** varios mensajes del mismo proveedor, **When** hay múltiples workers y goroutines, **Then** el máximo de llamadas simultáneas por proveedor medido es **1**.
2. **Given** proveedores distintos, **When** hay capacidad, **Then** se procesan en paralelo (throughput ≥ 0,8 × proveedores activos / latencia).
3. **Given** un mensaje re-entregado por vencimiento del visibility timeout mientras el original sigue en vuelo, **When** el segundo worker intenta tomarlo, **Then** el lease vigente lo rechaza y el worker extiende la visibilidad sin llamar a la API.

### User Story 3 — Idempotencia (P1)
**Acceptance Scenarios**:
1. **Given** una ejecución `SUCCEEDED`, **When** llega un duplicado, **Then** se borra el mensaje sin llamar a la API.
2. **Given** una ejecución `FAILED_PERMANENT`, **When** llega un duplicado, **Then** igual que el escenario anterior.
3. **Given** un crash después de la llamada y antes de persistir, **When** se re-entrega el mensaje, **Then** se vuelve a llamar con el mismo `Idempotency-Key` (riesgo residual documentado en ADR-005).

### User Story 4 — Presupuesto global de concurrencia (P1)
Como plataforma en la misma cuenta que la API interna (2 lambdas por llamada), limito las llamadas en vuelo para no agotar la cuota regional de Lambda.

**Acceptance Scenarios**:
1. **Given** `INFLIGHT_PER_TASK=k` y `MAX_TASKS=t`, **When** arranca el worker, **Then** valida `k × t ≤ LAMBDA_BUDGET/2` o falla el arranque (fail-fast).
2. **Given** el semáforo de k ocupado, **When** llegan más mensajes, **Then** no se reciben más hasta liberar lugar (no hay mensajes retenidos que consuman visibility timeout).

### User Story 5 — Graceful shutdown (P2)
**Given** SIGTERM (deploy o scale-in), **When** hay llamadas en curso, **Then** se dejan de recibir mensajes, se terminan las llamadas en curso hasta en `stopTimeout - 5 s`, y lo no terminado se libera con `ChangeMessageVisibility(0)`.

### Edge Cases
- Un registro borrado del catálogo entre el despacho y la ejecución → `FAILED_PERMANENT` con la razón `REQUEST_NOT_FOUND`.
- Un registro deshabilitado después del despacho → `SKIPPED`, que es un estado terminal.
- Una respuesta enorme → S3 (`s3://bucket/results/run_date=D/provider=P/id.json`) + puntero.
- Una llamada que tarda más que el visibility timeout → *heartbeat* que extiende la visibilidad cada `VT/2`.
- Cold start de las dos lambdas → timeout por llamada de 25 s (API Gateway corta en 29 s).

## Requirements
- **FR-001**: Consumo con long polling (20 s) en `R` goroutines receptoras y procesamiento acotado por un semáforo de tamaño `k`.
- **FR-002**: Transiciones condicionales: `ENQUEUED|RETRYING → IN_PROGRESS` (o un lease vencido) → terminal.
- **FR-003**: Llamada HTTP con `context.WithTimeout`, cliente con keep-alive, SigV4, `Idempotency-Key` y `traceparent`.
- **FR-004**: Persistir el resultado según US1.2. TTL configurable (por defecto 90 días).
- **FR-005**: Heartbeat de visibilidad para las llamadas largas.
- **FR-006**: Rate limiter por proveedor configurable (`PROVIDER_RPS`, opcional).
- **FR-007**: Delegar la clasificación de errores y los reintentos en la feature 004 (puerto `RetryPolicy`).
- **FR-008**: Autoscaling por `ApproximateAgeOfOldestMessage` y backlog por task.

## Key Entities
- **Execution** (completa): `request_id` (PK), `run_date` (SK), `provider`, `status`, `attempts`, `lease_owner`, `lease_expires_at`, `http_status`, `response_body | response_s3_key`, `latency_ms`, `error_class`, `error_message`, `created_at`, `completed_at`, `ttl`. GSI `gsi_run_date_status` (PK `run_date#status`, SK `provider#request_id`) para la reconciliación y los reportes.

## Success Criteria
- **SC-001**: máximo de concurrencia medida por proveedor = 1 en todos los tests de carga.
- **SC-002**: 0 llamadas duplicadas efectivas en un test con 5 % de mensajes duplicados inyectados.
- **SC-003**: sostiene 120 req/s (escenario de 10M) con p99 de overhead propio < 50 ms.
- **SC-004**: 0 mensajes perdidos ante un SIGTERM durante la carga.
