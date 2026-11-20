# Feature Specification: Manejo de errores y reintentos

**Feature Branch**: `004-errores-y-reintentos`
**Creado**: 2026-11-20 · **Estado**: Draft
**Trazabilidad**: R4 (errores reintentables y no reintentables), R5 (no martillar a un proveedor), C3 (throttling de las 2 lambdas)
**Depende de**: 003

## Contexto
Algunos errores son transitorios (timeouts, 5xx, throttling de las lambdas de la API) y deben reintentarse con backoff. Otros son definitivos (validación, autorización, recurso inexistente) y reintentarlos sólo desperdicia cuota y oculta el problema. Ningún error puede perderse en silencio.

## User Scenarios & Testing

### User Story 1 — Clasificación de errores (P1) 🎯
**Acceptance Scenarios** (tabla de verdad, configurable):

| Señal | Clase | Reintenta |
|---|---|---|
| Timeout de red / `context.DeadlineExceeded` / conexión reseteada | `TRANSIENT_NETWORK` | ✅ |
| HTTP 500, 502, 503, 504 | `TRANSIENT_SERVER` | ✅ |
| HTTP 429 / `TooManyRequestsException` / Lambda throttling | `THROTTLED` | ✅ (respeta `Retry-After`) |
| HTTP 408 | `TRANSIENT_NETWORK` | ✅ |
| HTTP 400, 422 | `INVALID_REQUEST` | ❌ |
| HTTP 401, 403 | `AUTH` | ❌ (+ alerta P1: es un problema de configuración) |
| HTTP 404 | `NOT_FOUND` | ❌ |
| HTTP 409 con `Idempotency-Key` repetida | `ALREADY_PROCESSED` | ❌ → se trata como éxito |
| Respuesta 2xx con body inválido según el contrato | `INVALID_RESPONSE` | ✅ hasta 1 vez, después ❌ |
| Error desconocido | `UNKNOWN` | ✅ (conservador, con límite) |

### User Story 2 — Backoff sin bloquear a otros proveedores (P1) 🎯
**Acceptance Scenarios**:
1. **Given** un error reintentable y `attempts < 2` dentro del mismo procesamiento, **When** el error es rápido (< 1 s), **Then** se reintenta en proceso con backoff de 200 ms–1 s con jitter.
2. **Given** se agotaron los reintentos en proceso, **When** se decide reintentar, **Then** `execution → RETRYING`, `attempts++`, y `ChangeMessageVisibility(delay)` con `delay = min(15 min, 30 s × 2^(n-1)) ± jitter 20 %`.
3. **Given** el proveedor P en backoff, **When** hay mensajes de otros proveedores, **Then** se procesan normalmente (sólo se bloquea el grupo de P).
4. **Given** `ApproximateReceiveCount > 6`, **When** vuelve a fallar, **Then** el mensaje va a la DLQ y `execution → FAILED_EXHAUSTED`.

### User Story 3 — Error no reintentable (P1)
**Given** un error no reintentable, **When** ocurre, **Then** `execution → FAILED_PERMANENT` con `error_class` y `error_message` (sanitizado), se borra el mensaje y se emite la métrica `ExecutionFailed{provider, class}`.

### User Story 4 — Circuit breaker por proveedor (P2)
**Acceptance Scenarios**:
1. **Given** ≥ 50 % de errores reintentables en los últimos 20 intentos de P, **When** se evalúa, **Then** el breaker de P se abre por 60 s.
2. **Given** el breaker de P abierto, **When** llega un mensaje de P, **Then** no se llama a la API; se hace `ChangeMessageVisibility(cooldown)` y **no cuenta** como intento.
3. **Given** half-open, **When** 1 llamada de prueba tiene éxito, **Then** se cierra.

### User Story 5 — Operación de la DLQ (P2)
**Given** mensajes en la DLQ, **When** el operador corre `redrive --provider P --since D`, **Then** se re-encolan y la ejecución vuelve a `ENQUEUED`, conservando el `attempts` histórico.

### Edge Cases
- `Retry-After` > 15 min → se acota a 15 min y se registra.
- El circuit breaker vive en memoria por task (no es global). Se acepta: con pocos tasks converge rápido y evita un componente compartido (YAGNI). Se documenta.
- Reintentos que cruzan la medianoche → el `run_date` original no cambia; la reconciliación lo cuenta en el día que corresponde.
- Un error de DynamoDB o S3 al persistir no es un error del proveedor: se reintenta con el SDK y, si persiste, `Nack` sin incrementar `attempts` del proveedor.

## Requirements
- **FR-001**: `Classifier` puro: `(response, error) → ErrorClass`, con la tabla de US1 configurable.
- **FR-002**: `RetryPolicy` pura: `(class, attempts, retryAfter) → Decision{Complete|RetryInProcess|RetryLater(delay)|FailPermanent|Exhausted}`.
- **FR-003**: Backoff exponencial con *full jitter*, acotado.
- **FR-004**: DLQ FIFO con `maxReceiveCount = 6`; alarma si `ApproximateNumberOfMessagesVisible > 0`.
- **FR-005**: Circuit breaker por proveedor (US4).
- **FR-006**: Mensajes de error sanitizados (sin PII ni tokens) y truncados a 1 KB.
- **FR-007**: CLI `redrive` para la DLQ con filtros.

## Success Criteria
- **SC-001**: el 100 % de los casos de la tabla de US1 está cubierto por tests table-driven.
- **SC-002**: con un proveedor con 100 % de 503 durante 30 min, los demás proveedores mantienen su throughput (± 5 %).
- **SC-003**: 0 ejecuciones en estado no terminal 24 h después de su `run_date` (verificado por la feature 005).
- **SC-004**: un error no reintentable genera 0 reintentos.
