# Feature Specification: Dispatcher (despacho por minuto)

**Feature Branch**: `002-dispatcher`
**Creado**: 2026-11-20 · **Estado**: Draft
**Trazabilidad**: R1 (automática, diaria), R2 (distribuida en el día), R5 (agrupa por proveedor), C1 (escala)
**Depende de**: 001

## Contexto
Cada minuto hay que tomar los registros del slot actual y encolarlos para su ejecución, una sola vez por día, sin perder minutos aunque el servicio se caiga o se redeploye.

## User Scenarios & Testing

### User Story 1 — Despacho automático del slot actual (P1) 🎯
Como plataforma, cada minuto encolo exactamente una vez los registros habilitados de ese minuto.

**Test independiente**: con un reloj falso, avanzar 1440 minutos sobre 100k registros y verificar que cada uno fue encolado exactamente una vez para ese `run_date`.

**Acceptance Scenarios**:
1. **Given** el minuto `m` del día `D`, **When** el ticker dispara, **Then** se crea `execution(id, D)=PENDING` y se envía un mensaje FIFO con `MessageGroupId=provider` y `MessageDeduplicationId=id#D` para cada registro habilitado con `slot=m`.
2. **Given** un registro ya despachado para `D`, **When** se re-despacha el slot, **Then** no se crea un segundo mensaje efectivo (el `PutItem` condicional falla y se omite).
3. **Given** un registro `enabled=false`, **When** se despacha su slot, **Then** se omite.

### User Story 2 — Catch-up de minutos perdidos (P1)
Como plataforma, si el dispatcher estuvo caído X minutos, al volver despacho todos los slots pendientes del día.

**Acceptance Scenarios**:
1. **Given** `watermark = (D, 600)` y ahora es `(D, 615)`, **When** dispara el ticker, **Then** se despachan los slots 601–615 en orden y el watermark queda en 615.
2. **Given** una caída que cruza la medianoche, **When** se recupera, **Then** se completan los slots restantes de `D` con `run_date=D` y luego los de `D+1`.
3. **Given** un crash a mitad de un slot, **When** se reanuda, **Then** se re-procesa ese slot completo sin duplicar (idempotencia).

### User Story 3 — Alta disponibilidad con un solo líder (P1)
Como operador, corro 2 réplicas en distintas AZ y sólo una despacha.

**Acceptance Scenarios**:
1. **Given** 2 réplicas, **When** ambas arrancan, **Then** sólo una obtiene el lease de líder (TTL 30 s, renovación cada 10 s).
2. **Given** el líder muere, **When** vence el lease, **Then** la otra réplica toma el liderazgo en ≤ 40 s y hace catch-up.
3. **Given** un líder que perdió el lease (pausa de GC o partición de red), **When** intenta avanzar el watermark, **Then** la escritura falla por el *fencing token* (versión del lease).

### User Story 4 — Recuperación de PENDING huérfanos (P2)
**Given** un `execution=PENDING` con más de 5 min de antigüedad (crash entre el Put y el Send), **When** corre el barrido del slot, **Then** se re-envía a SQS.

### Edge Cases
- Un slot con 70k registros (escenario de 100M): se despacha en < 60 s con queries paralelas por shard y `SendMessageBatch` de a 10.
- Throttling de SQS o DynamoDB: reintento con backoff; el watermark no avanza hasta completar el slot.
- Cambio de hora o timezone: el `run_date` se calcula en `RUN_DAY_TZ` (UTC por defecto, sin DST).
- Deploy: SIGTERM → termina el slot en curso o lo deja sin avanzar el watermark → libera el lease.

## Requirements
- **FR-001**: Despachar cada slot una vez por minuto, desde el watermark hasta el minuto actual.
- **FR-002**: Crear `execution` PENDING con `PutItem` condicional (`attribute_not_exists`) antes de enviar a SQS.
- **FR-003**: Mensaje SQS: `{request_id, run_date, provider, slot, dispatched_at, trace_context}`, `MessageGroupId=provider`, `MessageDeduplicationId=request_id#run_date`.
- **FR-004**: Transicionar `PENDING → ENQUEUED` después del envío exitoso.
- **FR-005**: Elección de líder por lease en la tabla `coordination`, con fencing token.
- **FR-006**: Avanzar el watermark sólo cuando el slot está completo en todos sus shards.
- **FR-007**: Métricas: `SlotDispatchDuration`, `RecordsDispatched`, `DispatchLagMinutes`, `IsLeader`.
- **FR-008**: Alarma si `DispatchLagMinutes > 5`.

## Key Entities
- **Execution** (creada acá): `request_id`, `run_date`, `status`, `provider`, `slot`, `created_at`, `ttl`.
- **Lease**: `name`, `owner`, `expires_at`, `version` (fencing).
- **Watermark**: `run_date`, `last_slot`, `version`.

## Success Criteria
- **SC-001**: en una simulación acelerada de 1 día, el 100 % de los registros se despachan exactamente una vez.
- **SC-002**: el despacho de un slot con 1M de registros totales dura p99 < 5 s.
- **SC-003**: el failover de líder ocurre en ≤ 40 s sin pérdida ni duplicados efectivos.
- **SC-004**: `DispatchLagMinutes` = 0 en estado estable.
