# Feature Specification: Catálogo de registros y asignación de slots

**Feature Branch**: `001-catalogo-y-slots`
**Creado**: 2026-11-20
**Estado**: Draft
**Trazabilidad al enunciado**: R1 (diaria, cada registro), R2 (distribuida en el día), C1 (1M → N×)

## Contexto
El enunciado parte de una tabla con N registros (`id, proveedor, endpoint, body`), cada uno una consulta a una API interna. Para repartir las consultas a lo largo del día sin escanear la tabla, cada registro necesita saber **en qué minuto del día** le toca ejecutarse.

## User Scenarios & Testing *(obligatorio)*

### User Story 1 — Cada registro tiene un minuto asignado (Prioridad: P1)
Como plataforma, necesito que cada registro tenga un `slot` (0–1439) estable, para despacharlo una vez por día en su minuto.

**Por qué P1**: sin slots no hay distribución temporal (R2). Bloquea las features 002 y 005.
**Test independiente**: cargar 1M de registros sintéticos con 50 proveedores y verificar que cada uno tiene un slot válido y que la distribución es uniforme.

**Acceptance Scenarios**:
1. **Given** un registro nuevo sin slot, **When** se da de alta, **Then** se le asigna un `slot ∈ [0,1439]` y un `shard ∈ [0,S)`.
2. **Given** el mismo `(provider, id)`, **When** se calcula el slot dos veces, **Then** el resultado es idéntico (determinismo).
3. **Given** 1M de registros, **When** se asignan los slots, **Then** ningún minuto tiene más de `1,10 ×` el promedio (`N/1440`).
4. **Given** un proveedor con k registros (k ≥ 1440), **When** se asignan sus slots, **Then** ningún minuto tiene más de `⌈k/1440⌉ + 1` registros de ese proveedor.

### User Story 2 — Backfill del catálogo existente (Prioridad: P1)
Como operador, necesito asignar slots al millón de registros existentes sin downtime y de forma reanudable.

**Test independiente**: correr el backfill sobre 1M de registros en LocalStack, cortarlo a la mitad y reanudarlo. Verificar que termina sin duplicados ni registros sin slot.

**Acceptance Scenarios**:
1. **Given** registros sin slot, **When** corre el job de backfill, **Then** todos quedan con slot, y en un segundo run no cambia nada (idempotente).
2. **Given** el job interrumpido, **When** se reanuda, **Then** continúa desde el último checkpoint.

### User Story 3 — Validación de capacidad por proveedor (Prioridad: P2)
Como equipo, necesito saber si algún proveedor no puede completar su volumen diario respetando la exclusividad (procesamiento en serie).

**Acceptance Scenarios**:
1. **Given** un proveedor con `k × p95_latency > 86.400 s × 0,7`, **When** corre la validación, **Then** se emite la métrica `ProviderOverCapacity{provider}` y una alarma.

### Edge Cases
- Un proveedor con menos registros que 1440: se reparten en minutos equiespaciados, no en un solo minuto.
- Un registro dado de alta después de que pasó su slot del día: se ejecuta en la siguiente ocurrencia de su slot.
- Un registro deshabilitado (`enabled=false`): conserva el slot pero el dispatcher lo omite.
- Cambio de `provider` de un registro: se recalcula el slot.
- Crecimiento de N: aumentar S (shards) requiere re-sharding online. El slot no cambia, sólo el shard.

## Requirements *(obligatorio)*

### Functional Requirements
- **FR-001**: El sistema DEBE asignar a cada registro un `slot ∈ [0,1440)` determinístico.
- **FR-002**: La asignación DEBE ser uniforme a nivel global y por proveedor (criterios de US1).
- **FR-003**: El sistema DEBE asignar `shard = hash(id) mod S`, con S configurable.
- **FR-004**: El sistema DEBE exponer la consulta "registros habilitados del slot X, shard Y" sin `Scan` (GSI `gsi_slot_shard`).
- **FR-004b**: El sistema DEBE exponer la consulta ordenada de registros por proveedor sin `Scan` (GSI `gsi_provider`: `PK provider`, `SK id`, `KEYS_ONLY`), permitiendo calcular `rank` en streaming con memoria acotada a `O(k_max)` (independiente de `N`).
- **FR-005**: El backfill DEBE ser idempotente, reanudable por checkpoint `(proveedor, cursor)`, paralelizado por proveedor con rate limit configurable de WCU, y persistir el registro de proveedores (`k` e histograma) en `coordination`. Si un proveedor acumula `> 1440` registros nuevos desde el último backfill, se rebalancea completo; con `≤ 1440`, se aplica asignación incremental por histograma.
- **FR-006**: El sistema DEBE validar diariamente la capacidad por proveedor (US3) a partir del registro de proveedores en `coordination`.
- **FR-007**: Los registros DEBEN validarse en el alta: `endpoint` relativo a una allowlist y `body` como JSON válido de ≤ 256 KB.

### Key Entities
- **Request**: `id`, `provider`, `endpoint`, `method`, `body`, `enabled`, `slot`, `shard`, `slot_shard` (= `"%04d#%02d"`), `version`, `created_at`, `updated_at`.
- **SlotAssigner**: política de dominio pura que calcula `(slot, shard)`.

## Success Criteria *(obligatorio)*
- **SC-001**: el 100 % de los registros habilitados tiene un slot válido.
- **SC-002**: el desvío máximo por minuto es ≤ 10 % sobre el promedio con 1M de registros.
- **SC-003**: el backfill de 1M de registros termina en < 30 min con 4 workers.
- **SC-004**: la query de un slot completo (todos los shards) devuelve en p99 < 2 s con 1M y < 10 s con 100M.

## Supuestos
- S1 (docs/architecture.md): el catálogo vive o se replica en DynamoDB.
- El `method` HTTP por defecto es `POST` si el registro no lo especifica.
