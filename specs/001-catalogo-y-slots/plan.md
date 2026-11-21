# Implementation Plan: Catálogo de registros y asignación de slots

**Branch**: `001-catalogo-y-slots` · **Spec**: [spec.md](./spec.md)

## Summary
Tabla `requests` en DynamoDB con un GSI `gsi_slot_shard` y una política de dominio `SlotAssigner` pura y determinística. Un job `slotter` (ECS one-off task) hace el backfill reanudable. Un validador diario calcula la capacidad por proveedor.

## Technical Context
- **Lenguaje**: Go (última estable, ≥ 1.25), AWS SDK for Go v2
- **Storage**: DynamoDB on-demand, PITR, SSE-KMS
- **Testing**: `go test`, `testify` (o stdlib), testcontainers-go + LocalStack, property-based tests con `pgregory.net/rapid`
- **Target**: ECS Fargate ARM64 (job `slotter`)
- **Escala**: 1M → 100M de registros

## Constitution Check
| Principio | Cumple | Cómo |
|---|---|---|
| I Spec primero | ✅ | spec.md aprobado |
| II TDD | ✅ | `SlotAssigner` con tests de propiedades antes de implementar |
| III Clean Arch | ✅ | `SlotAssigner` en `domain`, sin dependencias externas |
| IV.3 Distribución | ✅ | FR-002 + SC-002 |
| V Escalabilidad | ✅ | GSIs `gsi_slot_shard` y `gsi_provider`, cero `Scan` incluso en backfill; memoria `O(k_max)` (streaming por proveedor), nunca `O(N)` |
| IX Simplicidad | ✅ | Función pura, sin servicios extra |

## Diseño

### Algoritmo de asignación (determinístico y uniforme por proveedor)
Necesitamos uniformidad **por proveedor**, porque cada proveedor se procesa en serie. Un `hash(id) mod 1440` simple produce colisiones por proveedor (varios registros del mismo proveedor en el mismo minuto, que tendrían que esperarse entre sí).

```
offset(provider) = fnv64a(provider) mod 1440               // desfasa a los proveedores entre sí
rank(id)         = posición estable del id dentro del proveedor
slot             = (offset + ⌊rank × 1440 / k⌋) mod 1440    // k = registros del proveedor
```
- **Backfill en streaming por proveedor (sin `Scan`, sin sort en memoria)**:
  1. La lista de proveedores se obtiene del registro de proveedores en `coordination` (un ítem por proveedor con `k`, `histogram[1440]` y `new_since_backfill`; compartido con US3).
  2. El backfill procesa proveedores en paralelo con `errgroup` (concurrencia acotada) y un rate limiter global de WCU (`golang.org/x/time/rate`).
  3. Por cada proveedor:
     - Si es backfill inicial o acumuló `> 1440` registros nuevos desde el último backfill, ejecuta **rebalanceo completo**: consulta `gsi_provider` (`PK=provider, SK=id, KEYS_ONLY`) primero con `Select: COUNT` para obtener `k` y luego en streaming paginado. Como DynamoDB devuelve los ítems ordenados por `SK=id`, `rank = posición` sin ordenar ni agrupar en memoria (**memoria acotada `O(k_max)` — en streaming `O(page_size) + O(1440)` — independiente de `N`**). Actualiza los slots y persiste el nuevo `k` + histograma en `coordination`.
     - Si acumuló `≤ 1440` registros nuevos, aplica **asignación incremental** (`AssignIncremental`) usando el histograma persistido en `coordination`, evitando reescribir los registros existentes.
  4. **Checkpoint reanudable**: `(proveedor completado, cursor, rank)` persistido en `coordination`. Ante una interrupción, los proveedores completados se omiten y los pendientes retoman desde su último cursor.
- **Altas incrementales** (sin recalcular todo): el slot nuevo es el del "hueco" más grande del proveedor. Se lee el histograma `provider_slot_counts` de `coordination` y se elige el minuto de menor ocupación, desempatando por hash. Es O(1440) en memoria.
- `shard = fnv32a(id) mod S`.

### Modelo de datos
```
Table requests
  PK  id (S)
  attrs provider, provider_id("prov#id"), endpoint, method, body, enabled(BOOL),
        slot(N), shard(N), slot_shard(S "0742#03", omitempty), version(N),
        created_at, updated_at
  GSI gsi_slot_shard  PK slot_shard  SK provider_id ("prov#id")
       projection: INCLUDE (provider, enabled, created_at)   (el worker lee endpoint/body con GetItem)
  GSI gsi_provider    PK provider    SK id
       projection: KEYS_ONLY                                 (backfill ordenado en streaming sin Scan ni sort en RAM)

Table coordination
  PK  name (S)
  - "provider#<provider>"           -> registro de proveedores: provider, k(N), histogram(L[1440]), new_since_backfill(N), updated_at (fuente para US2 y US3)
  - "backfill#<job>#<provider>"     -> checkpoint: status(PENDING|DONE), cursor(S), rank(N), updated_at
```
- **Proyección `INCLUDE` en `gsi_slot_shard`**: en la API de DynamoDB, incluir `provider`, `enabled` y `created_at` sobre las claves del índice reduce el costo frente a `ALL`, garantiza que el worker siempre lea la versión más reciente de `endpoint`/`body` vía `GetItem`, y permite filtrar `enabled=true` (001/002) y `created_at` en el reconciler (005) sin `GetItem` adicionales.
- **GSI `gsi_provider` (`KEYS_ONLY`)**: elimina el `Scan` completo y el agrupamiento/ordenamiento `O(N)` en memoria (que consumiría ~16 GB con 100M registros). DynamoDB entrega los `id` ordenados por `SK` para cada `provider`, por lo que `rank` es la posición en el stream.
- **GSI sparse**: los registros sin slot asignado (`slot < 0`) omiten `slot_shard`, por lo que DynamoDB los excluye automáticamente de `gsi_slot_shard` hasta que `SetSlot` los indexa. `provider_id` se persiste al alta para evitar recomputar concatenaciones en `UpdateItem`.
- **Validaciones e invariantes de clave/PII**: `provider` prohíbe `#` (evita ambigüedad en la SK `provider#id`); `Request` implementa `slog.LogValuer` y `fmt.Stringer` omitiendo `body` (sólo expone `body_bytes`) para impedir fugas de PII en logs.
- **Paginación en `ListBySlotShard`**: el cursor `page` codifica `LastEvaluatedKey` como token opaco JSON en base64 URL-safe y avanza automáticamente páginas vacías resultantes del `FilterExpression` (`enabled = true`).
- **Registro de proveedores e histograma en `coordination`**: cada proveedor tiene un ítem con `k`, el histograma de 1440 minutos y el contador de altas desde el último backfill. Sirve como fuente de proveedores para US2 (backfill + altas incrementales) y US3 (validación de capacidad).

### Estructura
```
internal/domain/request.go          // entidad + validaciones (FR-007)
internal/domain/slot.go             // SlotAssigner (puro)
internal/app/assignslots/           // caso de uso backfill + incremental
internal/app/capacity/              // caso de uso validación de capacidad
internal/adapters/dynamo/requests.go
cmd/slotter/main.go
```

### Puertos
```go
type RequestReader interface {
    ListBySlotShard(ctx context.Context, slot, shard int, page string) ([]RequestKey, string, error)
}
type ProviderIndexReader interface {
    CountByProvider(ctx context.Context, provider string) (int, error)
    ListIDsByProvider(ctx context.Context, provider string, page string) ([]string, string, error)
}
type SlotWriter interface {
    SetSlot(ctx context.Context, id string, slot, shard int, expectedVersion int64) error
}
type ProviderRegistry interface {
    ListProviders(ctx context.Context) ([]ProviderState, error)
    SaveProviderState(ctx context.Context, state ProviderState) error
}
type Checkpointer interface {
    Load(ctx context.Context, job, provider string) (BackfillCheckpoint, error)
    Save(ctx context.Context, job, provider string, cp BackfillCheckpoint) error
}
```

## Estrategia de test
- **Propiedades** (rapid): determinismo, rango válido, uniformidad global y por proveedor para k aleatorio en [1, 500k] (los tests de 1M / 500k corren en `make test-full` y se omiten con `-short` en `make test`).
- **Unit**: validaciones de `Request` y redacción de PII; umbral `> 1440` rebalanceo vs incremental; idempotencia y reanudación por `(proveedor, cursor)`.
- **Integración** (LocalStack): `gsi_slot_shard` devuelve ítems habilitados paginados; `gsi_provider` devuelve IDs ordenados por SK sin `Scan`; `SetSlot` condicional por `version`; backfill interrumpido y reanudado con persistencia de registro/histograma en `coordination`.
- **Bench & Memoria**: `BenchmarkAssign1M` (ruta caliente pura), `BenchmarkStreamingBackfill1M` / `BenchmarkStreamingBackfill4M` y test de invarianza de memoria (`1M` vs `4M` con `k_max` fijo → heap similar, probando memoria `O(k_max)` y no `O(N)`).

## Riesgos
| Riesgo | Mitigación |
|---|---|
| Hot partition en el GSI con 100M | S = 32 shards en `gsi_slot_shard`, queries en paralelo |
| Memoria O(N) agrupando 100M en RAM (~16 GB) | Eliminado con `gsi_provider` (`PK=provider, SK=id, KEYS_ONLY`): streaming ordenado por SK, memoria acotada `O(k_max)` / `O(page_size)` |
| Backfill consume WCU de producción | Rate limiter configurable + on-demand + rebalanceo completo sólo si nuevos > 1440 |
| Proveedor sobre capacidad (S5) | Validación US3 leyendo `k` del registro en `coordination` + alarma + pregunta de negocio |

## Complexity Tracking
Ninguna violación.
