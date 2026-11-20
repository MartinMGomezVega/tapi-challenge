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
| V Escalabilidad | ✅ | GSI shardeado, sin `Scan` en el camino crítico (el backfill es offline) |
| IX Simplicidad | ✅ | Función pura, sin servicios extra |

## Diseño

### Algoritmo de asignación (determinístico y uniforme por proveedor)
Necesitamos uniformidad **por proveedor**, porque cada proveedor se procesa en serie. Un `hash(id) mod 1440` simple produce colisiones por proveedor (varios registros del mismo proveedor en el mismo minuto, que tendrían que esperarse entre sí).

```
offset(provider) = fnv64a(provider) mod 1440               // desfasa a los proveedores entre sí
rank(id)         = posición estable del id dentro del proveedor
slot             = (offset + ⌊rank × 1440 / k⌋) mod 1440    // k = registros del proveedor
```
- El backfill calcula `rank` ordenando los ids de cada proveedor (offline, un proveedor por vez).
- **Altas incrementales** (sin recalcular todo): el slot nuevo es el del "hueco" más grande del proveedor. Para eso se mantiene un histograma `provider_slot_counts` en la tabla `coordination` y se elige el minuto de menor ocupación, desempatando por hash. Es O(1440) en memoria.
- `shard = fnv32a(id) mod S`.

### Modelo de datos
```
Table requests
  PK  id (S)
  attrs provider, endpoint, method, body, enabled(BOOL), slot(N), shard(N),
        slot_shard(S "0742#03"), version(N), created_at, updated_at
  GSI gsi_slot_shard  PK slot_shard  SK provider_id ("prov#id")
       projection: KEYS_ONLY + provider, enabled   (el worker lee endpoint/body con GetItem)
```
KEYS_ONLY reduce el costo del GSI y garantiza que el worker siempre lea la versión más reciente del registro.

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
type SlotWriter interface {
    SetSlot(ctx context.Context, id string, slot, shard int, expectedVersion int64) error
}
type Checkpointer interface { Load(ctx, job string) (string, error); Save(ctx, job, cursor string) error }
```

## Estrategia de test
- **Propiedades** (rapid): determinismo, rango válido, uniformidad global y por proveedor para k aleatorio en [1, 500k].
- **Unit**: validaciones de `Request`.
- **Integración** (LocalStack): GSI devuelve exactamente los ítems del slot; backfill interrumpido y reanudado.
- **Bench**: `BenchmarkAssign1M` < 2 s.

## Riesgos
| Riesgo | Mitigación |
|---|---|
| Hot partition en el GSI con 100M | S = 32 shards, queries en paralelo |
| Backfill consume WCU de producción | Rate limiter configurable + on-demand |
| Proveedor sobre capacidad (S5) | Validación US3 + alarma + pregunta de negocio |

## Complexity Tracking
Ninguna violación.
