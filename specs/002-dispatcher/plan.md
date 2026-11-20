# Implementation Plan: Dispatcher

**Branch**: `002-dispatcher` · **Spec**: [spec.md](./spec.md)

## Summary
Servicio ECS `dispatcher-svc` (2 réplicas) con un ticker interno de 1 minuto. El líder (por lease con fencing en DynamoDB) calcula los slots pendientes desde el watermark, consulta el GSI en paralelo por shard, crea las ejecuciones PENDING de forma condicional y las envía a SQS FIFO en batches de 10.

## Technical Context
Go ≥ 1.25 · AWS SDK v2 (DynamoDB, SQS) · `golang.org/x/sync/errgroup` · reloj inyectable (`Clock` port) para tests deterministas.

## Constitution Check
| Principio | ✅ | Cómo |
|---|---|---|
| II TDD | ✅ | Reloj falso + fakes de puertos para el caso de uso |
| III Clean Arch | ✅ | `DispatchSlot` y `Tick` en `app`, AWS en `adapters` |
| IV.2 Idempotencia | ✅ | Put condicional + DedupId |
| IV.3 Distribución | ✅ | Un slot por minuto |
| V Escalabilidad | ✅ | Shards en paralelo, batches |
| IX Simplicidad | ⚠️ | El ticker interno en lugar de EventBridge→RunTask se justifica abajo |

## Diseño

### Por qué un ticker interno y no EventBridge Scheduler → RunTask
El arranque de un task de Fargate tarda 30–60 s y sería inestable cada minuto. Un servicio siempre corriendo con lease es más preciso y más barato (1 task chica, 0,25 vCPU). El watermark hace que la precisión del ticker no sea crítica: si un tick se atrasa, el siguiente despacha los slots acumulados.

### Algoritmo `Tick(now)`
```
if !lease.IsHeldAndValid(): try acquire; if not leader → return
wm := watermark.Load()
for (d, s) in range(wm.next(), slotOf(now)):          // incluye cruce de día
    errgroup with limit = S:
        for shard in 0..S-1:
            for page in requests.ListBySlotShard(s, shard):
                for rec in page where rec.enabled:
                    created := executions.CreatePending(rec.id, d)   // condicional
                    if !created && status != PENDING_ORPHAN: continue
                    batch.add(msg)                                   // flush cada 10
                flush → executions.MarkEnqueued(batchIDs)
    watermark.Advance(d, s, fencing=lease.version)    // falla si se perdió el liderazgo
```

### Lease con fencing
`UpdateItem coordination[name=dispatcher-leader] SET owner=:me, expires_at=:t+30s, version=version+1 IF attribute_not_exists(owner) OR expires_at < :now OR owner = :me`. El watermark se escribe con la condición `lease_version = :v`, así que un ex-líder "zombie" no puede avanzarlo.

### Puertos
```go
type Clock interface{ Now() time.Time }
type LeaderLease interface{ TryAcquire(ctx) (token int64, ok bool, err error); Release(ctx) error }
type WatermarkStore interface{ Load(ctx) (Watermark, error); Advance(ctx, Watermark, token int64) error }
type ExecutionCreator interface{ CreatePending(ctx, id string, d RunDate, meta Meta) (created bool, status Status, err error); MarkEnqueued(ctx, keys []ExecKey) error }
type JobQueue interface{ SendBatch(ctx, []JobMessage) (failed []JobMessage, err error) }
```

### Estructura
```
internal/domain/rundate.go, slotclock.go   // slotOf(now, tz), range con cruce de día
internal/app/dispatch/                      // Tick, DispatchSlot
internal/adapters/dynamo/{executions,coordination}.go
internal/adapters/sqsq/queue.go
cmd/dispatcher/main.go                      // ticker, health check, SIGTERM
```

## Estrategia de test
- **Unit** (reloj falso): un día completo de 1440 ticks; caída de 15 min; cruce de medianoche; crash a mitad de slot.
- **Concurrencia**: 2 instancias de `Tick` en goroutines con un fake de lease compartido; se verifica que nunca avanzan los dos el watermark (`go test -race`).
- **Integración** (LocalStack): DedupId efectivo, Put condicional, fencing.
- **Simulación acelerada**: 100k registros, 1 día = 2 min (reloj escalado). Se verifica SC-001.

## Riesgos
| Riesgo | Mitigación |
|---|---|
| Split-brain | Fencing token en el watermark + idempotencia aguas abajo |
| Slot grande que tarda más de 60 s | Shards en paralelo; métrica `SlotDispatchDuration` con alarma > 45 s |
| Dedup de SQS limitado a 5 min | La idempotencia real está en el Put condicional (capa 2) |

## Complexity Tracking
| Desvío | Por qué | Alternativa más simple descartada |
|---|---|---|
| Lease + watermark | Exactly-once efectivo y catch-up | RunTask por minuto: latencia de arranque, sin catch-up |
