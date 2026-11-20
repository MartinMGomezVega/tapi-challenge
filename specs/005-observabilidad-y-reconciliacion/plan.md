# Implementation Plan: Observabilidad y reconciliación

**Branch**: `005-observabilidad-y-reconciliacion` · **Spec**: [spec.md](./spec.md)

## Summary
`cmd/reconciler` es una ECS task diaria que verifica la cobertura de `D`, re-despacha los faltantes y publica un reporte y métricas. La telemetría es transversal: `slog` JSON, OpenTelemetry (ADOT sidecar → X-Ray) y métricas EMF en CloudWatch. Alarmas y dashboard en Terraform.

## Technical Context
Go ≥ 1.25 · `log/slog` · `go.opentelemetry.io/otel` · EMF (stdout JSON, sin llamadas a la API de CloudWatch) · S3 + Glue table + Athena para los reportes.

## Constitution Check
| Principio | ✅ | Cómo |
|---|---|---|
| II TDD | ✅ | Caso de uso `Reconcile` con fakes; el cálculo de cobertura es puro |
| IV.4 Sin fallos silenciosos | ✅ | La reconciliación es la red de seguridad final |
| V Escalabilidad | ✅ | Recorrido por slot y shard en paralelo, sin Scan |
| VI Observabilidad | ✅ | Esta feature |

## Diseño

### Por qué recorrer el catálogo y no `executions`
La pregunta es "¿qué registro **no** tiene ejecución?". Eso sólo se responde partiendo del catálogo (el conjunto esperado). Recorrerlo por `slot_shard` reutiliza el GSI de la feature 001 y se paraleliza naturalmente.

### Algoritmo
```
expected := 0; byStatus := map
errgroup(limit=64) for slot in 0..1439, shard in 0..S-1:
    keys := requests.ListBySlotShard(slot, shard) where enabled && created_before_slot(D)
    execs := executions.BatchGet(keys × D)        // chunks de 100
    for k in keys:
        e := execs[k]
        switch { case e==nil: missing ; case !terminal(e) && stale(e, 2h): stuck ; default: byStatus[e.status]++ }
redispatch(missing ∪ stuck)   // vía el puerto JobQueue de la 002 + CreatePending/ResetStale
report → S3 ; metrics → EMF ; coverage := terminal/expected
```

### Telemetría
- `internal/platform/telemetry`: `NewLogger`, `NewTracer`, `Metrics` (EMF writer). Se inyectan por constructor y no son globales.
- Propagación de la traza: el dispatcher escribe `traceparent` en `MessageAttributes` y el worker la extrae.
- Middleware de logging con campos de contexto (`slog.With`).

### Estructura
```
internal/domain/coverage.go
internal/app/reconcile/
internal/platform/telemetry/{log,trace,emf}.go
cmd/reconciler/main.go
deploy/terraform/modules/observability/   // alarmas, dashboard, SNS
docs/runbooks/*.md
```

## Estrategia de test
- Unit: cobertura y clasificación `missing/stuck/terminal`; exclusión de los registros creados después del slot.
- Integración: 10k registros + fault injection en LocalStack → reporte correcto y re-despacho idempotente (correrlo dos veces = mismo resultado).
- Benchmark: 1M en LocalStack → extrapolación documentada a 100M.
- Contract test del formato EMF (golden file).

## Riesgos
| Riesgo | Mitigación |
|---|---|
| El re-despacho masivo satura a un proveedor | Pasa por la misma FIFO: la exclusividad se mantiene |
| Costo de lectura con 100M | KEYS_ONLY + BatchGet: ~100M RRU por día; se documenta en el análisis de costos |

## Complexity Tracking
Ninguna violación.
