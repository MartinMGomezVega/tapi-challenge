# Benchmark & Extrapolación — Reconciler (`005-T017`, SC-002)

**Fecha**: 2026-11-21 · **Hardware**: Apple M1 Pro (ARM64), Go 1.26.11

## 1. Benchmark CPU + agregación en memoria (1M registros, 1440 × 4 shards, 64 goroutines)
Comando:
```bash
go test -bench=BenchmarkReconcile1M -benchmem -run='^$' ./internal/app/reconcile/...
```
Resultado medido:
- `BenchmarkReconcile1M-10`: **235,2 ms/op** para **1.002.240 registros** (~4,26 M registros/s de procesamiento Go puro con `errgroup` de 64 workers).

## 2. Proyección de I/O DynamoDB en AWS (1M y 100M registros — SC-002)
El reconciler realiza:
1. `Query` sobre `gsi_slot_shard` (`1440 × S` particiones en paralelo, sin `Scan` completo).
2. `BatchGetItem` en chunks de 100 claves sobre `executions` (`N / 100` llamadas).

| Escenario | `N` registros | Shards (`S`) | Particiones (`1440×S`) | `BatchGetItem` calls (`N/100`) | Latencia DynamoDB p50/p95 | Paralelismo | Tiempo proyectado | Objetivo SC-002 | Estado |
|---|---|---|---|---|---|---|---|---|---|
| **1M** | 1.000.000 | 4 | 5.760 | 10.000 | 8 ms / 15 ms | 64 | ~15.760 × 10 ms / 64 ≈ **2,5 s** (con margen p95 + CPU + S3 < **30 s**) | **< 10 min** | ✅ (20× margen) |
| **10M** | 10.000.000 | 8 | 11.520 | 100.000 | 8 ms / 15 ms | 128 | ~111.520 × 10 ms / 128 ≈ **9 s** (con margen p95 < **3 min**) | — | ✅ |
| **100M** | 100.000.000 | 32 | 46.080 | 1.000.000 | 8 ms / 15 ms | 256 (o 4 tasks ECS × 64) | ~1.046.080 × 15 ms / 256 ≈ **61 s** (~15 min en Worst-Case con paginación y backoff) | **< 2 h** | ✅ (8× margen) |

> **Nota de costo I/O (100M)**: 100M lecturas `EventuallyConsistent` (0,5 RRU cada una) = 50M RRU en `executions` + ~5M RRU en `gsi_slot_shard` (`INCLUDE` atributos mínimos), detallado en `docs/costs.md`.
