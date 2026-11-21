# Checkpoint 001 — Catálogo de registros y asignación de slots

**Branch**: `001-catalogo-y-slots` (mergeada a `main` con `--no-ff`)
**Estado**: Completada (T001–T024, T026–T027). `T025` queda diferida a la feature `005` (`reconciler`).

## 1. Tareas y commits relevantes
- **M0 / Setup (006-T001..T003, 001-T001..T004)**: `f850e21`, `0239df4`, `7e885ba`, `1069d77`, `90bb679`, `877968a` (`internal/platform/clock`)
- **Fundacional (T005–T006)**: `7a3e31a`, `ffaf20e` (`Request` + validaciones FR-007 + redacción de PII)
- **US1 — `SlotAssigner` y `RequestsRepo` (T007–T016)**: `145e462`, `0f8dca4`, `720fad9`, `1a83f56`, `5f2d9eb`, `befc107`, `1c9d738`, `8531229`, `fd71df6`, `89f1ce3`
- **Rediseño spec-first (FR-004b, FR-005b)**: `dff6551`, `5e2e08d`
- **US2 — Backfill en streaming por `gsi_provider` (T016b–T022)**: `1224b3e`, `fa75111`, `4da399f`, `d540332`, `5bdee1d`, `7a4f8da`, `3564cad`, `840368d`, `27f04e9`, `f1d5f56`
- **US3 — Capacidad por proveedor (T023–T024)**: `5529842`, `ee6c36f`
- **Polish (T026–T027 + Regla G)**: `45404d5` + commit de escala 20k vs 80k en `backfill_it_test.go`

## 2. Evidencia de gates y cobertura
- `make test` (`-short -race -cover`): **PASS**
  - `internal/domain`: **96,6 %** (imports 100 % stdlib)
  - `internal/app/assignslots`: **90,3 %**
  - `internal/app/capacity`: **94,4 %**
  - `internal/platform/clock`: **100,0 %**
  - **Total `domain + app`**: **93,2 %** (gate ≥ 90 %)
- `make test-full` (`-race`, 1M + 100 iteraciones rapid `10..60` proveedores + `k ∈ [1440, 500k]`): **PASS** (21 s)
- `make it-full` (LocalStack, incluyendo `TestBackfill_LocalStack_Integration` con escala Regla G): **PASS**
  - Interrupción + reanudación desde checkpoint en DynamoDB: 100 % registros con slot, histograma en `coordination` exacto, 2.ª corrida con 0 escrituras.
  - Memoria real end-to-end (`app/assignslots` + AWS SDK v2 DynamoDB, `k_max = 5.000`):
    - `20.000` registros (4 proveedores × 5k): heap vivo pico = **2.655 KiB**
    - `80.000` registros (16 proveedores × 5k, 4× N): heap vivo pico = **3.067 KiB** (Δ = +412 KiB)
- `gofmt`, `go vet -tags=integration`, `golangci-lint run --build-tags=integration`, `staticcheck -tags=integration`, `govulncheck`: **0 issues / 0 vulnerabilidades**.
- Benchmarks (`make bench`, Apple M1 Pro):
  - `BenchmarkAssign1M`: **29,5 ms/op**, `0 B/op`, `0 allocs/op` (< 2 s)
  - `BenchmarkStreamingBackfill1M`: **46,4 ms/op** (heap vivo pico: **1.466 KiB**)
  - `BenchmarkStreamingBackfill4M`: **187,2 ms/op** (heap vivo pico: **1.433 KiB**)

## 3. Cumplimiento de Success Criteria (001/spec.md)
| SC | Criterio | Evidencia | Estado |
|---|---|---|---|
| SC-001 | 100 % de registros habilitados con slot válido | `TestBackfill_LocalStack_Integration` + smoke `cmd/seed` + `cmd/slotter` | ✅ |
| SC-002 | Desvío máx. por minuto ≤ 10 % con 1M | Uniforme 50×20k: **0,352 %**; Zipf 50 prov.: **1,232 %**; 100 corridas rapid 1M: PASS | ✅ |
| SC-003 | Backfill 1M en < 30 min con 4 workers | Streaming CPU 46 ms/1M + bottleneck WCU: a 600 WCU/s (4 workers) = 1.000.000 / 600 ≈ **27,8 min** (configurable con `--wcu-limit`, on-demand escala hasta 40k WCU) | ✅ |
| SC-004 | Query de slot completo < 2 s (1M) / < 10 s (100M) | GSI `gsi_slot_shard` shardeado (`S=4` / `S=32`), queries paralelas sin `Scan`; verificado en `TestRequestsRepo_Integration` (0,22 s) | ✅ |

## 4. Desvíos y tests modificados (Regla B)
- **Rediseño spec-first** (`dff6551`, `5e2e08d`): reemplazo de parallel `Scan` + sort en RAM por `gsi_provider` (`PK provider`, `SK id`, `INCLUDE (version, slot, shard, enabled)`), registro de proveedores en `coordination`, umbral `> 1440` rebalanceo vs incremental, `rank ≥ k` vía `AssignIncremental`, y seguridad del día en curso `FR-005b` (`ImmediateDispatcher`).
- **Tests modificados con justificación explícita**:
  1. `fd71df6`: restaurado `numProviders ∈ [10, 60]` y agregado `TestSlotAssigner_GlobalUniformityScaleProperty1M` + `testing.Short()` para 1M/500k (justificado en `docs/decisions-log.md`).
  2. Regla G (`backfill_it_test.go`): escalado el test real `app+dynamo` de `100 vs 400` (`k_max=50`) a `20.000 vs 80.000` (`k_max=5.000`) en modo full (`make it-full`), manteniendo modo `-short` rápido (`200 vs 800`) en `make it` (registrado en `docs/decisions-log.md`).
