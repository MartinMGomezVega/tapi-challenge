# Tasks: Catálogo de registros y asignación de slots

**Input**: [spec.md](./spec.md), [plan.md](./plan.md)
**Formato**: `[ID] [P?] [Story] Descripción` · `[P]` = paralelizable · TDD: cada test DEBE fallar antes de su implementación.

## Fase 1: Setup
- [x] T001 Inicializar módulo Go (`go mod init github.com/<user>/tapi-scheduler`), layout `cmd/ internal/ test/`
- [x] T002 [P] Configurar `golangci-lint`, `staticcheck`, `govulncheck`, `Makefile` (`make test`, `make lint`, `make it`)
- [x] T003 [P] `docker-compose.yml` con LocalStack (DynamoDB, SQS, S3) + script de creación de tablas
- [x] T004 [P] Helper de testcontainers-go para LocalStack en `test/testenv/`

## Fase 2: Fundacional
- [ ] T005 Test: validaciones de `Request` (endpoint en allowlist, body JSON ≤ 256 KB, provider no vacío) → `internal/domain/request_test.go`
- [ ] T006 Implementar `Request` + errores de dominio tipados → `internal/domain/request.go`

## Fase 3: US1 — Slot por registro (P1) 🎯 MVP
- [ ] T007 [US1] Test de propiedad: `Assign` es determinístico → `internal/domain/slot_test.go`
- [ ] T008 [US1] Test de propiedad: `slot ∈ [0,1440)`, `shard ∈ [0,S)`
- [ ] T009 [US1] Test de propiedad: uniformidad global ≤ 1,10 × promedio con 1M
- [ ] T010 [US1] Test de propiedad: uniformidad por proveedor (`≤ ⌈k/1440⌉+1` por minuto) y k < 1440 equiespaciado
- [ ] T011 [US1] Implementar `SlotAssigner` (offset + rank) → `internal/domain/slot.go`
- [ ] T012 [US1] Test: asignación incremental elige el minuto de menor ocupación
- [ ] T013 [US1] Implementar asignación incremental con histograma
- [ ] T014 [US1] Test de integración: `ListBySlotShard` devuelve sólo los ítems del slot, paginado → `internal/adapters/dynamo/requests_it_test.go`
- [ ] T015 [US1] Implementar adaptador `requests` (GetItem, Query GSI, SetSlot condicional por `version`)
- [ ] T016 [P] [US1] Benchmark `BenchmarkAssign1M` (< 2 s)

**Checkpoint**: slots válidos y uniformes, consultables por GSI.

## Fase 4: US2 — Backfill (P1)
- [ ] T017 [US2] Test: el caso de uso `AssignSlots` es idempotente (2 runs = mismo estado)
- [ ] T018 [US2] Test: reanuda desde el checkpoint tras una interrupción (fake `Checkpointer`)
- [ ] T019 [US2] Implementar `app/assignslots` (parallel scan por segmento, rate limiter de WCU, checkpoint)
- [ ] T020 [US2] Test de integración: 100k registros en LocalStack, cortar y reanudar
- [ ] T021 [US2] `cmd/slotter` con flags (`--segments`, `--wcu-limit`, `--shards`) y graceful shutdown
- [ ] T022 [P] [US2] Generador de dataset sintético (`cmd/seed`): 1M de registros con una distribución de proveedores Zipf

**Checkpoint**: 1M de registros con slot en < 30 min (SC-003).

## Fase 5: US3 — Capacidad por proveedor (P2)
- [ ] T023 [US3] Test: el cálculo `k × p95 > 86400 × 0,7` marca al proveedor como sobre capacidad
- [ ] T024 [US3] Implementar `app/capacity` + métrica EMF `ProviderOverCapacity`
- [ ] T025 [US3] Integrar en el `reconciler` (feature 005) como paso diario

## Fase 6: Polish
- [ ] T026 [P] Documentar el algoritmo en `docs/architecture.md` (ADR-004) con el gráfico del histograma de 1M
- [ ] T027 Verificar cobertura ≥ 90 % en `domain` y `app`

## Dependencias
Setup → Fundacional → US1 → (US2 ∥ US3). Las features 002 y 005 dependen de US1.
