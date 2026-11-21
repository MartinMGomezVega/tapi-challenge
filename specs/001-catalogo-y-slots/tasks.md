# Tasks: Catálogo de registros y asignación de slots

**Input**: [spec.md](./spec.md), [plan.md](./plan.md)
**Formato**: `[ID] [P?] [Story] Descripción` · `[P]` = paralelizable · TDD: cada test DEBE fallar antes de su implementación.

## Fase 1: Setup
- [x] T001 Inicializar módulo Go (`go mod init github.com/<user>/tapi-scheduler`), layout `cmd/ internal/ test/`
- [x] T002 [P] Configurar `golangci-lint`, `staticcheck`, `govulncheck`, `Makefile` (`make test`, `make lint`, `make it`)
- [x] T003 [P] `docker-compose.yml` con LocalStack (DynamoDB, SQS, S3) + script de creación de tablas
- [x] T004 [P] Helper de testcontainers-go para LocalStack en `test/testenv/`

## Fase 2: Fundacional
- [x] T005 Test: validaciones de `Request` (endpoint en allowlist, body JSON ≤ 256 KB, provider no vacío) → `internal/domain/request_test.go`
- [x] T006 Implementar `Request` + errores de dominio tipados → `internal/domain/request.go`

## Fase 3: US1 — Slot por registro (P1) 🎯 MVP
- [x] T007 [US1] Test de propiedad: `Assign` es determinístico → `internal/domain/slot_test.go`
- [x] T008 [US1] Test de propiedad: `slot ∈ [0,1440)`, `shard ∈ [0,S)`
- [x] T009 [US1] Test de propiedad: uniformidad global ≤ 1,10 × promedio con 1M
- [x] T010 [US1] Test de propiedad: uniformidad por proveedor (`≤ ⌈k/1440⌉+1` por minuto) y k < 1440 equiespaciado
- [x] T011 [US1] Implementar `SlotAssigner` (offset + rank) → `internal/domain/slot.go`
- [x] T012 [US1] Test: asignación incremental elige el minuto de menor ocupación
- [x] T013 [US1] Implementar asignación incremental con histograma
- [x] T014 [US1] Test de integración: `ListBySlotShard` devuelve sólo los ítems del slot, paginado → `internal/adapters/dynamo/requests_it_test.go`
- [x] T015 [US1] Implementar adaptador `requests` (GetItem, Query GSI, SetSlot condicional por `version`)
- [x] T016 [P] [US1] Benchmark `BenchmarkAssign1M` (< 2 s)

**Checkpoint**: slots válidos y uniformes, consultables por GSI.

## Fase 4: US2 — Backfill (P1)
- [x] T016b [US2] Agregar GSI `gsi_provider` (`PK provider`, `SK id`, `INCLUDE (version, slot, shard, enabled)`) en `scripts/localstack-init.sh` y `test/testenv/localstack.go`
- [x] T016c [US2] Test de integración + implementación de `CountByProvider` y `ListByProvider` (ordenado por SK, proyectando `version, slot, shard, enabled`, paginado, sin `Scan`) en `internal/adapters/dynamo/requests.go` (FR-004b)
- [x] T017 [US2] Test: el caso de uso `AssignSlots` es idempotente (2 runs = mismo estado), aplica rebalanceo completo si `nuevos > 1440` o asignación incremental si `nuevos ≤ 1440`, y asigna altas concurrentes (`rank ≥ k`) vía `AssignIncremental`
- [x] T017b [US2] Test: seguridad del día en curso (FR-005b / R1) — registro habilitado con `slot_nuevo < slot_actual ≤ slot_viejo` se despacha vía `ImmediateDispatcher`; caso inverso (ya ejecutado → slot futuro) no despacha de nuevo
- [x] T018 [US2] Test: reanuda desde el checkpoint `(proveedor, cursor, rank)` tras una interrupción (fake `Checkpointer`)
- [x] T018b [US2] Test: el backfill persiste el registro del proveedor (`k` e histograma `provider_slot_counts`) en `coordination` para altas incrementales y US3
- [x] T019 [US2] Implementar `app/assignslots` (Query en streaming por proveedor sobre `gsi_provider` sin `Scan` ni sort en RAM, `errgroup` por proveedor, rate limiter de WCU, checkpoint `(proveedor, cursor)`, umbral `> 1440` rebalanceo vs incremental, `rank ≥ k` vía histograma y puerto `ImmediateDispatcher` con stub)
- [x] T019b [US2] Implementar store DynamoDB en `coordination` para registro de proveedores (`k` + histograma) y checkpoints de backfill (`internal/adapters/dynamo/coordination.go`) con tests de integración
- [x] T020 [US2] Test de integración: backfill en LocalStack con corte y reanudación, verificando 100 % de slots asignados, registro/histograma persistido en `coordination` y memoria acotada sobre `app/assignslots` + adaptador real
- [x] T021 [US2] `cmd/slotter` con flags (`--workers`, `--wcu-limit`, `--shards`) y graceful shutdown
- [x] T022 [P] [US2] Generador de dataset sintético (`cmd/seed`): 1M de registros con distribución de proveedores Zipf y registro de proveedores en `coordination`

**Checkpoint**: 1M de registros con slot en < 30 min (SC-003).

## Fase 5: US3 — Capacidad por proveedor (P2)
- [x] T023 [US3] Test: el cálculo `k × p95 > 86400 × 0,7` marca al proveedor como sobre capacidad
- [x] T024 [US3] Implementar `app/capacity` + métrica EMF `ProviderOverCapacity`
- [ ] T025 [US3] Integrar en el `reconciler` (feature 005) como paso diario

## Fase 6: Polish
- [ ] T026 [P] Documentar el algoritmo en `docs/architecture.md` (ADR-004) con el gráfico del histograma de 1M
- [ ] T027 Verificar cobertura ≥ 90 % en `domain` y `app`

## Dependencias
Setup → Fundacional → US1 → (US2 ∥ US3). Las features 002 y 005 dependen de US1.
