# Tasks: Dispatcher

**Input**: [spec.md](./spec.md), [plan.md](./plan.md) · TDD obligatorio.

## Fase 1: Fundacional (dominio de tiempo)
- [x] T001 Test: `SlotOf(now, tz)` devuelve `(run_date, slot)` correcto en UTC y en `America/Argentina/Buenos_Aires` → `internal/domain/slotclock_test.go`
- [x] T002 Test: `RangeSlots(from, to)` itera en orden e incluye el cruce de medianoche
- [ ] T003 Implementar `RunDate`, `SlotOf`, `RangeSlots`
- [x] T004 [P] Port `Clock` + `FakeClock` (avance manual) en `internal/platform/clock`

## Fase 2: US1 — Despacho del slot actual (P1) 🎯
- [ ] T005 [US1] Test: `DispatchSlot` crea PENDING y envía un mensaje por registro habilitado (fakes)
- [ ] T006 [US1] Test: omite registros `enabled=false`
- [ ] T007 [US1] Test: si `CreatePending` devuelve `created=false`, no envía mensaje
- [ ] T008 [US1] Test: batching de 10 y re-envío de los fallidos parciales de `SendBatch`
- [ ] T009 [US1] Implementar `app/dispatch.DispatchSlot` con errgroup por shard
- [ ] T010 [US1] Test de integración: `executions.CreatePending` condicional en LocalStack
- [ ] T011 [US1] Implementar adaptador `executions` (CreatePending, MarkEnqueued con BatchWrite/Transact)
- [ ] T012 [US1] Test de integración: mensaje FIFO con GroupId y DedupId correctos
- [ ] T013 [US1] Implementar adaptador `sqsq.Queue.SendBatch`

## Fase 3: US2 — Catch-up (P1)
- [ ] T014 [US2] Test: `Tick` con watermark atrasado 15 min despacha 15 slots en orden
- [ ] T015 [US2] Test: cruce de medianoche (`D` 1435..1439 y `D+1` 0..3)
- [ ] T016 [US2] Test: el watermark no avanza si el slot falla a mitad de camino
- [ ] T017 [US2] Implementar `app/dispatch.Tick` + `WatermarkStore`

## Fase 4: US3 — Liderazgo (P1)
- [ ] T018 [US3] Test: sólo una de 2 instancias obtiene el lease (fake con reloj)
- [ ] T019 [US3] Test: con el lease vencido, la otra lo toma; el ex-líder no puede avanzar el watermark (fencing)
- [ ] T020 [US3] Test de integración: lease condicional real en DynamoDB, 2 goroutines, `-race`
- [ ] T021 [US3] Implementar adaptador `coordination` (lease + watermark con fencing)

## Fase 5: US4 — Huérfanos (P2)
- [ ] T022 [US4] Test: PENDING con más de 5 min se re-envía; ENQUEUED no
- [ ] T023 [US4] Implementar la re-emisión de huérfanos en `DispatchSlot`

## Fase 6: Servicio
- [ ] T024 `cmd/dispatcher`: config (env), ticker alineado al minuto, `/healthz`, SIGTERM → libera el lease
- [ ] T025 [P] Métricas EMF (`SlotDispatchDuration`, `RecordsDispatched`, `DispatchLagMinutes`, `IsLeader`)
- [ ] T026 Simulación acelerada de 1 día con 100k registros en docker-compose → verifica SC-001
- [ ] T027 [P] Dockerfile multi-stage (`golang` → `distroless/static:nonroot`, ARM64)

## Dependencias
001-US1 → Fase 1 → US1 → US2 → US3 → US4 → Servicio.
