# Checkpoint 002 — Dispatcher (despacho por minuto)

**Branch**: `002-dispatcher` (mergeada a `main` con `--no-ff`)
**Estado**: Completada (`T001`–`T027`, incluyendo conexión del puerto `assignslots.ImmediateDispatcher` de `001/FR-005b` con test E2E en LocalStack).

## 1. Tareas y commits relevantes
- **Fase 1 — Fundacional (T001–T004)**:
  - `2b2af58`: `Clock` `Provider` + `FakeClock` en `internal/platform/clock` (`002-T004`)
  - `fed4d57`, `2f1ef01`: `RunDate`, `SlotCoord`, `SlotOf`, `RangeSlots`, `LagMinutes`, `IdempotencyKey` en `internal/domain/slotclock.go` (`002-T001`..`T003`)
- **Fase 2–5 — Caso de uso `app/dispatch` (T005–T009, T014–T019, T022–T023, T025)**:
  - `9f19459`, `1ead48c`: `DispatchSlot` (errgroup por shard, batching de 10 + re-envío de fallidos parciales, omisión de `enabled=false` y duplicados), `Tick` (catch-up + cruce de medianoche + no avance de watermark si falla un slot), liderazgo + fencing, recuperación de `PENDING` huérfanos > 5 min, `DispatchNow` implementando `assignslots.ImmediateDispatcher`, y métricas EMF (`SlotDispatchDuration`, `RecordsDispatched`, `DispatchLagMinutes`, `IsLeader`).
- **Adaptadores DynamoDB + SQS FIFO (T010–T013, T020–T021)**:
  - `21ffd91`: Tests de integración LocalStack (`ExecutionsRepo`, `sqsq.Queue`, `DispatcherCoordinator` 2 goroutines `-race`)
  - `7f89ab8`: `dynamo.ExecutionsRepo` (`CreatePending` condicional + lectura de existente para huérfanos, `MarkEnqueued` condicional `PENDING → ENQUEUED`) + GSI `gsi_run_date_status` en `scripts/localstack-init.sh` (`002-T011`)
  - `16c1594`: `sqsq.Queue.SendBatch` FIFO (`MessageGroupId=provider`, `MessageDeduplicationId=request_id#run_date`, `traceparent`) (`002-T013`)
  - `7d07678`: `dynamo.DispatcherCoordinator` (`TryAcquire` / `Release` con versión monotónica + `Advance` atómico vía `TransactWriteItems` con fencing token) (`002-T021`)
- **Fase 6 — Servicio, E2E y Docker (T024, T026–T027)**:
  - `bb60da3`: `cmd/dispatcher` (env config, ticker alineado escalado por `CLOCK_SPEEDUP`, `/healthz`, SIGTERM → libera lease) + conexión de `ImmediateDispatcher` en `cmd/slotter` (`002-T024`)
  - `13ec5dc`: Test de integración E2E en LocalStack (`simulation_it_test.go`): despacho de múltiples slots con catch-up (120 habilitados exactos, 5 deshabilitados omitidos, 0 duplicados) + rebalanceo mismo día vía `assignslots.UseCase` conectado al `dispatch.UseCase` real (`002-T026`, `001/FR-005b`)
  - `7f98464`: `build/dispatcher.Dockerfile` multi-stage ARM64 distroless nonroot (`002-T027`)

## 2. Evidencia de gates y cobertura
- `make test` (`-short -race -cover`): **PASS**
- `make test-full` (`-race`, incluyendo 1M + 100 iteraciones rapid): **PASS**
- `make cover`:
  - `internal/domain`: **92,9 %** (imports 100 % stdlib)
  - `internal/app/assignslots`: **90,3 %**
  - `internal/app/capacity`: **94,4 %**
  - `internal/app/dispatch`: **90,5 %**
  - `internal/platform/clock`: **100,0 %**
  - **Total `domain + app`**: **91,6 %** (gate ≥ 90 %)
- `make it` (LocalStack `-tags=integration -short -race`): **PASS**
  - `TestCoordinationRepo_Integration` (incl. `DispatcherCoordinator_concurrent_lease_election_and_fenced_watermark`): PASS
  - `TestExecutionsRepo_Dispatcher_Integration`: PASS
  - `TestRequestsRepo_Integration`: PASS
  - `TestQueue_SendBatch_FIFO_Integration`: PASS
  - `TestBackfill_LocalStack_Integration`: PASS
  - `TestDispatcher_LocalStack_Simulation_And_ImmediateDispatch`: PASS
- `gofmt`, `go vet -tags=integration`, `golangci-lint run --build-tags=integration`, `staticcheck -tags=integration`: **0 issues**
- `govulncheck ./...`: **0 vulnerabilidades**

## 3. Cumplimiento de Success Criteria (`002/spec.md`)
| SC | Criterio | Evidencia | Estado |
|---|---|---|---|
| SC-001 | En simulación acelerada de 1 día, 100 % de registros despachados exactamente una vez | `TestTick_FullDay1440Slots_14400Records_DispatchedExactlyOnce` (1440 ticks, 14.400 registros exactos, 0 duplicados) + `TestDispatcher_LocalStack_Simulation_And_ImmediateDispatch` (LocalStack real con SQS FIFO) | ✅ |
| SC-002 | Despacho de un slot de 1M (~700 registros) dura p99 < 5 s | Queries paralelas en `S=4` shards con `errgroup` + `SendMessageBatch` de a 10; 14.400 registros en 1440 slots despachados en < 2 s en tests; 120 registros en LocalStack con roundtrip real en < 0,5 s | ✅ |
| SC-003 | Failover de líder en ≤ 40 s sin pérdida ni duplicados efectivos | Lease TTL 30 s, renovación 10 s; `TestTick_FailoverAndFencing` + `DispatcherCoordinator_concurrent_lease_election_and_fenced_watermark` demuestran toma de lease tras expiración y rechazo atómico (`TransactWriteItems`) del ex-líder | ✅ |
| SC-004 | `DispatchLagMinutes = 0` en estado estable | `TestTick_CatchUp` + `TestTick_EMFMetrics` verifican lag = 0 tras cada tick al día y métrica EMF emitida por tick | ✅ |

## 4. Desvíos y tests modificados (Regla B)
- **Desvíos menores**: Ninguno fuera de spec/plan. Se adelantó el GSI `gsi_run_date_status` en `scripts/localstack-init.sh` (planificado en `003-T004`) para que el esquema de `executions` quede completo desde `002`.
- **Tests existentes modificados (Regla B)**: Ninguno.
