# Tasks: Worker

**Input**: [spec.md](./spec.md), [plan.md](./plan.md) · TDD obligatorio · `go test -race` en todo.

## Fase 1: Fundacional
- [ ] T001 Test table-driven: transiciones válidas e inválidas de `ExecutionStatus` → `internal/domain/execution_test.go`
- [ ] T002 Implementar la máquina de estados pura
- [ ] T003 [P] `test/mockapi`: servidor Go con latencia configurable, inyección de errores (5xx, 429, timeout, 4xx) y métrica `max_inflight[provider]`
- [ ] T004 [P] Crear la tabla `executions` + GSI `gsi_run_date_status` en el script de LocalStack

## Fase 2: US1 — Ejecutar y guardar (P1) 🎯
- [ ] T005 [US1] Test: `ExecuteRequest` éxito → `Acquire` → `Call` → `Complete` → `Ack` (fakes)
- [ ] T006 [US1] Test: un registro inexistente da `FAILED_PERMANENT/REQUEST_NOT_FOUND`; uno deshabilitado da `SKIPPED`
- [ ] T007 [US1] Test: una respuesta de más de 350 KB va a `BlobStore` antes de `Complete`, con key determinística
- [ ] T008 [US1] Implementar `app/execute.ExecuteRequest`
- [ ] T009 [US1] Test de integración: `executions.Acquire/Complete` condicionales (incluye un lease vencido)
- [ ] T010 [US1] Implementar el adaptador `dynamo/executions`
- [ ] T011 [US1] Test: el cliente HTTP envía SigV4, `Idempotency-Key` y `traceparent`, y respeta el timeout (con `httptest`)
- [ ] T012 [US1] Implementar `httpapi.Client` (transport afinado)
- [ ] T013 [P] [US1] Implementar el adaptador `s3blob`

## Fase 3: US2 — Exclusividad por proveedor (P1) 🎯
- [ ] T014 [US2] Test: el consumer agrupa un batch por `MessageGroupId` y procesa cada grupo en secuencia (fake SQS, `-count=50`)
- [ ] T015 [US2] Test: mensajes de grupos distintos se procesan en paralelo
- [ ] T016 [US2] Test: `Acquire` con `LeasedByOther` extiende la visibilidad y no llama a la API
- [ ] T017 [US2] Implementar `sqsq.Consumer` (receivers, semáforo, agrupación, heartbeat)
- [ ] T018 [US2] Test de integración E2E: LocalStack FIFO + mockapi, 20 proveedores × 500 mensajes, 4 workers × 16 → `max_inflight == 1` por proveedor

**Checkpoint**: invariante IV.1 demostrado con evidencia (guardar el reporte en `docs/evidence/`).

## Fase 4: US3 — Idempotencia (P1)
- [ ] T019 [US3] Test: un duplicado sobre `SUCCEEDED` o `FAILED_PERMANENT` da `Ack` sin `Call`
- [ ] T020 [US3] Test E2E: 5 % de duplicados inyectados → 0 llamadas duplicadas a mockapi (por `Idempotency-Key`)

## Fase 5: US4 — Presupuesto global (P1)
- [ ] T021 [US4] Test: la config con `k × t > LAMBDA_BUDGET/2` hace fallar el arranque
- [ ] T022 [US4] Test: el receiver no pide más mensajes que los cupos libres del semáforo
- [ ] T023 [US4] Implementar la validación de config y la adquisición previa de cupos
- [ ] T024 [P] [US4] Rate limiter opcional por proveedor (`PROVIDER_RPS`)

## Fase 6: US5 — Graceful shutdown (P2)
- [ ] T025 [US5] Test: SIGTERM detiene la recepción, termina lo que está en vuelo y libera lo pendiente con `Nack(0)`
- [ ] T026 [US5] Implementar el shutdown con `signal.NotifyContext` + errgroup
- [ ] T027 [US5] Test E2E: `kill -TERM` durante la carga → 0 mensajes perdidos (SC-004)

## Fase 7: Servicio y carga
- [ ] T028 `cmd/worker` (config, `/healthz`, wiring)
- [ ] T029 [P] Dockerfile distroless ARM64
- [ ] T030 Test de carga a 120 req/s con 200 proveedores Zipf → reporte SC-001…SC-003 en `docs/evidence/load-test.md`

## Dependencias
Fase 1 → US1 → US2 → (US3 ∥ US4) → US5 → Fase 7. La feature 004 se conecta a través del puerto `RetryPolicy` (stub en esta feature).
