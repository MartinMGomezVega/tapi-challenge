# Tasks: Manejo de errores y reintentos

**Input**: [spec.md](./spec.md), [plan.md](./plan.md) · TDD obligatorio.

## Fase 1: US1 — Clasificación (P1) 🎯
- [x] T001 [US1] Test table-driven con todas las filas de la tabla de US1 → `internal/domain/errclass_test.go`
- [x] T002 [US1] Test: parseo de `Retry-After` (segundos y fecha HTTP)
- [x] T003 [US1] Implementar `Classifier` configurable
- [x] T004 [US1] Test: el adaptador traduce `net.Error`, `context.DeadlineExceeded` y `*http.Response` a `domain.Response`
- [x] T005 [US1] Implementar `httpapi/errors.go`

## Fase 2: US2 + US3 — Política de reintentos (P1) 🎯
- [x] T006 [US2] Test: `RetryInProcess` sólo para clases transitorias rápidas y `attempt < 2`
- [x] T007 [US2] Test de propiedad: delay en [30 s, 15 min], jitter ±20 %, respeta `Retry-After`
- [x] T008 [US2] Test: `receiveCount ≥ 6` da `Exhausted`
- [x] T009 [US3] Test: las clases no reintentables dan `FailPermanent`; `ALREADY_PROCESSED` da `Complete`
- [x] T010 Implementar `RetryPolicy` (rand inyectable)
- [x] T011 Test: `ExecuteRequest` aplica cada `Decision` (Nack con delay, FailPermanent + Ack, Exhausted + marca)
- [x] T012 Conectar `RetryPolicy` en `app/execute` (reemplazar el stub de 003)
- [x] T013 Test: un error de persistencia (DynamoDB) hace `Nack` sin incrementar `attempts` del proveedor
- [x] T014 [P] Terraform/LocalStack: DLQ FIFO + `RedrivePolicy maxReceiveCount=6`
- [x] T015 Test E2E: proveedor X 100 % 503 → `FAILED_EXHAUSTED` y DLQ; los demás no se afectan (SC-002)
- [x] T016 Test E2E: un 400 → 0 reintentos (SC-004)

## Fase 3: US4 — Circuit breaker (P2)
- [x] T017 [US4] Test con reloj falso: closed → open (50 % de 20) → half-open → closed/open
- [x] T018 [US4] Test: con el breaker abierto hay `Nack(cooldown)` sin llamar y sin contar el intento
- [x] T019 [US4] Implementar `domain/breaker.go` + integración en el worker
- [x] T020 [P] [US4] Métrica `BreakerOpen{provider}`

## Fase 4: US5 — DLQ (P2)
- [x] T021 [US5] Test: `redrive` filtra por proveedor y fecha, y re-encola con el mismo DedupId + `execution → ENQUEUED`
- [x] T022 [US5] Implementar `cmd/redrive`
- [x] T023 [P] [US5] Runbook `docs/runbooks/dlq.md`

## Fase 5: Polish
- [x] T024 [P] Sanitización y truncado de `error_message` (FR-006) con tests
- [x] T025 Actualizar el ADR-006 con los resultados del E2E

## Dependencias
003 (puerto `RetryPolicy`) → Fase 1 → Fase 2 → (Fase 3 ∥ Fase 4).
