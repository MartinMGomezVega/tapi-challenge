# Checkpoint 004 — Manejo de errores y reintentos

**Branch**: `004-errores-y-reintentos` (mergeada a `main` con `--no-ff`)
**Estado**: Completada (`T001`–`T025`).

## 1. Tareas y commits relevantes
- **Fase 1 — Clasificación (`004-T001`..`T005`, `T024`)**:
  - `29343c6` + GREEN commit: `domain.Classifier` configurable con 100 % de la tabla de US1 (`TRANSIENT_NETWORK`, `TRANSIENT_SERVER`, `THROTTLED`, `INVALID_REQUEST`, `AUTH`, `NOT_FOUND`, `ALREADY_PROCESSED`, `INVALID_RESPONSE`, `UNKNOWN`), `ParseRetryAfter` (segundos y fecha HTTP) y `SanitizeErrorMessage` (redacta tokens/emails/números largos, elimina caracteres de control y trunca a 1 KB — FR-006).
  - `5893750` + GREEN commit: `httpapi.TranslateOutcome` (`net.Error`, `context.DeadlineExceeded`, `ECONNRESET`/`EOF`, `X-Amzn-ErrorType` / `TooManyRequestsException` → `domain.ProviderOutcome`).
- **Fase 2 — `RetryPolicy` y conexión en worker (`004-T006`..`T016`)**:
  - `b19641f` + GREEN commit: `domain.RetryPolicy` con rand inyectable (`RetryInProcess` rápido < 1 s y `attempt < 2`; backoff exponencial `[30 s, 15 min]` ±20 % jitter respetando `Retry-After`; `INVALID_RESPONSE` 1 reintento luego `FailPermanent`; `ALREADY_PROCESSED → Complete`; `receiveCount ≥ 6 → Exhausted`).
  - `execute.DomainRetryPolicy` conectado a `app/execute` y `cmd/worker` (reemplazando el stub de 003), más test de persistencia DynamoDB (`Nack` sin llamar al proveedor ni quemar intentos).
  - `TestWorker_LocalStack_RetriesExhaustionIsolationAndRedrive`: E2E en LocalStack FIFO (`maxReceiveCount=6`) demostrando `bad503` → `FAILED_EXHAUSTED` + DLQ con aislamiento total de proveedores sanos (SC-002), `bad400` → `FAILED_PERMANENT` con 0 reintentos (SC-004), y `redrive` real desde DLQ.
- **Fase 3 — Circuit Breaker (`004-T017`..`T020`)**:
  - `6604df5` + GREEN commit: `domain.ProviderBreaker` (ventana deslizante de 20, umbral ≥ 50 %, cooldown 60 s, half-open con sonda única) + integración en `app/execute` y `cmd/worker` (`Nack(cooldown)` sin llamar API ni contar intento) + métrica `BreakerOpen{provider}`.
- **Fase 4 & 5 — DLQ Redrive, Runbook y ADR-006 (`004-T021`..`T025`)**:
  - `716f985` + GREEN commit: `app/redrive`, `sqsq.DLQAdapter`, `dynamo.ExecutionsRepo.ResetForRedrive` (`FAILED_EXHAUSTED → ENQUEUED` preservando `attempts`), `cmd/redrive` (`--provider`, `--since`, `--dry-run`, `--max`).
  - `docs/runbooks/dlq.md` + actualización de ADR-006 en `docs/architecture.md` con evidencia E2E.

## 2. Evidencia de gates y cobertura
- `make test` (`-short -race -cover`) & `make test-full` (`-race`): **PASS**
- `make cover`:
  - `internal/domain`: **92,1 %** (imports 100 % stdlib)
  - `internal/app/assignslots`: **90,3 %**
  - `internal/app/capacity`: **94,4 %**
  - `internal/app/dispatch`: **90,5 %**
  - `internal/app/execute`: **94,7 %**
  - `internal/app/redrive`: **97,1 %**
  - **Total `domain + app`**: **92,4 %** (gate ≥ 90 %)
- E2E LocalStack (`TestWorker_LocalStack_RetriesExhaustionIsolationAndRedrive`): **PASS** (25,17 s)
- `gofmt`, `go vet -tags=integration`, `golangci-lint run --build-tags=integration`, `staticcheck -tags=integration`: **0 issues**
- `govulncheck ./...`: **0 vulnerabilidades**

## 3. Cumplimiento de Success Criteria (`004/spec.md`)
| SC | Criterio | Evidencia | Estado |
|---|---|---|---|
| SC-001 | 100 % de la tabla US1 cubierta con tests table-driven | `TestClassifier_TruthTable` (20 casos + override custom) y `TestParseRetryAfter` | ✅ |
| SC-002 | Proveedor con 100 % 503 no degrada a otros proveedores | `TestWorker_LocalStack_RetriesExhaustionIsolationAndRedrive`: `bad503` llega a `FAILED_EXHAUSTED` + DLQ mientras `good-0..2` completan 30/30 sin afectación | ✅ |
| SC-003 | 0 ejecuciones no terminales 24 h después de `run_date` | Transiciones exhaustivas en `RetryPolicy` + DLQ (`maxReceiveCount=6`) + red de seguridad verificada en feature `005` (`reconciler`) | ✅ |
| SC-004 | Error no reintentable genera 0 reintentos | `TestWorker_LocalStack_RetriesExhaustionIsolationAndRedrive`: `bad400` registra exactamente `calls == 1` en `mockapi` y estado `FAILED_PERMANENT` | ✅ |

## 4. Desvíos y tests modificados (Regla B)
- **Desvíos menores**: Ninguno. Se mantuvo retrocompatibilidad del puerto `execute.RetryPolicy` agregando la interfaz opcional `DurationAwareRetryPolicy` para pasar `callDuration` sin alterar tests de la feature `003`.
- **Tests existentes modificados (Regla B)**: Ninguno.
