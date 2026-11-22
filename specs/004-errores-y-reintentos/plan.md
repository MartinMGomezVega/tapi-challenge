# Implementation Plan: Manejo de errores y reintentos

**Branch**: `004-errores-y-reintentos` · **Spec**: [spec.md](./spec.md)

## Summary
Dos políticas de dominio puras (`Classifier` y `RetryPolicy`) que se conectan al worker por el puerto ya definido en la feature 003. Los reintentos son de dos niveles: en proceso (cortos) y diferidos con `ChangeMessageVisibility` (backoff largo). Después de 6 entregas, el mensaje va a la DLQ. Hay un circuit breaker en memoria por proveedor.

## Technical Context
Go ≥ 1.25 · sin dependencias nuevas (el breaker es propio, ~80 líneas, o `sony/gobreaker` si se prefiere) · SQS DLQ con redrive policy.

## Constitution Check
| Principio | ✅ | Cómo |
|---|---|---|
| II TDD | ✅ | Políticas puras, 100 % table-driven |
| III Clean Arch | ✅ | `Classifier` recibe un `Response` de dominio, no `*http.Response` (el adaptador traduce) |
| IV.4 Sin fallos silenciosos | ✅ | Todo camino termina en un estado terminal + métrica |
| IX Simplicidad | ✅ | Breaker local y no distribuido (se justifica en el spec) |

## Diseño

### Por qué `ChangeMessageVisibility` y no un `DelaySeconds` por mensaje
Las colas FIFO **no admiten delay por mensaje**. Re-encolar un mensaje nuevo rompería el orden y la exclusividad del grupo. `ChangeMessageVisibility` sobre el mensaje en vuelo mantiene el grupo bloqueado durante el backoff. Ese es justamente el *backpressure* deseado para un proveedor con problemas, y no afecta a los otros grupos (ADR-006). Además, cuando un batch de SQS contiene varios mensajes del mismo grupo y uno de ellos resulta diferido (`OutcomeDeferred`), `processGroup` aborta el resto de ese grupo y libera los mensajes pendientes con `Nack(0)` para no seguir golpeando al proveedor degradado.

### Decisión
```go
type Decision struct {
    Kind  DecisionKind // Complete, RetryInProcess, RetryLater, FailPermanent, Exhausted
    Delay time.Duration
    Class ErrorClass
}
func (p RetryPolicy) Decide(class ErrorClass, inProcessAttempt, receiveCount int, retryAfter time.Duration) Decision
```
- `RetryInProcess` sólo si `class ∈ {TRANSIENT_NETWORK, TRANSIENT_SERVER}`, `inProcessAttempt < 2` y la llamada fallida duró menos de 1 s (si no, liberamos el slot).
- `RetryLater.Delay = clamp(max(retryAfter, base × 2^(receiveCount-1)) × U(0.8, 1.2), 30 s, 15 min)`.
- `Exhausted` si `receiveCount ≥ maxReceiveCount`. El worker marca `FAILED_EXHAUSTED` **antes** de que SQS mueva el mensaje a la DLQ, así el estado queda consistente aunque nadie mire la DLQ.

### Circuit breaker
Estados `closed → open → half-open`, ventana deslizante de 20 resultados por proveedor, `sync.Mutex` por proveedor (map con `sync.Map`). Puerto `Breaker{Allow(provider) bool; Record(provider, ok bool)}`.

### Estructura
```
internal/domain/errclass.go       // ErrorClass + Classifier
internal/domain/retrypolicy.go    // RetryPolicy + backoff (rand inyectable para tests)
internal/domain/breaker.go
internal/adapters/httpapi/errors.go   // *http.Response / net.Error → domain.Response
cmd/redrive/main.go
```

## Estrategia de test
- Table-driven del `Classifier` (todas las filas de US1 + `Retry-After` en segundos y en fecha HTTP).
- Propiedades del backoff: monótono (sin jitter), acotado, con jitter en ±20 %.
- Breaker con reloj falso: transiciones completas.
- E2E: mockapi con el proveedor X en 100 % 503 → se verifican SC-002 y que X termina en `FAILED_EXHAUSTED` después de 6 recepciones.

## Riesgos
| Riesgo | Mitigación |
|---|---|
| Un reintento de un POST no idempotente | Idempotency-Key (S3) + ADR-005 |
| Tormenta de reintentos tras la caída de la API | Jitter + breaker + presupuesto global |
| La DLQ crece sin que nadie mire | Alarma > 0 + runbook + reporte diario (feature 005) |

## Complexity Tracking
Ninguna violación.
