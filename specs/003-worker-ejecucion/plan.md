# Implementation Plan: Worker

**Branch**: `003-worker-ejecucion` · **Spec**: [spec.md](./spec.md)

## Summary
`worker-svc` en ECS Fargate. Un pipeline con concurrencia acotada: R goroutines receptoras → canal → semáforo de k → caso de uso `ExecuteRequest`. La exclusividad por proveedor la garantiza SQS FIFO (`MessageGroupId`), con un lease en `executions` como segunda barrera. El resultado se persiste con transiciones condicionales.

## Technical Context
Go ≥ 1.25 · AWS SDK v2 (SQS, DynamoDB, S3) · `net/http` con transport afinado · `aws/signer/v4` · `golang.org/x/sync/semaphore`, `errgroup` · `golang.org/x/time/rate` · OpenTelemetry.

## Constitution Check
| Principio | ✅ | Cómo |
|---|---|---|
| II TDD | ✅ | `ExecuteRequest` con fakes; tests de concurrencia con `-race` |
| III Clean Arch | ✅ | El caso de uso no conoce SQS ni HTTP |
| IV.1 Exclusividad | ✅ | FIFO + lease + test de carga que mide la concurrencia |
| IV.2 Idempotencia | ✅ | Estados condicionales + Idempotency-Key |
| V Escalabilidad | ✅ | Autoscaling horizontal |
| VII Seguridad | ✅ | SigV4 por VPC endpoint `execute-api`, rol con mínimo privilegio |

## Diseño

### Modelo de concurrencia
```
            ┌─ receiver#1 ─┐                        ┌─ handle(msg) ─┐
SQS FIFO ───┼─ receiver#2 ─┼──► chan Message (cap k) ┼─ handle(msg) ─┼──► ack / nack
            └─ receiver#R ─┘   semáforo k           └─ handle(msg) ─┘
```
- Cada receptor **adquiere los cupos del semáforo antes** de `ReceiveMessage` (`MaxNumberOfMessages = cupos libres ≤ 10`), así nunca hay mensajes retenidos sin procesar consumiendo su visibility timeout (US4.2).
- `R = 2`, `k = 32` por defecto. El presupuesto global se valida en el arranque.
- En FIFO, un `ReceiveMessage` puede devolver varios mensajes **del mismo grupo**, pero en orden. Para no paralelizarlos dentro de un mismo task, los mensajes de un batch se agrupan por `MessageGroupId` y cada grupo se procesa **secuencialmente en una goroutine**. Este detalle es fácil de pasar por alto y es crítico.

### Caso de uso `ExecuteRequest(msg)`
```
exec := executions.Acquire(id, date, owner, leaseTTL)   // condicional
switch exec.outcome:
  AlreadyTerminal → ack
  LeasedByOther   → extend visibility(lease remaining); return
req := requests.Get(id)            // NotFound → FAILED_PERMANENT ; disabled → SKIPPED
ctx, cancel := context.WithTimeout(ctx, 25s); hb := heartbeat(msg)
res, err := api.Call(ctx, req, idemKey)
decision := retryPolicy.Decide(err, res, exec.attempts)   // feature 004
apply(decision)  → Complete / ScheduleRetry / FailPermanent
```

### Cliente HTTP (C2 y C3)
- `MaxIdleConnsPerHost = k`, `IdleConnTimeout = 90s`, HTTP/1.1 keep-alive. TLS hacia el VPC endpoint privado de API Gateway.
- Timeout de 25 s por llamada, menor que el límite de integración de API Gateway (29 s), para contemplar cold starts encadenados de las 2 lambdas.

### Persistencia
- `executions` es una sola tabla para estado y resultado: un único ítem por `request_id#run_date` permite transiciones atómicas sin transacciones multi-ítem.
- Payloads de más de 350 KB van a S3 (el límite de un ítem de DynamoDB es 400 KB) y se escriben **antes** del `UpdateItem` final; la key de S3 es determinística, así que también es idempotente.

### Puertos
```go
type ExecutionStore interface {
    Acquire(ctx, key ExecKey, owner string, ttl time.Duration) (Execution, AcquireOutcome, error)
    Complete(ctx, key ExecKey, owner string, r Result) error
    ScheduleRetry(ctx, key ExecKey, owner string, e ErrInfo) error
    FailPermanent(ctx, key ExecKey, owner string, e ErrInfo) error
}
type RequestGetter interface{ Get(ctx, id string) (domain.Request, error) }
type ProviderAPI interface{ Call(ctx, req domain.Request, idemKey string) (Response, error) }
type BlobStore interface{ Put(ctx, key string, b []byte) error }
type MessageAcker interface{ Ack(ctx, m Msg) error; Nack(ctx, m Msg, delay time.Duration) error; Extend(ctx, m Msg, d time.Duration) error }
```

### Estructura
```
internal/domain/execution.go           // estados y transiciones válidas (máquina de estados pura)
internal/app/execute/                  // ExecuteRequest
internal/adapters/sqsq/consumer.go     // receivers + semáforo + agrupación por grupo
internal/adapters/httpapi/client.go    // SigV4, timeouts, Idempotency-Key
internal/adapters/dynamo/executions.go
internal/adapters/s3blob/
cmd/worker/main.go
test/mockapi/                          // mock de la API: latencia, errores y detector de concurrencia por proveedor
```

## Estrategia de test
- **Unit**: máquina de estados (todas las transiciones válidas e inválidas, table-driven); `ExecuteRequest` con fakes.
- **Concurrencia**: el consumer con un fake de SQS que devuelve batches con mensajes del mismo grupo; se verifica el procesamiento secuencial por grupo (`-race`, `-count=50`).
- **Integración**: LocalStack FIFO real + mockapi con un contador `inflight[provider]` y una aserción `max ≤ 1`.
- **Carga**: 1 hora acelerada a 120 req/s con 200 proveedores Zipf; se verifican SC-001…SC-004. Fault injection: 5 % de duplicados, `kill -TERM` aleatorio.

## Riesgos
| Riesgo | Mitigación |
|---|---|
| Paralelizar un batch del mismo grupo dentro del task | Agrupación por `MessageGroupId` + test dedicado |
| Agotar la cuota de Lambda de la cuenta (C3) | Presupuesto global fail-fast + alarma sobre `ConcurrentExecutions` de la cuenta |
| Visibility timeout < duración de la llamada | Heartbeat + VT = 60 s |

## Complexity Tracking
| Desvío | Por qué |
|---|---|
| Lease además de FIFO | Defensa en profundidad ante el vencimiento del VT (invariante IV.1) |
