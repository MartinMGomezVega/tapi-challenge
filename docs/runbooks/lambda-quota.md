# Runbook: Presión sobre la cuota de Lambda (`ConcurrentExecutions > 80 %`)

- **Alarma**: `tapi-lambda-quota-pressure` (`AWS/Lambda ConcurrentExecutions > 0.80 × LAMBDA_BUDGET`)
- **Severidad**: P1
- **Contexto (C3)**: Cada invocación a la API interna dispara **2 Lambdas encadenadas** en la misma cuenta y región. Si se agota la cuota regional, se degrada toda la cuenta.

## 1. Diagnóstico rápido
1. Revisar en CloudWatch `AWS/Lambda`: `ConcurrentExecutions` y `Throttles` por función.
2. Revisar en el dashboard de Tapi: `ExecutionFailed{class=THROTTLED}` y `RunningTaskCount` del `worker`.
3. Verificar que `INFLIGHT_PER_TASK × RunningTaskCount × 2` esté dentro de `LAMBDA_BUDGET`.

## 2. Mitigación inmediata
1. Reducir `max_capacity` del autoscaling de ECS `worker` (o `INFLIGHT_PER_TASK`) para liberar concurrencia de Lambda inmediatamente.
2. Si la degradación proviene de latencia alta en un proveedor (colas largas reteniendo Lambdas), configurar `PROVIDER_RPS` reducido para ese proveedor.
3. Solicitar incremento de cuota `ConcurrentExecutions` en AWS Service Quotas (y actualizar `lambda_budget` en Terraform una vez aprobado).

## 3. Cierre
- `ConcurrentExecutions < 70 %` de la cuota regional y `AWS/Lambda Throttles == 0` durante 15 min.
