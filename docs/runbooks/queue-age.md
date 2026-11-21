# Runbook: Envejecimiento de cola (`ApproximateAgeOfOldestMessage > 30 min`)

- **Alarma**: `tapi-queue-age-high` (`ApproximateAgeOfOldestMessage > 1800 s` en `tapi-jobs.fifo`)
- **Severidad**: P2 (escala a P1 si supera 2 h)
- **Impacto**: Backlog creciente en la cola FIFO; posible incumplimiento de ejecución dentro del día `D`.

## 1. Diagnóstico rápido
1. Correlacionar con `BreakerOpen{provider}` y `ProviderLatency` p95:
   - **Un solo proveedor lento/caído**: el grupo FIFO de ese proveedor acumula antigüedad sin bloquear al resto. Verificar `ApproximateNumberOfMessagesVisible` vs `BreakerOpen`.
   - **Múltiples proveedores atrasados**: falta capacidad de workers o la cuota global de Lambda está frenando el autoscaling.
2. Revisar `RunningTaskCount` vs `MAX_TASKS` del servicio ECS `worker`.
3. Revisar [lambda-quota.md](./lambda-quota.md) antes de subir `MAX_TASKS`.

## 2. Mitigación
- Si hay margen en la cuota de Lambda (`ConcurrentExecutions < 70 %`), aumentar `MAX_TASKS` respetando `INFLIGHT_PER_TASK × MAX_TASKS ≤ LAMBDA_BUDGET / 2`.
- Si un proveedor específico está degradado con 5xx/429, dejar actuar al circuit breaker y configurar `PROVIDER_RPS` si corresponde; comunicar al equipo de integraciones.

## 3. Cierre
- `ApproximateAgeOfOldestMessage < 300 s` durante 15 min continuos.
