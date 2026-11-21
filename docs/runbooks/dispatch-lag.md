# Runbook: Atraso en el despacho (`DispatchLagMinutes > 5`)

- **Alarma**: `tapi-dispatch-lag-high` (`DispatchLagMinutes > 5` durante 5 min consecutivos)
- **Severidad**: P1
- **Impacto**: Los registros no están ingresando a la cola SQS FIFO a tiempo; si supera 30 min se acumula deuda de slots para catch-up.

## 1. Diagnóstico rápido (< 5 min)
1. Verificar el estado del servicio ECS `dispatcher` (2 réplicas en 2 AZs): confirmar cuántas tasks están `RUNNING` y pasando `/healthz`.
2. Revisar la métrica `IsLeader`:
   - `Sum(IsLeader) == 0`: ninguna instancia logra adquirir el lease en DynamoDB `coordination`. Revisar throttling o permisos IAM sobre `coordination`.
   - `Sum(IsLeader) == 1` pero `SlotDispatchDuration` > 60 s: cuello de botella en `ListBySlotShard` o `SendMessageBatch`.
3. Consultar logs JSON del dispatcher:
   ```sql
   fields @timestamp, level, msg, slot, run_date, error
   | filter service = "dispatcher" and level = "ERROR"
   | sort @timestamp desc | limit 50
   ```

## 2. Mitigación
- **Task caída / deploy roto**: hacer rollback de la task definition de `dispatcher` o forzar un nuevo despliegue (`aws ecs update-service --force-new-deployment`). El nuevo líder hará catch-up automático desde el watermark.
- **Throttling DynamoDB/SQS**: confirmar modo `PAY_PER_REQUEST` en `requests`/`executions` y cuotas de SQS FIFO high-throughput (`3.000 msg/s` por partición).
- **Slot muy poblado (> 70k/min)**: incrementar `SHARDS` y re-ejecutar `slotter` en ventana de mantenimiento.

## 3. Verificación de cierre
- `DispatchLagMinutes == 0` estable por 10 min.
- `IsLeader == 1` estable sin oscilaciones de liderazgo.
