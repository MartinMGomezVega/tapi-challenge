# Runbook: Mensajes en la DLQ (`DLQVisibleMessages > 0`)

- **Alarma**: `tapi-dlq-messages-visible` (`ApproximateNumberOfMessagesVisible > 0` durante 1 minuto en `tapi-jobs-dlq.fifo`)
- **Severidad**: P2 (P1 si afecta > 5 % del volumen diario o múltiples proveedores)
- **Propósito**: Identificar la causa de mensajes que agotaron sus 6 recepciones (`FAILED_EXHAUSTED`), remediar la causa raíz y ejecutar `redrive` seguro.

## 1. Diagnóstico rápido (< 5 min)
1. Inspeccionar la profundidad de la DLQ y las métricas por proveedor en el dashboard CloudWatch (`ExecutionFailed{provider, class}`, `BreakerOpen{provider}`).
2. Consultar logs estructurados en CloudWatch Logs Insights filtrando por `FAILED_EXHAUSTED` (nunca contiene PII):
   ```sql
   fields @timestamp, provider, request_id, run_date, error_class, http_status, error_message
   | filter msg = "execution retries exhausted, routing to DLQ"
   | stats count() by provider, error_class, http_status
   ```
3. Identificar el patrón dominante:
   - `TRANSIENT_SERVER` (5xx) / `TRANSIENT_NETWORK`: caída o degradación prolongada del proveedor o de la API interna.
   - `THROTTLED` (429 / `TooManyRequestsException`): saturación de cuota Lambda (ver [lambda-quota.md](./lambda-quota.md)) o rate limit del proveedor.
   - `UNKNOWN`: error inesperado; revisar el `error_message` sanitizado.

## 2. Mitigación y verificación previa al redrive
1. Confirmar que el proveedor o la API interna volvió a responder 2xx (`BreakerOpen{provider} == 0`).
2. Si el proveedor tiene límite de tasa reducido, ajustar `PROVIDER_RPS` en la configuración del worker antes de re-inyectar volumen.
3. Ejecutar primero en modo `--dry-run` para cuantificar los mensajes afectados:
   ```bash
   redrive --provider <proveedor> --since <YYYY-MM-DD> --dry-run
   ```

## 3. Ejecución del redrive
1. Re-encolar preservando `MessageGroupId=provider`, `MessageDeduplicationId=request_id#run_date` y el contador histórico `attempts` (transiciona `FAILED_EXHAUSTED → ENQUEUED`):
   ```bash
   redrive --provider <proveedor> --since <YYYY-MM-DD> --max 500
   ```
2. Monitorear durante 5 minutos:
   - `ExecutionSucceeded{provider}` aumenta y `DLQVisibleMessages` decrece.
   - `BreakerOpen{provider}` permanece en 0.
3. Repetir sin `--max` para drenar el remanente.

## 4. Cierre
- Confirmar `ApproximateNumberOfMessagesVisible == 0` en `tapi-jobs-dlq.fifo`.
- Verificar que la reconciliación diaria (`reconciler --date <YYYY-MM-DD>`) reporta cobertura ≥ 99,9 % sin ejecuciones estancadas.
