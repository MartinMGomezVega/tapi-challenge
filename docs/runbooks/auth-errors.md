# Runbook: Errores de autenticación (`ExecutionFailed{class=AUTH} > 0`)

- **Alarma**: `tapi-auth-errors` (`ExecutionFailed` con dimensión `class=AUTH` > 0 en 5 min)
- **Severidad**: P1
- **Impacto**: HTTP 401/403 contra la API interna. No se reintenta (fallo permanente) y suele indicar un problema de configuración IAM/SigV4 o Resource Policy de API Gateway.

## 1. Diagnóstico rápido
1. Filtrar logs del worker por `error_class = "AUTH"`:
   ```sql
   fields @timestamp, provider, request_id, http_status, error_message
   | filter service = "worker" and error_class = "AUTH"
   | stats count() by provider, http_status
   ```
2. Verificar:
   - Rol IAM del task `worker`: acción `execute-api:Invoke` sobre el ARN correcto del API Gateway privado.
   - Resource Policy del API Gateway: condición `aws:SourceVpce` coincide con el VPC Endpoint `execute-api` activo.
   - Sincronización de reloj del contenedor (SigV4 rechaza skew > 5 min; en Fargate es manejado por Amazon Time Sync).

## 2. Mitigación
1. Corregir la política IAM o Resource Policy vía Terraform (nunca manual en consola) y desplegar.
2. Si el error afectó registros del día que quedaron en `FAILED_PERMANENT` con `error_class=AUTH`, coordinar un re-despacho controlado una vez validado el acceso con un smoke request.

## 3. Cierre
- `ExecutionFailed{class=AUTH} == 0` durante 15 min continuos.
