# Runbook: Proveedor excede capacidad secuencial diaria (`ProviderOverCapacity > 0`)

- **Alarma**: `tapi-provider-over-capacity` (`ProviderOverCapacity{provider} > 0` emitida por `cmd/reconciler`)
- **Severidad**: P2
- **Contexto (001 US3 / Invariante IV.1)**: Bajo concurrencia estricta = 1 por proveedor, un proveedor con $k$ registros diarios y latencia p95 $L$ necesita $k \times L$ segundos/día. Al superar el 70 % de 86.400 s (60.480 s ≈ 151.200 registros a 400 ms p95), el proveedor corre riesgo de no completar su cuota en 24 h.

## 1. Diagnóstico rápido
1. Buscar el log `provider exceeds daily sequential capacity budget` de `reconciler` para ver `provider`, `count`, `estimated_time` y `budget`.
2. Verificar la latencia real `ProviderLatency` p95 de ese proveedor en CloudWatch.
3. Confirmar si hubo un alta masiva legítima o registros duplicados/obsoletos (`enabled=true` que deberían deshabilitarse).

## 2. Mitigación y opciones de negocio
1. **Optimización de latencia**: coordinar con el equipo del proveedor/API interna para reducir p95 (p. ej. keep-alive, caché de sesión, evitar cold starts con Provisioned Concurrency).
2. **Limpieza del catálogo**: deshabilitar adhesiones inactivas (`enabled=false`).
3. **Relajar exclusividad (decisión de producto)**: si el proveedor admite concurrencia $C > 1$, particionar `MessageGroupId = provider#(hash % C)` (requiere ADR y aprobación explícita de negocio).

## 3. Cierre
- Tiempo estimado diario $k \times L_{\text{p95}} \le 70\,\%$ de 24 h y `ProviderOverCapacity{provider} == 0`.
