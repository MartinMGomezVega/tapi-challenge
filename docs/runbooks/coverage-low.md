# Runbook: Cobertura diaria baja (`DailyCoverage < 99,9 %`)

- **Alarma**: `tapi-daily-coverage-low` (`DailyCoverage < 99.9` % emitida por `cmd/reconciler`)
- **Severidad**: P1
- **Impacto**: Violación potencial del requisito R1 ("cada registro debe ejecutarse todos los días").

## 1. Diagnóstico rápido
1. Descargar el reporte diario generado en S3:
   - `s3://<RESULTS_BUCKET>/reports/run_date=<YYYY-MM-DD>/summary.json`
   - `s3://<RESULTS_BUCKET>/reports/run_date=<YYYY-MM-DD>/summary.csv`
2. Identificar si el faltante proviene de:
   - `missing > 0`: el dispatcher perdió slots o hubo errores no recuperados.
   - `stuck > 0`: ejecuciones atascadas en `PENDING`/`ENQUEUED`/`IN_PROGRESS` > 2 h.
   - `in_flight > 0`: reintentos todavía activos después de medianoche.
   - Proveedores específicos con alta tasa de `FAILED_EXHAUSTED` (ver [dlq.md](./dlq.md)).

## 2. Mitigación
1. Si `reconciler` corrió sin `--redispatch`, ejecutar manualmente con re-despacho idempotente:
   ```bash
   reconciler --date <YYYY-MM-DD> --redispatch=true --parallelism 64
   ```
2. Monitorear el drenaje de la cola FIFO durante 15–30 min.
3. Re-ejecutar `reconciler --date <YYYY-MM-DD> --redispatch=false` para recalcular `DailyCoverage`.

## 3. Cierre
- `DailyCoverage ≥ 99,9 %` (objetivo 100 % tras re-despacho) y `MissingExecutions == 0`.
- Abrir post-mortem si la causa fue una regresión en `dispatcher` o `worker`.
