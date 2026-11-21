# Progreso de ejecución autónoma

- **Feature actual**: `001-catalogo-y-slots` (Fase 5 US3 + Fase 6 Polish)
- **Última tarea terminada**: `001-T022` (`f1d5f56`)
- **Próxima tarea**: `001-T023` (test de capacidad por proveedor) → `001-T024` → Rule G (T020 escala memoria 20k vs 80k) → `001-T026` → `001-T027` → `checkpoint-001.md` → merge `--no-ff` a `main`
- **Bloqueos**: Ninguno
- **Pendientes diferidos entre features**:
  - `001-T025`: integrar `app/capacity` en `reconciler` (se ejecuta en feature `005`).
  - `006-T004`: `make demo` E2E (se ejecuta en feature `006` cuando existan `mockapi`, `dispatcher`, `worker`).
