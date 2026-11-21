# Progreso de ejecución autónoma

- **Feature actual**: `003-worker-ejecucion` (por iniciar desde `main` tras merge de `002-dispatcher`)
- **Última tarea terminada**: `002-dispatcher` completa (`T001`–`T027`, conexión real de `ImmediateDispatcher` FR-005b, `checkpoint-002.md` generado)
- **Próxima tarea**: Merge `--no-ff` de `002-dispatcher` a `main` → crear branch `003-worker-ejecucion` → `003-T001` (tests table-driven de `ExecutionStatus`)
- **Bloqueos**: Ninguno
- **Pendientes diferidos entre features**:
  - `001-T025`: integrar `app/capacity` en `reconciler` (feature `005`).
  - `006-T004`: `make demo` E2E (feature `006`).
