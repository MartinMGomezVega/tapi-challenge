## Resumen
<!-- Describí brevemente el cambio y el enlace a la tarea en specs/00X-*/tasks.md -->
- **Feature / Tarea**: `00X-T00Y`
- **FR / SC cubierto**:

## Checklist de Constitución (`specs/constitution.md`)
- [ ] **I. Spec primero**: El cambio corresponde a un `spec.md` / `plan.md` / `tasks.md` aprobado.
- [ ] **II. TDD estricto**: Se escribió el test antes del código de producción; `make test` y `make cover` (≥ 90 % en `domain`/`app`) pasan con `-race`.
- [ ] **III. Clean Architecture**: `domain` sólo importa stdlib; `app` no importa AWS SDK ni `net/http`.
- [ ] **IV. Invariantes**: Exclusividad por proveedor, idempotencia `request_id#run_date`, distribución uniforme y sin fallos silenciosos preservados.
- [ ] **V. Escalabilidad**: Sin `Scan` completo en el camino crítico; memoria acotada.
- [ ] **VI. Observabilidad**: Logs JSON estructurados sin PII (`body`/respuestas nunca logueados); métricas EMF y trazas preservadas.
- [ ] **VII. Seguridad**: Mínimo privilegio IAM, KMS, TLS, sin secretos ni NAT.
- [ ] **VIII. IaC**: Todo recurso AWS definido en Terraform (`fmt`, `validate`, `tflint`, `checkov`, `terraform test`).

## Evidencia de pruebas
- `make test`:
- `make cover`:
- `make it`:
- `make lint`:
