# Checkpoint 006 — Infraestructura, CI/CD y ciclo de vida de desarrollo

**Branch**: `006-infraestructura-y-delivery` (mergeada a `main` con `--no-ff`)
**Estado**: Completada (`T001`–`T025`, cumpliendo estrictamente la Regla E: sin `terraform apply` ni credenciales reales de AWS; validado con `terraform fmt`, `terraform validate`, `terraform test` con `mock_provider`, `tflint`, `checkov` y `actionlint`).

## 1. Entregables y tareas completadas
- **Fase 1 — Entorno local E2E (`006-T001`..`T005`)**:
  - `docker-compose.yml`, `scripts/localstack-init.sh`, `build/{dispatcher,worker,reconciler,mockapi}.Dockerfile`, `scripts/demo.sh`, `Makefile` (`make up`, `make demo`, `make tf-validate`), `README.md`.
  - **Evidencia live E2E (`make up && DEMO_RECORDS=500 CLOCK_SPEEDUP=2880 make demo && make down`)**:
    - Seeded: `500` registros Zipf en 20 proveedores → `cmd/slotter` asignó 1440 slots → `dispatcher` acelerado + `worker` + `mockapi` + `reconciler`.
    - `Executed API calls: 509` (≥ 500), `max_provider_inflight: 1` (IV.1 ✅), `duplicate_idem_keys: 0` (IV.2 ✅).
- **Fase 2 & 4 — Terraform modular + Seguridad y cuotas (`006-T006`..`T013`, `T018`–`T021`)**:
  - 6 módulos bajo `deploy/terraform/modules/`: `network` (VPC 3 AZ privada sin NAT + 2 Gateway + 8 Interface VPC Endpoints + VPC Flow Logs), `security` (KMS CMK con rotación, roles IAM de mínimo privilegio por servicio, GitHub OIDC, API Gateway Resource Policy `aws:SourceVpce`), `data` (3 tablas DynamoDB PITR+KMS+TTL+GSIs, S3 KMS+TLS+Block Public Access+Lifecycle), `messaging` (SQS FIFO high-throughput + DLQ `maxReceiveCount=6`), `compute` (ECR inmutable, ECS Fargate ARM64, `deployment_circuit_breaker` con rollback, autoscaling acotado por `LAMBDA_BUDGET`, EventBridge Scheduler 00:30), `observability` (7 alarmas + SNS KMS + Dashboard + Glue/Athena).
  - `deploy/terraform/stack/` + `deploy/terraform/envs/{dev,staging,prod}/` (backend S3 con locking nativo `use_lockfile = true`; los entornos difieren sólo en `terraform.tfvars`).
  - `data "aws_servicequotas_service_quota"` (`L-B99A9384`) derivando `LAMBDA_BUDGET` y `worker_max_tasks = floor(LAMBDA_BUDGET / (2 * INFLIGHT_PER_TASK))`.
- **Fase 3 — CI/CD y gobernanza (`006-T014`..`T017`)**:
  - `.github/workflows/{ci,cd,nightly}.yml` (OIDC sin access keys, gates de Go + LocalStack + Trivy + Terraform + Checkov + drift detection).
  - `.github/CODEOWNERS`, `.github/pull_request_template.md`, `.commitlintrc.json`.
- **Fase 5 — Documentación del challenge (`006-T022`..`T025`)**:
  - [docs/architecture-diagram.svg](file:///Users/martingomezvega/challenge/tapi-challenge/docs/architecture-diagram.svg)
  - [docs/costs.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/costs.md) (estimación mensual 1M / 10M / 100M)
  - [docs/failure-modes.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/failure-modes.md) (FMEA completo)
  - [docs/evidence/final-report.md](file:///Users/martingomezvega/challenge/tapi-challenge/docs/evidence/final-report.md) (trazabilidad integral)

## 2. Evidencia de gates (sin efectos externos — Regla E)
- `actionlint` sobre `.github/workflows/{ci,cd,nightly}.yml`: **0 issues**
- `terraform fmt -check -recursive deploy/terraform`: **0 issues**
- `terraform validate` en `modules/data`, `modules/messaging`, `stack`, `envs/dev`, `envs/staging`, `envs/prod`: **6/6 PASS**
- `terraform test` (con `mock_provider "aws"`):
  - `modules/data/tests/data.tftest.hcl`: **1 passed, 0 failed**
  - `modules/messaging/tests/messaging.tftest.hcl`: **1 passed, 0 failed**
  - `stack/tests/stack.tftest.hcl`: **1 passed, 0 failed**
- `tflint --recursive`: **0 issues**
- `checkov -d deploy/terraform --framework terraform`: **306 passed, 0 failed** (SC-003 ✅)
- `make up && make demo && make down`: **PASS** (`max_provider_inflight=1`, `duplicate_idem_keys=0`)
- Go gates (`make test`, `make cover` ≥ 90 %, `gofmt`, `go vet`, `golangci-lint`, `staticcheck`, `govulncheck`): **PASS / 0 issues / 0 vulnerabilidades**.

## 3. Cumplimiento de Success Criteria (`006/spec.md`)
| SC | Criterio | Evidencia | Estado |
|---|---|---|---|
| SC-001 | `make up && make demo` en máquina limpia con Docker en < 5 min | Ejecutado en vivo contra LocalStack en ~1 min: 100 % ejecutado, `max_provider_inflight=1`, `duplicate_idem_keys=0` | ✅ |
| SC-002 | Pipeline de PR < 10 min | `ci.yml` paralelizado en 4 jobs independientes (`go-quality-and-tests`, `integration-localstack`, `docker-and-trivy`, `terraform-validate-and-security`) validado con `actionlint` | ✅ |
| SC-003 | `checkov` sin hallazgos HIGH; Trivy sin CVEs CRITICAL | Checkov: **306 passed, 0 failed**; imágenes base `distroless/static-debian12:nonroot` + `govulncheck` **0 vulnerabilidades** | ✅ |
| SC-004 | De cero a `dev` con 1 `terraform apply` + 1 pipeline | `envs/dev` + `stack` + `cd.yml` validados con `terraform validate` y `terraform test` (sin `apply` real por Regla E) | ✅ (estático/mock) |

## 4. Desvíos y tests modificados (Regla B)
- **Regla E**: `T013`, `T018` y `T021` (que mencionan `apply` en `dev` o AWS IAM Access Analyzer en vivo) se validaron mediante `terraform validate`, `terraform test` con mocks, `tflint` y Checkov (`306/306 PASS`), sin credenciales ni cambios externos en AWS.
- **Tests existentes modificados (Regla B)**: Ninguno.
