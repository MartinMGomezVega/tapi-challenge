# Tasks: Infraestructura, CI/CD y ciclo de vida

**Input**: [spec.md](./spec.md), [plan.md](./plan.md)

## Fase 1: US1 — Entorno local (P1) 🎯
- [x] T001 [US1] `docker-compose.yml` (localstack, init, mockapi, dispatcher, worker, seed)
- [x] T002 [US1] Script `init` idempotente: tablas, GSIs, FIFO + DLQ, bucket
- [x] T003 [US1] Reloj acelerado (`CLOCK_SPEEDUP`) en `internal/platform/clock` con test
- [x] T004 [US1] `make up`, `make demo` (seed → simula 1 día → imprime cobertura y `max_inflight`)
- [x] T005 [P] [US1] README "Quick start" verificado en una máquina limpia (SC-001)

## Fase 2: US2 — Terraform (P1) 🎯
- [x] T006 [US2] Backend remoto S3 con locking nativo (`use_lockfile = true`) + estructura `modules/` y `envs/`
- [x] T007 [P] [US2] Módulo `network` (VPC 3 AZ, sin NAT, endpoints)
- [x] T008 [P] [US2] Módulo `data` + `terraform test` con mocks (PITR, TTL, KMS, GSIs)
- [x] T009 [P] [US2] Módulo `messaging` + `terraform test` con mocks (FIFO high-throughput, DLQ, `maxReceiveCount=6`)
- [x] T010 [US2] Módulo `security` (KMS, roles por servicio con mínimo privilegio, OIDC)
- [x] T011 [US2] Módulo `compute` (ECS, task defs ARM64, servicios, autoscaling, reconciler + Scheduler, ECR)
- [x] T012 [US2] Módulo `observability` (enlaza con la feature 005)
- [x] T013 [US2] `envs/{dev,staging,prod}` validados (`terraform fmt/validate/test`, `tflint`, `checkov`; sin `apply` externo por Regla E)

## Fase 3: US3 — CI/CD (P1)
- [x] T014 [US3] `.github/workflows/ci.yml`: lint, vet, staticcheck, govulncheck, unit `-race` + cobertura, integración (LocalStack), build, Trivy, `terraform validate/test` + tflint + checkov (validado con `actionlint`)
- [x] T015 [US3] `cd.yml`: OIDC → ECR push (tag SHA) → deploy dev → smoke E2E → aprobación → staging → prod (validado con `actionlint`)
- [x] T016 [P] [US3] `nightly.yml`: test de carga acelerado, detección de drift, govulncheck (validado con `actionlint`)
- [x] T017 [P] [US3] CODEOWNERS, plantilla de PR (checklist de constitución + link a la tarea), Conventional Commits (`.commitlintrc.json`)
- [x] T018 [US3] ECS deployment circuit breaker con rollback configurado en `dispatcher` y `worker`

## Fase 4: US4 — Seguridad y cuotas (P1)
- [x] T019 [US4] Resource policy de API Gateway restringida a `aws:SourceVpce` (`modules/security`)
- [x] T020 [US4] `LAMBDA_BUDGET` derivado de Service Quotas (`L-B99A9384`) → variable del worker + `max_capacity` (verificado en `stack.tftest.hcl`)
- [x] T021 [P] [US4] Review de IAM y seguridad IaC con Checkov (`306/306` checks PASS, `0` hallazgos)

## Fase 5: Documentación del challenge
- [x] T022 Diagrama final exportado (SVG) en `docs/architecture-diagram.svg`
- [x] T023 [P] Estimación de costos mensual (1M, 10M, 100M) en `docs/costs.md`
- [x] T024 [P] Análisis de modos de falla (FMEA) en `docs/failure-modes.md`
- [x] T025 Revisión final: matriz de trazabilidad (`docs/architecture.md` §1) verificada con evidencia en `docs/evidence/final-report.md`

## Dependencias
Fase 1 se hace al inicio del proyecto (junto con 001-T001). Fases 2–4 en paralelo con 003–005. Fase 5 al final.
