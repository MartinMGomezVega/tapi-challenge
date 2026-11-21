# Tasks: Infraestructura, CI/CD y ciclo de vida

**Input**: [spec.md](./spec.md), [plan.md](./plan.md)

## Fase 1: US1 — Entorno local (P1) 🎯
- [x] T001 [US1] `docker-compose.yml` (localstack, init, mockapi, dispatcher, worker, seed)
- [x] T002 [US1] Script `init` idempotente: tablas, GSIs, FIFO + DLQ, bucket
- [x] T003 [US1] Reloj acelerado (`CLOCK_SPEEDUP`) en `internal/platform/clock` con test
- [ ] T004 [US1] `make up`, `make demo` (seed 10k → simula 1 día → imprime cobertura y `max_inflight`)
- [ ] T005 [P] [US1] README "Quick start" verificado en una máquina limpia (SC-001)

## Fase 2: US2 — Terraform (P1) 🎯
- [ ] T006 [US2] Backend remoto S3 con locking + estructura `modules/` y `envs/`
- [ ] T007 [P] [US2] Módulo `network` (VPC 3 AZ, sin NAT, endpoints)
- [ ] T008 [P] [US2] Módulo `data` + `terraform test` (PITR, TTL, KMS, GSIs)
- [ ] T009 [P] [US2] Módulo `messaging` + `terraform test` (FIFO high-throughput, DLQ, `maxReceiveCount=6`)
- [ ] T010 [US2] Módulo `security` (KMS, roles por servicio con mínimo privilegio, OIDC)
- [ ] T011 [US2] Módulo `compute` (ECS, task defs ARM64, servicios, autoscaling, reconciler + Scheduler, ECR)
- [ ] T012 [US2] Módulo `observability` (enlaza con la feature 005)
- [ ] T013 [US2] `envs/dev` aplicado de punta a punta (SC-004)

## Fase 3: US3 — CI/CD (P1)
- [ ] T014 [US3] `.github/workflows/ci.yml`: lint, vet, staticcheck, govulncheck, unit `-race` + cobertura, integración (servicio LocalStack), build, Trivy, `terraform plan` + tflint + checkov
- [ ] T015 [US3] `cd.yml`: OIDC → ECR push (tag SHA) → deploy dev → smoke E2E → aprobación → staging → prod
- [ ] T016 [P] [US3] `nightly.yml`: test de carga acelerado, detección de drift, govulncheck
- [ ] T017 [P] [US3] CODEOWNERS, plantilla de PR (checklist de constitución + link a la tarea), Conventional Commits (commitlint)
- [ ] T018 [US3] ECS deployment circuit breaker con rollback (verificado con una imagen rota en dev)

## Fase 4: US4 — Seguridad y cuotas (P1)
- [ ] T019 [US4] Resource policy de API Gateway restringida a `aws:SourceVpce` (o documentar el supuesto S2)
- [ ] T020 [US4] `LAMBDA_BUDGET` derivado de Service Quotas → variable del worker + `max_capacity`
- [ ] T021 [P] [US4] Review de IAM con IAM Access Analyzer; sin hallazgos

## Fase 5: Documentación del challenge
- [ ] T022 Diagrama final exportado (Mermaid → PNG/SVG) en `docs/`
- [ ] T023 [P] Estimación de costos mensual (1M, 10M, 100M) en `docs/costs.md`
- [ ] T024 [P] Análisis de modos de falla (FMEA) en `docs/failure-modes.md`
- [ ] T025 Revisión final: la matriz de trazabilidad (`docs/architecture.md` §1) con todos los ítems verificados con evidencia

## Dependencias
Fase 1 se hace al inicio del proyecto (junto con 001-T001). Fases 2–4 en paralelo con 003–005. Fase 5 al final.
