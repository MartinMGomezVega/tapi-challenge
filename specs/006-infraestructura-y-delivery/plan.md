# Implementation Plan: Infraestructura, CI/CD y ciclo de vida

**Branch**: `006-infraestructura-y-delivery` · **Spec**: [spec.md](./spec.md)

## Summary
Terraform modular para todo el stack AWS en una región y una cuenta, con red privada sin NAT (sólo VPC endpoints). docker-compose para el entorno local. GitHub Actions con OIDC para CI/CD con quality gates. Esta feature materializa el **ciclo de vida de desarrollo** que pide el enunciado.

## Technical Context
Terraform ≥ 1.9 · AWS provider 5.x/6.x · tflint · checkov · Docker / docker-compose · LocalStack · GitHub Actions · Trivy · ECR.

## Constitution Check
| Principio | ✅ | Cómo |
|---|---|---|
| VII Seguridad | ✅ | Sin NAT, VPC endpoints, KMS, IAM mínimo, OIDC |
| VIII IaC | ✅ | 100 % Terraform |
| II TDD | ✅ | Gates de cobertura en la CI; `terraform test` para los módulos críticos |
| IX Simplicidad | ✅ | 2 servicios + 1 job; sin Kubernetes ni service mesh |

## Ciclo de vida de desarrollo (SDLC)

```
 Idea ─► spec.md ─► plan.md ─► tasks.md ─► branch ─► TDD (red/green/refactor) ─► PR
   ▲       (SDD: revisión            (1 PR ≈ 1-3 tareas)                          │
   │        de negocio)                                                           ▼
   │                                                    CI: lint, vet, staticcheck, govulncheck,
   │                                                    unit -race + cobertura, integración LocalStack,
   │                                                    build + Trivy, terraform plan + checkov
   │                                                                              │
   │                                                                     Code review + merge
   │                                                                              ▼
   │                                    CD: imagen inmutable (SHA) → dev (smoke E2E) → staging
   │                                    (test de carga nightly) → aprobación → prod (rolling + circuit breaker)
   │                                                                              │
   └──────── retro / post-mortem ◄── alarmas + runbooks + reporte de cobertura ◄──┘
```

| Etapa | Artefacto | Gate de salida |
|---|---|---|
| Especificación | `spec.md` | User stories con criterios de aceptación; FR trazados al enunciado |
| Diseño | `plan.md` + ADR | Constitution Check aprobado |
| Planificación | `tasks.md` | Tests antes que la implementación; dependencias explícitas |
| Construcción | código + tests | CI verde, cobertura ≥ 90 % en dominio |
| Verificación | evidencia en `docs/evidence/` | SC del spec cumplidos |
| Despliegue | pipeline | Smoke E2E en dev; aprobación para prod |
| Operación | dashboard, alarmas, runbooks | Cobertura diaria ≥ 99,9 % |

## Diseño de infraestructura
```
deploy/terraform/
  modules/
    network/        VPC 3 AZ, subnets privadas, SG, VPC endpoints (gateway: dynamodb, s3; interface: sqs, ecr.api, ecr.dkr, logs, sts, execute-api, xray, monitoring)
    data/           tablas requests, executions, coordination (on-demand, PITR, TTL, KMS), bucket S3 de resultados y reportes
    messaging/      SQS FIFO high-throughput (deduplication_scope=messageGroup, fifo_throughput_limit=perMessageGroupId) + DLQ FIFO
    compute/        ECS cluster, task defs ARM64, servicios dispatcher (2) y worker (autoscaling), task reconciler + EventBridge Scheduler, ECR
    security/       KMS CMK, roles IAM por servicio, permisos OIDC de GitHub
    observability/  log groups, alarmas, SNS, dashboard
  envs/{dev,staging,prod}/main.tf + terraform.tfvars
```
- **Sin NAT**: todo el tráfico va por VPC endpoints. Es más seguro y más barato, y es posible porque todos los servicios están en la misma región y cuenta (C2).
- **Autoscaling del worker**: target tracking sobre una métrica `BacklogPerTask = visibles / running tasks`, con target = `k × 60 s / L` (lo que un task procesa en 1 min), más un step scaling por `AgeOfOldestMessage`.
- **Límite superior** `max_capacity` derivado de `LAMBDA_BUDGET` (C3).

## Entorno local
`docker-compose.yml`: `localstack`, `init` (crea tablas y colas), `mockapi`, `dispatcher`, `worker`, `seed`. El reloj se acelera con la variable `CLOCK_SPEEDUP=720` (1 día = 2 min) mediante `internal/platform/clock` (alineado con `002/tasks.md` T004).

## Estrategia de test
- `terraform validate`, `tflint`, `checkov` y `terraform test` para los módulos `data` y `messaging` (verifican FIFO, DLQ y PITR).
- Smoke E2E post-deploy: sembrar 100 registros en slots próximos y verificar `SUCCEEDED` en < 3 min.

## Riesgos
| Riesgo | Mitigación |
|---|---|
| Endpoints interface tienen costo fijo por AZ | Aceptado: más barato que NAT + data processing; se documenta en costos |
| Drift de infraestructura | `terraform plan` nightly con alerta si hay drift |

## Complexity Tracking
Ninguna violación.
