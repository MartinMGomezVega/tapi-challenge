# Feature Specification: Infraestructura, CI/CD y ciclo de vida de desarrollo

**Feature Branch**: `006-infraestructura-y-delivery`
**Creado**: 2026-11-20 · **Estado**: Draft
**Trazabilidad**: T1 (todo el stack de AWS), C2 (misma región y cuenta), C3 (cuota de Lambda compartida), criterios de evaluación "Ciclo de vida de desarrollo" y "Plan de desarrollo"

## Contexto
La solución debe poder desplegarse de forma reproducible, segura y con entornos aislados. El evaluador pide explícitamente el **ciclo de vida de desarrollo**: esta feature lo hace concreto y verificable.

## User Scenarios & Testing

### User Story 1 — Entorno local en un comando (P1) 🎯
**Given** un clon del repo, **When** corro `make up && make demo`, **Then** se levantan LocalStack (DynamoDB, SQS, S3), mockapi, dispatcher y worker; se siembran 10k registros; y una simulación acelerada de 1 día termina mostrando cobertura 100 % y `max_inflight/proveedor = 1`.

### User Story 2 — Infraestructura como código (P1) 🎯
**Acceptance Scenarios**:
1. **Given** las variables de `dev`, **When** corro `terraform apply`, **Then** se crean VPC (3 AZ, subnets privadas, **sin NAT**), VPC endpoints (DynamoDB gateway, S3 gateway; SQS, ECR api/dkr, Logs, STS, execute-api, X-Ray de tipo interface), tablas, colas, ECS cluster y servicios, roles IAM, KMS, alarmas y dashboard.
2. **Given** un PR que cambia la infraestructura, **When** corre la CI, **Then** se publica `terraform plan` como comentario y pasan `tflint` y `checkov`.

### User Story 3 — CI/CD (P1)
**Acceptance Scenarios**:
1. **Given** un PR, **When** corre la CI, **Then** se ejecutan: `gofmt`, `golangci-lint`, `go vet`, `staticcheck`, `govulncheck`, tests unitarios con `-race` y cobertura ≥ 90 % en `domain`/`app`, tests de integración con LocalStack, build de imágenes y scan con Trivy.
2. **Given** un merge a `main`, **When** corre el CD, **Then** se construye una imagen inmutable (tag = SHA), se despliega a `dev` con smoke E2E y, con aprobación manual, a `staging` → `prod`.
3. **Given** un deploy de ECS que falla en los health checks, **When** ocurre, **Then** el *deployment circuit breaker* hace rollback automático.
4. La autenticación de GitHub Actions a AWS es por **OIDC** (sin access keys).

### User Story 4 — Seguridad y cuotas compartidas (P1)
**Acceptance Scenarios**:
1. Cada servicio tiene un rol con mínimo privilegio (el dispatcher no puede invocar la API; el worker no puede escribir en `coordination`).
2. DynamoDB, SQS y S3 están cifrados con una CMK de KMS; S3 tiene Block Public Access y TLS obligatorio.
3. El worker invoca la API sólo por el VPC endpoint `execute-api`, con una resource policy de API Gateway que lo restringe a `aws:SourceVpce`.
4. El presupuesto `LAMBDA_BUDGET` se deriva en Terraform de la cuota de concurrencia de la cuenta (data source de Service Quotas) y se pasa al worker.

### Edge Cases
- Un deploy del dispatcher durante un slot → el lease y el watermark garantizan continuidad (feature 002).
- Un rollback del schema de DynamoDB → los cambios son sólo aditivos (expand/contract).
- Un aumento de cuota de Lambda requerido para el escenario de 100M → se documenta como prerequisito operativo.

## Requirements
- **FR-001**: Terraform ≥ 1.9 con módulos `network`, `data`, `messaging`, `compute`, `observability`, `security`; estado remoto en S3 con locking nativo.
- **FR-002**: Entornos `dev`, `staging`, `prod` por workspaces o directorios, que difieren sólo en `tfvars`.
- **FR-003**: ECS Fargate ARM64 (Graviton); autoscaling del worker por backlog por task (custom metric) y edad del mensaje más viejo; dispatcher fijo en 2 réplicas.
- **FR-004**: GitHub Actions: `ci.yml` (PR), `cd.yml` (main), `nightly.yml` (test de carga acelerado + `govulncheck`).
- **FR-005**: Branching trunk-based, Conventional Commits, PRs chicos (1 tarea o un grupo de `tasks.md`), revisión obligatoria, CODEOWNERS.
- **FR-006**: `docker-compose.yml` para el entorno local y la demo reproducible.

## Success Criteria
- **SC-001**: `make up && make demo` funciona en una máquina limpia con Docker en < 5 min.
- **SC-002**: el pipeline de PR tarda < 10 min.
- **SC-003**: `checkov` sin hallazgos HIGH; Trivy sin CVEs CRITICAL.
- **SC-004**: de cero a `dev` desplegado con un `terraform apply` + un pipeline.
