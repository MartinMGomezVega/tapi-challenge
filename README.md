# Tapi — Desafío técnico backend: consultas programadas a proveedores

Sistema que ejecuta **una consulta diaria por cada registro** de una tabla de N registros (1M → N×), **distribuida a lo largo del día**, **sin concurrencia por proveedor**, con **manejo de errores reintentables y no reintentables** y **persistencia de resultados**. Todo en AWS.

**Stack**: Go · DynamoDB · SQS FIFO · ECS Fargate (ARM64) · EventBridge Scheduler · S3 · CloudWatch/X-Ray · Terraform · Docker/LocalStack · GitHub Actions.
**Metodología**: SDD (spec → plan → tasks) + TDD estricto + Clean Architecture.

## Documentos
| Documento | Contenido |
|---|---|
| [Constitución](specs/constitution.md) | Principios no negociables del proyecto |
| [Arquitectura](docs/architecture.md) | Trazabilidad al enunciado, diagramas, capacidad, ADRs, alta disponibilidad y DR, supuestos |
| [006 · plan](specs/006-infraestructura-y-delivery/plan.md) | Ciclo de vida de desarrollo (SDLC) |

## Features (SDD)
| # | Feature | Requisitos del enunciado | Prioridad |
|---|---|---|---|
| 001 | [Catálogo y slots](specs/001-catalogo-y-slots/spec.md) | R1, R2, C1 | P1 |
| 002 | [Dispatcher](specs/002-dispatcher/spec.md) | R1, R2, R5 | P1 |
| 003 | [Worker: ejecución, exclusividad e idempotencia](specs/003-worker-ejecucion/spec.md) | R3, R5, C2, C3 | P1 |
| 004 | [Errores y reintentos](specs/004-errores-y-reintentos/spec.md) | R4, C3 | P1 |
| 005 | [Observabilidad y reconciliación](specs/005-observabilidad-y-reconciliacion/spec.md) | R1, R4 | P1 |
| 006 | [Infraestructura y delivery](specs/006-infraestructura-y-delivery/spec.md) | T1, C2, C3, SDLC | P1 |

## Plan de desarrollo (hitos)
| Hito | Contenido | Entregable demostrable |
|---|---|---|
| **M0 — Fundaciones** (día 1) | 006-Fase 1, 001-Setup, constitución, ADRs | `make up` levanta el entorno local |
| **M1 — Distribución temporal** (días 2–3) | 001 completo, 002 US1–US2 | 1 día simulado: cada registro despachado exactamente 1 vez |
| **M2 — Ejecución segura** (días 4–5) | 003 US1–US4 | **Evidencia**: `max_inflight/proveedor = 1`, 0 duplicados |
| **M3 — Resiliencia** (día 6) | 004 completo, 002 US3–US4, 003 US5 | Proveedor caído sin afectar a los demás; failover de líder |
| **M4 — Operabilidad** (día 7) | 005 completo | Cobertura 100 % con fault injection; dashboard |
| **M5 — Producción** (día 8) | 006 Fases 2–5 | `terraform apply` en dev + CI/CD + costos + FMEA |

Camino crítico: 001-US1 → 002-US1 → 003-US1/US2 → 004 → 005.

## Quick start
```bash
make up      # LocalStack + mockapi + servicios
make demo    # siembra 10k registros, simula 1 día en 2 min y muestra cobertura y concurrencia por proveedor
make test    # unit + race
make it      # integración (LocalStack)
```