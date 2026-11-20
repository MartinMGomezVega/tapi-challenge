# Constitución del proyecto — Tapi Scheduled Provider Queries

> Principios no negociables. Todo `plan.md` debe incluir un **Constitution Check** que verifique cada uno. Una violación sólo se acepta si queda justificada en la sección *Complexity Tracking* del plan.

## I. Spec primero (SDD)
Ningún código se escribe sin un `spec.md` aprobado. El flujo es siempre: `spec.md` → `plan.md` → `tasks.md` → implementación. El spec describe **qué** y **por qué**; el plan, **cómo**. Cada requisito funcional (FR) se traza a un requisito del enunciado y a al menos un test.

## II. Test primero (TDD estricto)
Red → Green → Refactor. No se escribe código de producción sin un test que falle antes. En `tasks.md`, la tarea de test precede siempre a la de implementación. Cobertura mínima del dominio y casos de uso: 90 %. Los adaptadores se prueban con tests de integración contra LocalStack (testcontainers-go).

## III. Clean Architecture (estilo Go)
Regla de dependencias: `domain` ← `app` (casos de uso + puertos) ← `adapters` ← `cmd`. El dominio no importa AWS SDK, `net/http` ni `database/sql`. Las interfaces (puertos) se definen del lado del consumidor, son chicas y se nombran por comportamiento. Nada de capas vacías ni de "frameworks" propios.

## IV. Invariantes del negocio
1. **Exclusividad por proveedor**: nunca hay más de una llamada en vuelo al mismo proveedor.
2. **Una ejecución por registro y por día** (`request_id#run_date`): toda operación es idempotente.
3. **Distribución temporal**: la carga se reparte uniformemente en los 1440 minutos del día.
4. **Ningún fallo se pierde en silencio**: todo termina en `SUCCEEDED`, `FAILED_PERMANENT` o `FAILED_EXHAUSTED`, con métrica y log.

## V. Escalabilidad por diseño
Todo componente debe funcionar con 1M de registros y escalar a N×1M sin cambios de código, sólo de configuración (shards, tasks, cuotas). Prohibido el `Scan` completo de tablas en el camino crítico.

## VI. Observabilidad
Logs estructurados (`log/slog`, JSON) con `request_id`, `provider`, `run_date`, `trace_id`. Trazas OpenTelemetry. Métricas de negocio (ejecuciones por estado, latencia por proveedor, cobertura diaria). Cada alarma tiene un runbook.

## VII. Seguridad y mínimo privilegio
Un rol IAM por servicio con permisos mínimos. Cifrado en reposo (KMS) y en tránsito (TLS). Tráfico privado mediante VPC endpoints, sin salida a internet. Sin secretos en el código ni en variables de entorno planas.

## VIII. Infraestructura como código
Todo recurso AWS se define en Terraform. Nada se crea a mano desde la consola. Los entornos (`dev`, `staging`, `prod`) difieren sólo en variables.

## IX. Simplicidad (YAGNI)
Se elige el servicio administrado más simple que cumpla el invariante. Cada componente nuevo necesita un ADR. Dos microservicios más un job, no más.

## Gobernanza
- Los cambios a esta constitución requieren un ADR y una versión nueva.
- Las PR se revisan contra esta constitución y deben tener la CI en verde (lint, `go vet`, `staticcheck`, `govulncheck`, tests, `terraform validate`/`tflint`/`checkov`).

**Versión**: 1.0.0 · **Ratificada**: 2026-11-20