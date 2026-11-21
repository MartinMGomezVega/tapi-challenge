# Estimación de Costos Mensuales AWS — 1M / 10M / 100M registros (`006-T023`)

**Región de referencia**: `us-east-1` · **Arquitectura**: ECS Fargate ARM64 (Graviton) + DynamoDB On-Demand + SQS FIFO High-Throughput + S3 + VPC Endpoints (sin NAT Gateway).
**Supuesto de negocio**: 1 consulta diaria por registro = 30 ejecuciones/mes por registro; retención 90 días en DynamoDB (`ttl`); 1 % de respuestas > 350 KB en S3; 5 % de reintentos.

## 1. Resumen mensual estimado (USD / mes)

| Componente | 1M registros (30M/mes) | 10M registros (300M/mes) | 100M registros (3.000M/mes) | Notas de cálculo |
|---|---:|---:|---:|---|
| **DynamoDB — `requests` + `executions` + `coordination` (WRU/RRU)** | \$82 | \$815 | \$8.140 | On-Demand: ~4 WRU + ~4 RRU/ejecución (CreatePending, MarkEnqueued, Acquire, Complete, Reconciler BatchGet); a 100M conviene Reserved Capacity (~50 % ahorro) |
| **DynamoDB — Almacenamiento + PITR (90d TTL)** | \$38 | \$380 | \$3.800 | ~1 KB/execution × 90 días + PITR (\$0,20/GB-mes) |
| **SQS FIFO High-Throughput** | \$48 | \$480 | \$4.800 | \$0,50/M requests FIFO (~3,2 API calls/msg: SendBatch 0,1 + Receive 0,1 + Delete 1 + retries/heartbeats) |
| **ECS Fargate ARM64 (`dispatcher` ×2 + `worker` + `reconciler`)** | \$64 | \$215 | \$1.480 | ARM64 Graviton (~20 % ahorro vs x86); 1M: 2×disp + 2–4 workers; 10M: 8–16 workers; 100M: 48–96 workers |
| **VPC Endpoints (8 Interface × 3 AZs + 2 Gateway gratis)** | \$175 | \$185 | \$265 | 24 ENIs × \$0,01/h × 730 h ≈ \$175 base + \$0,01/GB data; **ahorra NAT Gateway (\$98 base + \$0,045/GB)** |
| **S3 (`tapi-results` + reportes diarios + Athena)** | \$5 | \$28 | \$240 | 1 % respuestas > 350 KB (90d lifecycle) + reportes JSON/CSV |
| **CloudWatch Logs + EMF Metrics + X-Ray + KMS** | \$28 | \$120 | \$650 | EMF evita PutMetricData; retención 30d; muestreo X-Ray 5 % a escala |
| **TOTAL ESTIMADO (On-Demand)** | **\$440 / mes** | **\$2.223 / mes** | **\$19.375 / mes** | |
| **Costo por cada 1.000 consultas** | **\$0,0147** | **\$0,0074** | **\$0,0065** | Economía de escala al amortizar costos fijos de red/endpoints |

## 2. Palancas de optimización a escala (10M → 100M)
1. **DynamoDB Provisioned + Auto Scaling + Reserved Capacity (1 año)**: reduce el costo de WRU/RRU de **\$8.140 → ~\$3.400/mes** en 100M (**-58 %**).
2. **Fargate Savings Plans (Compute 1 año)**: reduce ECS Fargate ARM64 un **25–35 %**.
3. **Compresión gzip en S3 + TTL 30 días** en `executions` (archivando a S3 Parquet vía Export-to-S3): reduce el almacenamiento DynamoDB de **\$3.800 → ~\$1.300/mes** en 100M.
4. **Con optimizaciones aplicadas en 100M**: el total baja de **\$19.375 → ~\$11.500/mes** (**\$0,0038 / 1.000 consultas**).

> **Nota (C3)**: El costo de las 2 Lambdas y del API Gateway interno existentes de Tapi no se incluye aquí porque forman parte de la API interna ya operada por Tapi; el presupuesto `LAMBDA_BUDGET` evita incurrir en throttling o sobrecostos por reintentos descontrolados.
