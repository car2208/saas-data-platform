# Onboarding de un nuevo tenant

Este documento describe el proceso para incorporar un nuevo tenant (unidad de negocio / país) a la plataforma de datos SAAS.

## Prerrequisitos

- Acceso al repositorio `saas-data-platform`.
- Los datos del tenant deben estar disponibles en la capa RAW. Dependiendo de la estrategia de ingesta, los datos pueden llegar en archivos compartidos (cross-tenant) o en landing zones separadas por tenant. Ver `docs/observations.md` (observación #1) para la discusión sobre aislamiento en RAW.

## Paso 1: Crear configuración del tenant

Crear un archivo YAML en `config/tenants/` con el código del tenant en minúscula:

```yaml
# config/tenants/hn.yaml
tenant_id: "hn"
tenant_name: "Honduras"
```

El `tenant_id` debe coincidir con el valor del campo `pais` en los CSVs, normalizado a minúscula.

## Paso 2: Verificar datos de entrada

Confirmar que existen datos para el nuevo tenant en la capa RAW. El pipeline filtra automáticamente por el campo `pais` (normalizado a minúscula) al ingresar a Bronze.

## Paso 3: Ejecutar el pipeline

```bash
# Pipeline completo para el nuevo tenant
python -m saas_pipeline.cli --env dev --tenant hn

# O por capas para validar paso a paso
python -m saas_pipeline.cli --env dev --tenant hn --layer bronze
python -m saas_pipeline.cli --env dev --tenant hn --layer silver
python -m saas_pipeline.cli --env dev --tenant hn --layer gold
```

## Paso 4: Validar resultados

Verificar que se crearon las carpetas y tablas correctamente:

```
data/bronze/hn/deliveries/fecha_proceso=YYYYMMDD/
data/bronze/hn/materials/
data/silver/hn/fact_deliveries/fecha_proceso=YYYYMMDD/
data/silver/hn/dim_materials/
data/gold/hn/daily_metrics_by_delivery_type/fecha_proceso=YYYYMMDD/
```

Revisar los logs de calidad para detectar anomalías del nuevo tenant:

```bash
python -c "
from saas_pipeline.spark import get_spark
spark = get_spark()
df = spark.read.format('delta').load('data/shared/quality_logs')
df.filter(df.tenant_id == 'hn').show(truncate=False)
"
```

## Paso 5: Infraestructura (producción)

En un ambiente productivo con Databricks, el onboarding incluye provisionar infraestructura con Terraform:

- Schemas en Unity Catalog: `bronze_hn`, `silver_hn`, `gold_hn`
- Directorios en ADLS Gen2
- Permisos para el Service Principal del pipeline
- Secretos de conexión a fuentes del tenant

Ver `docs/infra.md` para el módulo de Terraform correspondiente.

## Ejecución multi-tenant

Una vez configurado, el tenant se puede procesar junto con los demás:

```bash
# Procesar todos los tenants configurados
python -m saas_pipeline.cli --env dev --tenant all
```

El parámetro `--tenant all` lee todos los archivos en `config/tenants/` y procesa cada uno secuencialmente. Si un tenant falla y `fail_fast=false` (por defecto), el pipeline continúa con los demás y reporta los fallos al final.
