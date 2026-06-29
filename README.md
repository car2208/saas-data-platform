# saas-data-platform

Plataforma de datos multi-tenant para procesamiento de entregas de producto, construida sobre PySpark + Delta Lake siguiendo el patrón Medallion (Bronze → Silver → Gold).

## Stack tecnológico

| Componente | Versión | Rol |
|---|---|---|
| Python | 3.11+ | Runtime |
| PySpark | 3.5.2 | Motor de procesamiento |
| Delta Lake | 3.2.0 | Formato de almacenamiento ACID |
| OmegaConf | 2.3+ | Configuración jerárquica YAML |
| Click | 8.1+ | CLI |
| Pytest | 7.4+ | Testing |
| Ruff | 0.9+ | Linter y formato |

## Estructura del repositorio

```
saas-data-platform/
├── .github/
│   └── workflows/
│       └── ci.yml                         # GitHub Actions (lint, tests, config validation)
├── config/
│   ├── base.yaml                          # Configuración base compartida
│   ├── env/
│   │   ├── dev.yaml                       # Paths locales, fail_on_critical=false
│   │   ├── qa.yaml                        # Paths /mnt/data-qa, tenant=all
│   │   └── main.yaml                      # Paths /mnt/data-prod, tenant=all
│   └── tenants/
│       └── sv.yaml                        # Configuración tenant El Salvador
├── data/                                  # Generado al ejecutar (no versionado excepto raw/)
│   ├── raw/                               # CSVs de entrada (versionados)
│   ├── bronze/<tenant>/deliveries/        # Delta particionado por fecha_proceso
│   ├── bronze/<tenant>/materials/         # Catálogo de materiales en Delta
│   ├── silver/<tenant>/fact_deliveries/   # Hechos enriquecidos con SCD2
│   ├── silver/<tenant>/dim_materials/     # Dimensión SCD Type 2
│   ├── gold/<tenant>/daily_metrics_by_delivery_type/
│   ├── silver_quarantine/<tenant>/        # Filas con anomalías
│   └── shared/quality_logs/               # Logs de validación cross-tenant
├── src/
│   └── saas_pipeline/
│       ├── __init__.py
│       ├── cli.py                         # Punto de entrada CLI
│       ├── config.py                      # Carga y validación de config
│       ├── spark.py                       # Creación de SparkSession + Delta
│       ├── bronze.py                      # Ingesta RAW → Delta
│       ├── silver.py                      # Transformaciones, SCD2, cuarentena
│       ├── gold.py                        # Agregaciones y métricas
│       └── quality.py                     # Validaciones y quality_logs
├── tests/
│   ├── conftest.py                        # Fixture de SparkSession para tests
│   ├── test_silver_transforms.py          # Tests de transformaciones Silver
│   └── test_quality.py                    # Tests de validaciones de calidad
├── mentoring/
│   ├── bad_code.py                        # Código original del junior (Anexo A)
│   ├── good_code.py                       # Versión refactorizada
│   └── code_review.md                     # Observaciones y feedback
├── docs/
│   ├── observations.md                    # Observaciones a la arquitectura (4 observaciones)
│   ├── infra.md                           # Terraform snippet para onboarding
│   └── onboarding-tenant.md              # Guía de onboarding de tenants
├── pyproject.toml                         # Dependencias y configuración del proyecto
└── .gitignore
```

## Requisitos previos

- Python 3.11+
- Java 8 o 11 (requerido por PySpark)
- Los CSVs de entrada deben estar en `data/raw/`

## Instalación

```bash
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Linux/Mac

pip install -e ".[dev]"
```

## Ejecución del pipeline

```bash
# Pipeline completo para un tenant
python -m saas_pipeline.cli --env dev --tenant sv

# Solo una capa (útil para reprocesos)
python -m saas_pipeline.cli --env dev --tenant sv --layer bronze
python -m saas_pipeline.cli --env dev --tenant sv --layer silver
python -m saas_pipeline.cli --env dev --tenant sv --layer gold

# Todos los tenants
python -m saas_pipeline.cli --env dev --tenant all

# Rango de fechas específico
python -m saas_pipeline.cli --env dev --tenant sv --start-date 2025-03-01 --end-date 2025-03-31
```

## Ejecución en Databricks

La ejecución en Databricks requiere ajustar los paths relativos del config a rutas absolutas del workspace. En un ambiente productivo, los paths se configurarían en config/env/ apuntando a ADLS Gen2.

## Tests y linter

```bash
# Correr tests
pytest tests/ -v

# Linter
ruff check src/
ruff format --check src/
```

## Onboarding de un nuevo tenant

1. Crear archivo de configuración en `config/tenants/<tenant_id>.yaml`:
   ```yaml
   tenant_id: "hn"
   tenant_name: "Honduras"
   ```
2. Verificar que los datos del tenant estén disponibles en la capa RAW.
3. Ejecutar el pipeline:
   ```bash
   python -m saas_pipeline.cli --env dev --tenant hn
   ```
4. El pipeline crea automáticamente la estructura de carpetas para el nuevo tenant en bronze, silver y gold.

Para más detalle ver `docs/onboarding-tenant.md`.

## Qué dejé fuera y por qué

- **Auto Loader / streaming:** La arquitectura lo contempla pero no es parte del alcance de implementación. Se simula con lectura batch de CSV.
- **Terraform funcional:** Se incluye un snippet ilustrativo en `docs/infra.md`, no un módulo ejecutable.
- **Dashboard:** No implementado por priorización de tiempo. El foco se centró en pipeline funcional con calidad de datos.
- **Pre-commit hooks:** No implementados. El CI con GitHub Actions cubre lint y tests.
- **Naming de 3 niveles en catálogo (`saas_<env>.bronze_<tenant>.<table>`):** Localmente Spark solo soporta 2 niveles (`schema.table`). El tercer nivel (catálogo `saas_<env>`) es un recurso de Unity Catalog que se provisiona con Terraform en Databricks. Las tablas se registran como `bronze_<tenant>.<table>` localmente y se mapearían al catálogo completo en producción.
