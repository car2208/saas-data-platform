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
├── .github/workflows/ci.yml          # GitHub Actions (lint, tests, config validation)
├── config/
│   ├── base.yaml                      # Configuración base compartida
│   ├── env/
│   │   ├── dev.yaml                   # Paths locales, fail_on_critical=false
│   │   ├── qa.yaml                    # Paths /mnt/data-qa, tenant=all
│   │   └── main.yaml                  # Paths /mnt/data-prod, tenant=all
│   └── tenants/
│       └── sv.yaml                    # Configuración tenant El Salvador
├── data/                              # Generado al ejecutar (no versionado excepto raw/)
│   ├── raw/                           # CSVs de entrada (versionados)
│   ├── bronze/<tenant>/deliveries/    # Delta particionado por fecha_proceso
│   ├── silver/<tenant>/fact_deliveries/
│   ├── silver/<tenant>/dim_materials/
│   ├── gold/<tenant>/daily_metrics_by_delivery_type/
│   ├── silver_quarantine/<tenant>/    # Filas con anomalías
│   └── shared/quality_logs/           # Logs de validación cross-tenant
├── src/saas_pipeline/
│   ├── cli.py                         # Punto de entrada CLI
│   ├── config.py                      # Carga y validación de config
│   ├── spark.py                       # Creación de SparkSession + Delta
│   ├── bronze.py                      # Ingesta RAW → Delta
│   ├── silver.py                      # Transformaciones, SCD2, cuarentena
│   ├── gold.py                        # Agregaciones y métricas
│   └── quality.py                     # Validaciones y quality_logs
├── tests/
├── mentoring/
├── docs/
│   ├── observations.md                # Observaciones a la arquitectura
│   └── infra.md                       # Terraform snippet
└── pyproject.toml
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
2. Colocar los datos del tenant en `data/raw/` (el CSV debe incluir filas con `pais=HN`).
3. Ejecutar el pipeline:
   ```bash
   python -m saas_pipeline.cli --env dev --tenant hn
   ```
4. El pipeline crea automáticamente la estructura de carpetas para el nuevo tenant en bronze, silver y gold.

## Qué dejé fuera y por qué

- **Auto Loader / streaming:** La arquitectura lo contempla pero no es parte del alcance de implementación. Se simula con lectura batch de CSV.
- **Terraform funcional:** Se incluye un snippet ilustrativo en `docs/infra.md`, no un módulo ejecutable. El examen no lo requiere.
- **Dashboard:** No implementado por priorización de tiempo. El foco se centró en pipeline funcional con calidad de datos.
- **Pre-commit hooks:** No implementados. El CI con GitHub Actions cubre lint y tests.
