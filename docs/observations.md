# Observaciones a la Arquitectura Provista

## 1. Capa RAW cross-tenant y aislamiento lógico

**Situación:** Los datasets de entrada (`global_mobility_data_entrega_productos.csv` y `materials_catalog.csv`) contienen datos de todos los tenants en un solo archivo. La capa RAW opera como un espacio compartido (cross-tenant), y el aislamiento por tenant se aplica recién a partir de Bronze, donde el pipeline filtra por `pais` y escribe a paths separados por tenant.

**Observación:** En un entorno productivo con múltiples unidades de negocio, esta configuración representa un riesgo de aislamiento: un proceso ejecutándose para el tenant `sv` tendría acceso de lectura a los datos crudos de todos los demás tenants en la capa RAW. Esto debilita la estrategia de aislamiento por schema que la arquitectura define a partir de Bronze.

**Propuesta alternativa:** Implementar landing zones separadas por tenant en ADLS Gen2, de forma que la estructura RAW también refleje el aislamiento:

```
data/raw/<tenant>/deliveries/fecha_proceso=YYYYMMDD/archivo.csv
data/raw/<tenant>/materials/archivo.csv
```

Complementar con ACLs a nivel de directorio en ADLS y políticas de acceso en Unity Catalog para garantizar que cada pipeline de tenant solo acceda a su propia landing zone. El trade-off es mayor complejidad en el proceso de ingesta desde las fuentes operacionales, pero se gana consistencia en el modelo de seguridad a lo largo de todas las capas.

**Resolución en esta implementación:** Se mantiene la estructura cross-tenant en RAW tal como la define la arquitectura provista, dado que los datos de entrada se entregan en ese formato. El aislamiento se aplica al momento de la ingesta a Bronze, filtrando por el campo `pais` y normalizando a minúscula.

---

## 2. Escalabilidad del pipeline: hacia un enfoque metadata-driven

**Situación:** La arquitectura actual define el procesamiento de dos datasets (entregas y catálogo de materiales) con lógica específica por tabla implementada directamente en código Python (`bronze.py`, `silver.py`). Cada nueva fuente de datos requiere escribir nuevas funciones, modificar el orquestador y agregar lógica de validación ad hoc.

**Observación:** Este enfoque funciona bien para un MVP con pocas tablas, pero no escala en un contexto multi-tenant donde se espera onboarding continuo de nuevas fuentes de datos. A medida que crece el número de tablas, el pipeline acumula lógica específica dispersa en múltiples archivos, lo que incrementa el costo de mantenimiento y el riesgo de inconsistencias entre tablas.

**Propuesta alternativa:** Evolucionar hacia un framework metadata-driven donde cada tabla se define mediante un archivo de metadatos declarativo:

```yaml
# config/metadata/deliveries.yaml
table:
  name: deliveries
  source:
    type: csv
    file: "global_mobility_data_entrega_productos.csv"
    delimiter: ","
    header: true
  bronze:
    partition_by: ["fecha_proceso"]
    tenant_column: "pais"
    idempotency: "replace_where"
  silver:
    target: "fact_deliveries"
    merge_keys: ["_tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega"]
    scd_type: null
  quality:
    checks:
      - name: "not_null_fecha"
        column: "fecha_proceso"
        rule: "not_null"
        severity: "critical"
```

Con este enfoque, agregar una nueva tabla al pipeline consiste en crear un archivo YAML de metadatos sin modificar código. El motor de ingesta lee los metadatos y ejecuta de forma genérica. Esto también permite:

- Filtrar qué tablas procesar desde el CLI (`--tables deliveries,materials`).
- Centralizar reglas de calidad por tabla.
- Documentar de forma viva el inventario de fuentes del pipeline.

**Trade-off:** Mayor inversión inicial en diseñar el framework genérico. Se recomienda como evolución para Horizonte 2, una vez validado el MVP actual.

**Resolución en esta implementación:** Se mantiene el enfoque con lógica específica por tabla dado el alcance de la prueba (2 datasets). La estructura del código está preparada para una migración futura a metadata-driven sin reestructuración mayor.

---

## 3. Parámetro --layer para ejecuciones parciales del pipeline

**Situación:** La arquitectura define un flujo secuencial Bronze → Silver → Gold que se ejecuta completo en cada corrida. No se contempla un mecanismo para ejecutar capas de forma independiente.

**Observación:** En operaciones de producción es común necesitar reprocesar una capa específica sin re-ejecutar todo el pipeline. Escenarios frecuentes incluyen: recomputar Gold tras un cambio en la lógica de agregación, re-ejecutar Silver después de corregir reglas de calidad, o reingestar Bronze ante la llegada de un archivo corregido desde la fuente.

**Mejora implementada:** Se agregó el parámetro `--layer` al CLI con las opciones `all`, `bronze`, `silver` y `gold`. Esto permite ejecuciones selectivas:

```bash
# Pipeline completo (comportamiento por defecto)
python -m saas_pipeline.cli --env dev --tenant sv

# Solo reingestar Bronze
python -m saas_pipeline.cli --env dev --tenant sv --layer bronze

# Recomputar Gold sin tocar Bronze ni Silver
python -m saas_pipeline.cli --env dev --tenant sv --layer gold
```

**Trade-off:** Ejecutar capas de forma aislada requiere que las capas anteriores ya existan (ej: no se puede correr Silver sin que Bronze haya escrito datos). No se implementó validación de dependencias entre capas; queda como mejora para una siguiente iteración.

---

*Este documento se irá ampliando con observaciones adicionales durante la implementación.*