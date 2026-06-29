# Code Review — bad_code.py

Buen trabajo logrando que el flujo funcione de punta a punta — leer, transformar y escribir. Eso demuestra que entiendes el objetivo. Los ajustes que necesita el código no son de lógica sino de ingeniería: cómo hacerlo escalable, robusto y mantenible para producción.

Te comparto cuatro observaciones puntuales que te van a ayudar a solidificar tu conocimiento y llevar tu código al siguiente nivel. Revisalas junto con la versión refactorizada en `good_code.py`.

---

## Observación 1: Uso de pandas donde corresponde Spark nativo

**Qué está mal:** Se usa `pd.read_csv()` para leer el archivo y `df.iterrows()` para iterar fila por fila. Luego se reconstruye un DataFrame de pandas y se convierte a Spark con `spark.createDataFrame(out)`.

**Por qué importa:** `iterrows()` es una operación secuencial que no escala — con millones de filas el procesamiento sería extremadamente lento. El propósito de usar Spark es justamente evitar procesamiento fila por fila, delegando las transformaciones al motor distribuido.

**Cómo se corrige:** Leer directamente con `spark.read.csv()` y usar operaciones columnares (`F.when`, `F.col`, `F.filter`) que Spark paraleliza automáticamente. Esto elimina la dependencia de pandas para el procesamiento.

---

## Observación 2: Lógica de negocio hardcoded y sin cobertura completa

**Qué está mal:** Solo se filtran los tipos `ZPRE` y `ZVE1` (rutina), ignorando `Z04` y `Z05` (bonificación). Los valores válidos están embebidos directamente en los condicionales sin posibilidad de configuración. El país (`"GT"`) y la ruta del archivo (`"data.csv"`) están hardcoded en la llamada `process("data.csv", "GT")`.

**Por qué importa:** Si cambian las reglas de negocio o se agrega un nuevo tipo de entrega, hay que modificar el código en lugar de ajustar una configuración. Además, el código ignora silenciosamente el 30-40% de las transacciones válidas (bonificaciones).

**Cómo se corrige:** Definir los tipos válidos como constantes o parámetros de configuración. Parametrizar rutas y tenant desde un CLI o archivo de configuración, nunca hardcoded en el script.

---

## Observación 3: Escritura no idempotente y sin formato adecuado

**Qué está mal:** Se escribe con `mode("overwrite").parquet(...)`, lo que sobrescribe todo el directorio de salida cada vez que se ejecuta. No hay particionado, no se usa Delta Lake, y la ruta `/tmp/output/` es temporal y no persistente.

**Por qué importa:** Sin particionado, reprocesar un solo día implica reescribir toda la tabla. Sin Delta Lake no hay soporte para MERGE, time travel ni transacciones ACID. Usar `/tmp/` significa que los datos se pierden al reiniciar la máquina.

**Cómo se corrige:** Usar formato Delta con `replaceWhere` para sobrescribir solo las particiones afectadas. Particionar por `fecha_proceso`. Usar rutas configurables que persistan los datos de forma duradera.

---

## Observación 4: Ausencia de validaciones, tipado y manejo de errores

**Qué está mal:** No hay validación de datos de entrada (filas con precio nulo, cantidades negativas, materiales inexistentes). No hay type hints en la función. El único feedback es `print("done")`, sin logging estructurado. No hay manejo de errores — si el archivo no existe, el script falla con un traceback sin contexto.

**Por qué importa:** En un pipeline productivo, los datos de entrada siempre contienen anomalías. Procesarlas silenciosamente genera métricas incorrectas que impactan decisiones de negocio. La falta de logging dificulta el debugging en producción.

**Cómo se corrige:** Implementar validaciones de calidad que envíen filas problemáticas a cuarentena. Agregar type hints para documentar el contrato de la función. Usar `logging` en lugar de `print`. Encapsular la lógica en funciones con manejo de excepciones controlado.

---

## Temas para investigar por tu cuenta

Te recomiendo investigar estos tres temas. Después compará tu solución con la versión refactorizada y fijate qué decisiones tomaste distinto. Cualquier duda la vemos juntos.

1. **Operaciones columnares vs fila por fila en Spark.** Entender por qué `iterrows()` no escala y cómo reemplazarlo con `F.when`, `F.col` y `filter` cambia completamente la forma en que se escribe código en PySpark.

2. **Delta Lake como reemplazo de Parquet.** Investigar qué ganas con transacciones ACID, `MERGE INTO` y time travel, y por qué el ecosistema Databricks lo adopta como estándar.

3. **Patrón de cuarentena para datos anómalos.** Revisar cómo separar las filas con problemas (precio nulo, cantidad negativa) en una tabla aparte en lugar de descartarlas silenciosamente o dejarlas pasar.
