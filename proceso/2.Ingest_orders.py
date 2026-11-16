# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Ingesta: orders (Raw → Bronze)
# MAGIC Lee `orders.csv` desde el contenedor **raw** (acceso por Managed Identity vía External Location) y lo carga en `bronze.orders`.
# MAGIC
# MAGIC - Esquema **explícito** (no `inferSchema`): más rápido (una sola lectura) y con control de tipos.
# MAGIC - Separador `;` (hallazgo de la exploración).
# MAGIC - Columnas de auditoría: `archivo_origen` (linaje) e `ingestion_date`.
# MAGIC - Bronze **no limpia**: los datos entran tal cual.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

dbutils.widgets.text("storageName", "adlssmartdatadev0811")
dbutils.widgets.text("catalogo", "catalog_dev")

storage  = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")

ruta_archivo  = f"abfss://raw@{storage}.dfs.core.windows.net/orders.csv"
tabla_destino = f"{catalogo}.bronze.orders"

# COMMAND ----------

esquema = StructType([
    StructField("order_id",               IntegerType(), False),
    StructField("user_id",                IntegerType(), True),
    StructField("eval_set",               StringType(),  True),
    StructField("order_number",           IntegerType(), True),
    StructField("order_dow",              IntegerType(), True),
    StructField("order_hour_of_day",      IntegerType(), True),
    StructField("days_since_prior_order", DoubleType(),  True),   # viene como 15.0 y vacío en el primer pedido
])

# COMMAND ----------

df = (spark.read
      .option("header", True)
      .option("sep", ";")
      .schema(esquema)
      .csv(ruta_archivo)
      .withColumn("archivo_origen", F.col("_metadata.file_path"))   # linaje: de qué archivo vino cada fila
      .withColumn("ingestion_date", F.current_timestamp()))

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escritura en Bronze
# MAGIC `insertInto` escribe **por posición**, por eso se seleccionan las columnas en el mismo orden que la tabla destino (creada en el PrepAmb).
# MAGIC `overwrite` reemplaza el contenido: si el workflow se ejecuta dos veces, no se duplican datos (idempotencia).

# COMMAND ----------

columnas_destino = spark.table(tabla_destino).columns
df.select(columnas_destino).write.mode("overwrite").insertInto(tabla_destino)

# COMMAND ----------

# Validación: lo leído debe ser igual a lo escrito
leidos   = df.count()
escritos = spark.table(tabla_destino).count()
print(f"Filas leídas de raw: {leidos:,} | Filas en {tabla_destino}: {escritos:,}")
assert leidos == escritos, "La cantidad de filas no coincide"

# COMMAND ----------

dbutils.notebook.exit(f"OK - {tabla_destino}: {escritos} filas")
