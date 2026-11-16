# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Ingesta: catálogos aisles y departments (Raw JSON → Bronze)
# MAGIC Los catálogos vienen en **JSON** (arreglo de objetos), por eso se leen con `multiLine = true`.
# MAGIC Se usa una función para no repetir código: la misma lógica sirve para ambos archivos.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

dbutils.widgets.text("storageName", "adlssmartdatadev0811")
dbutils.widgets.text("catalogo", "catalog_dev")

storage  = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")

ruta_raw = f"abfss://raw@{storage}.dfs.core.windows.net/"

# COMMAND ----------

def ingestar_json(archivo: str, tabla: str, esquema: StructType) -> int:
    """Lee un JSON de raw, agrega auditoría, lo escribe en bronze y devuelve las filas escritas."""
    tabla_destino = f"{catalogo}.bronze.{tabla}"

    df = (spark.read
          .option("multiLine", True)          # el archivo es un arreglo [ {...}, {...} ]
          .schema(esquema)
          .json(ruta_raw + archivo)
          .withColumn("archivo_origen", F.col("_metadata.file_path"))
          .withColumn("ingestion_date", F.current_timestamp()))

    columnas_destino = spark.table(tabla_destino).columns
    df.select(columnas_destino).write.mode("overwrite").insertInto(tabla_destino)

    leidos, escritos = df.count(), spark.table(tabla_destino).count()
    print(f"{archivo}: leídas {leidos} | escritas en {tabla_destino}: {escritos}")
    assert leidos == escritos, f"La cantidad de filas no coincide en {tabla_destino}"
    return escritos

# COMMAND ----------

esquema_aisles = StructType([
    StructField("aisle_id", IntegerType(), False),
    StructField("aisle",    StringType(),  True),
])

esquema_departments = StructType([
    StructField("department_id", IntegerType(), False),
    StructField("department",    StringType(),  True),
])

filas_aisles      = ingestar_json("aisles.json",      "aisles",      esquema_aisles)
filas_departments = ingestar_json("departments.json", "departments", esquema_departments)

# COMMAND ----------

display(spark.table(f"{catalogo}.bronze.departments").orderBy("department_id"))

# COMMAND ----------

dbutils.notebook.exit(f"OK - aisles: {filas_aisles} | departments: {filas_departments}")
