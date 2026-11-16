# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Ingesta: products (Raw → Bronze)
# MAGIC Carga `products.csv` en `bronze.products`.
# MAGIC
# MAGIC Cada línea termina en `;`, así que el archivo tiene una **5.ª columna sin nombre**. Se conserva como `columna_extra`:
# MAGIC en 2 productos corruptos (nombre con el código HTML `&#39;` que contiene un `;`), el resto de la información quedó ahí
# MAGIC con el formato `nombre, pasillo, departamento`. Se repara en Silver.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

dbutils.widgets.text("storageName", "adlssmartdatadev0811")
dbutils.widgets.text("catalogo", "catalog_dev")

storage  = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")

ruta_archivo  = f"abfss://raw@{storage}.dfs.core.windows.net/products.csv"
tabla_destino = f"{catalogo}.bronze.products"

# COMMAND ----------

esquema = StructType([
    StructField("product_id",    IntegerType(), False),
    StructField("product_name",  StringType(),  True),
    StructField("aisle_id",      IntegerType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("columna_extra", StringType(),  True),   # columna fantasma: se conserva para reparar en Silver
])

# COMMAND ----------

df = (spark.read
      .option("header", True)
      .option("sep", ";")
      .schema(esquema)
      .csv(ruta_archivo)
      .withColumn("archivo_origen", F.col("_metadata.file_path"))
      .withColumn("ingestion_date", F.current_timestamp()))

# Vista rápida de los registros problemáticos (entran igual a Bronze)
display(df.filter(F.col("aisle_id").isNull() | F.col("columna_extra").isNotNull()))

# COMMAND ----------

columnas_destino = spark.table(tabla_destino).columns
df.select(columnas_destino).write.mode("overwrite").insertInto(tabla_destino)

# COMMAND ----------

leidos   = df.count()
escritos = spark.table(tabla_destino).count()
print(f"Filas leídas de raw: {leidos:,} | Filas en {tabla_destino}: {escritos:,}")
assert leidos == escritos, "La cantidad de filas no coincide"

# COMMAND ----------

dbutils.notebook.exit(f"OK - {tabla_destino}: {escritos} filas")
