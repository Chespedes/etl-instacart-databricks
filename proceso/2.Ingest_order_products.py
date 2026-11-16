# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Ingesta: order_products (Raw → Bronze)
# MAGIC Une `order_products__prior.csv` y `order_products__train.csv` (mismas columnas) en `bronze.order_products`.
# MAGIC La columna `origen` (`prior` / `train`) conserva de qué archivo vino cada fila y es la columna de partición.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType

# COMMAND ----------

dbutils.widgets.text("storageName", "adlssmartdatadev0811")
dbutils.widgets.text("catalogo", "catalog_dev")

storage  = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")

ruta_raw      = f"abfss://raw@{storage}.dfs.core.windows.net/"
tabla_destino = f"{catalogo}.bronze.order_products"

# COMMAND ----------

esquema = StructType([
    StructField("order_id",          IntegerType(), False),
    StructField("product_id",        IntegerType(), False),
    StructField("add_to_cart_order", IntegerType(), True),   # posición en que se agregó al carrito
    StructField("reordered",         IntegerType(), True),   # 1 = el cliente ya lo había comprado antes
])

def leer_detalle(archivo: str, origen: str):
    """Lee un archivo de detalle y le agrega su origen y las columnas de auditoría."""
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .schema(esquema)
            .csv(ruta_raw + archivo)
            .withColumn("origen", F.lit(origen))
            .withColumn("archivo_origen", F.col("_metadata.file_path"))
            .withColumn("ingestion_date", F.current_timestamp()))

# COMMAND ----------

df_prior = leer_detalle("order_products__prior.csv", "prior")
df_train = leer_detalle("order_products__train.csv", "train")

# unionByName une por NOMBRE de columna (más seguro que union, que une por posición)
df = df_prior.unionByName(df_train)

display(df.groupBy("origen").count())

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
