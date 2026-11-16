# Databricks notebook source
# MAGIC %md
# MAGIC # 4. Load (Silver → Golden)
# MAGIC Cada tabla Golden responde una **pregunta de negocio**:
# MAGIC
# MAGIC | Tabla | Pregunta |
# MAGIC |---|---|
# MAGIC | `kpi_departamentos` | ¿Qué departamentos y pasillos venden más y tienen clientes más fieles? |
# MAGIC | `kpi_habitos_compra` | ¿Qué días y a qué horas compra la gente? |
# MAGIC | `top_productos_recompra` | ¿Cuáles son los productos "estrella" de recompra? |
# MAGIC | `segmentacion_clientes` | ¿Qué tipos de clientes tenemos? |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalogo", "catalog_dev")
catalogo = dbutils.widgets.get("catalogo")

silver = f"{catalogo}.silver"
golden = f"{catalogo}.golden"

items  = spark.table(f"{silver}.order_items")
orders = spark.table(f"{silver}.orders")

def escribir(df, tabla: str):
    destino = f"{golden}.{tabla}"
    (df.withColumn("fecha_proceso", F.current_timestamp())
       .select(spark.table(destino).columns)
       .write.mode("overwrite").insertInto(destino))
    print(f"OK -> {destino}: {spark.table(destino).count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. KPI por departamento y pasillo
# MAGIC `tasa_recompra` = productos recomprados / productos vendidos → mide la **fidelidad** a esa categoría.

# COMMAND ----------

kpi_departamentos = (items
    .groupBy("department", "aisle")
    .agg(F.count("*").alias("total_items"),
         F.countDistinct("order_id").alias("total_pedidos"),
         F.sum("reordered").alias("total_recompras"))
    .withColumn("tasa_recompra", F.round(F.col("total_recompras") / F.col("total_items"), 4)))

escribir(kpi_departamentos, "kpi_departamentos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Hábitos de compra (día x hora)
# MAGIC Se usan **todos los pedidos** de `silver.orders` (completos). `promedio_items` solo considera los pedidos que tienen detalle.

# COMMAND ----------

items_por_pedido = items.groupBy("order_id").agg(F.count("*").alias("n_items"))

kpi_habitos = (orders
    .join(items_por_pedido, "order_id", "left")
    .groupBy("order_dow", "dia_semana", "order_hour_of_day", "franja_horaria")
    .agg(F.count("order_id").alias("total_pedidos"),
         F.round(F.avg("n_items"), 2).alias("promedio_items")))

escribir(kpi_habitos, "kpi_habitos_compra")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Top 100 productos de recompra
# MAGIC Soporte mínimo de **50 compras** para que la tasa sea confiable (un producto comprado 2 veces y recomprado 2 tendría 100 %, pero no es representativo).

# COMMAND ----------

ventana = Window.orderBy(F.desc("veces_recomprado"), F.desc("tasa_recompra"))

top_productos = (items
    .groupBy("product_id", "product_name", "department")
    .agg(F.count("*").alias("veces_comprado"),
         F.sum("reordered").alias("veces_recomprado"))
    .filter(F.col("veces_comprado") >= 50)
    .withColumn("tasa_recompra", F.round(F.col("veces_recomprado") / F.col("veces_comprado"), 4))
    .withColumn("ranking", F.row_number().over(ventana))
    .filter(F.col("ranking") <= 100))

escribir(top_productos, "top_productos_recompra")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Segmentación de clientes
# MAGIC | Segmento | Regla |
# MAGIC |---|---|
# MAGIC | **Frecuente** | 20+ pedidos y compra cada 10 días o menos (en promedio) |
# MAGIC | **Ocasional** | 8+ pedidos |
# MAGIC | **Esporádico** | el resto |
# MAGIC > Los umbrales son reglas de negocio definidas para este análisis; se ajustan únicamente en esta celda.

# COMMAND ----------

perfil_pedidos = (orders
    .groupBy("user_id")
    .agg(F.count("order_id").cast("int").alias("total_pedidos"),
         F.round(F.avg("days_since_prior_order"), 2).alias("promedio_dias_entre_pedidos")))

perfil_items = (items
    .groupBy("user_id")
    .agg(F.count("*").alias("total_items"),
         F.round(F.avg("reordered"), 4).alias("tasa_recompra")))

segmentacion = (perfil_pedidos
    .join(perfil_items, "user_id", "left")
    .withColumn("total_items", F.coalesce("total_items", F.lit(0)).cast("bigint"))
    .withColumn("segmento",
        F.when((F.col("total_pedidos") >= 20) & (F.col("promedio_dias_entre_pedidos") <= 10), "Frecuente")
         .when(F.col("total_pedidos") >= 8, "Ocasional")
         .otherwise("Esporádico")))

escribir(segmentacion, "segmentacion_clientes")

display(spark.table(f"{golden}.segmentacion_clientes").groupBy("segmento").count().orderBy("segmento"))

# COMMAND ----------

dbutils.notebook.exit("OK - Load Golden")