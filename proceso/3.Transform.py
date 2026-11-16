# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Transform (Bronze → Silver)
# MAGIC Limpia, repara, enriquece y une los datos. Cada regla de calidad aplicada se **registra** en `silver.calidad_datos`.
# MAGIC
# MAGIC | Tabla Silver | Qué hace |
# MAGIC |---|---|
# MAGIC | `products` | Repara productos corruptos, decodifica `&#39;` → `'`, asigna `missing` a los irreparables, agrega nombre de pasillo y departamento |
# MAGIC | `orders` | Día de la semana y franja horaria legibles, días como entero, bandera `es_primer_pedido` |
# MAGIC | `order_items` | Tabla de hechos: detalle + pedido + producto (inner join: descarta detalles sin pedido) |
# MAGIC | `calidad_datos` | Bitácora de reglas de calidad y registros afectados |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

dbutils.widgets.text("catalogo", "catalog_dev")
catalogo = dbutils.widgets.get("catalogo")

bronze = f"{catalogo}.bronze"
silver = f"{catalogo}.silver"

# Bitácora de calidad: (tabla, regla, registros_afectados, accion)
calidad = []
def registrar(tabla: str, regla: str, afectados: int, accion: str):
    calidad.append((tabla, regla, int(afectados), accion))
    print(f"[{accion.upper():10}] {tabla}: {regla} -> {afectados:,} registros")

def escribir(df, tabla: str):
    """Escribe en una tabla Silver respetando el orden de columnas de la tabla destino."""
    destino = f"{silver}.{tabla}"
    df.select(spark.table(destino).columns).write.mode("overwrite").insertInto(destino)
    print(f"OK -> {destino}: {spark.table(destino).count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Products
# MAGIC Productos corruptos detectados en la exploración: un `;` dentro del nombre (del código HTML `&#39;`) partió la fila.
# MAGIC - **Reparables (2):** el resto quedó en `columna_extra` como `" nombre,pasillo,departamento"` → se reconstruye con una expresión regular.
# MAGIC - **Irreparables (9):** sin pasillo ni departamento → se asignan a `missing` (pasillo 100, departamento 21), que ya existen en los catálogos.

# COMMAND ----------

products   = spark.table(f"{bronze}.products").dropDuplicates(["product_id"])
aisles     = spark.table(f"{bronze}.aisles").select("aisle_id", "aisle")
departments = spark.table(f"{bronze}.departments").select("department_id", "department")

# Regex sobre columna_extra: (nombre),(pasillo),(departamento)
patron = r"^\s*(.*),(\d+),(\d+)\s*$"
es_reparable   = F.coalesce(F.col("columna_extra").rlike(patron), F.lit(False))   # coalesce: si columna_extra es nula, rlike da nulo -> False
es_irreparable = ~es_reparable & (F.col("aisle_id").isNull() | F.col("department_id").isNull())
tiene_html     = F.col("product_name").contains("&#39")

registrar("products", "Producto partido por ';' reparable desde columna_extra", products.filter(es_reparable).count(), "corregir")
registrar("products", "Producto sin pasillo/departamento: asignado a 'missing'", products.filter(es_irreparable).count(), "marcar")
registrar("products", "Código HTML &#39; en el nombre decodificado a apóstrofo", products.filter(tiene_html).count(), "corregir")

products_limpio = (products
    .withColumn("producto_reparado", es_reparable | es_irreparable)
    # Reconstruir nombre, pasillo y departamento de los reparables
    .withColumn("product_name",
        F.when(es_reparable, F.concat(F.col("product_name"), F.lit(";"), F.trim(F.regexp_extract("columna_extra", patron, 1))))
         .otherwise(F.col("product_name")))
    .withColumn("aisle_id",
        F.when(es_reparable, F.regexp_extract("columna_extra", patron, 2).cast("int"))
         .otherwise(F.col("aisle_id")))
    .withColumn("department_id",
        F.when(es_reparable, F.regexp_extract("columna_extra", patron, 3).cast("int"))
         .otherwise(F.col("department_id")))
    # Irreparables -> missing
    .withColumn("aisle_id",      F.coalesce("aisle_id", F.lit(100)))
    .withColumn("department_id", F.coalesce("department_id", F.lit(21)))
    # Decodificar HTML y limpiar espacios
    .withColumn("product_name", F.trim(F.regexp_replace(F.regexp_replace("product_name", "&#39;?", "'"), "&amp;", "&")))
    .join(aisles, "aisle_id", "left")
    .join(departments, "department_id", "left")
    .withColumn("fecha_proceso", F.current_timestamp()))

display(products_limpio.filter("producto_reparado"))

# COMMAND ----------

escribir(products_limpio, "products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Orders
# MAGIC - `order_dow` (0-6) → nombre del día. **Supuesto documentado:** 0 = Domingo (Instacart no lo especifica; es la interpretación más usada).
# MAGIC - `order_hour_of_day` → franja horaria.
# MAGIC - `days_since_prior_order` nulo = primer pedido del cliente → bandera `es_primer_pedido` (no es error).

# COMMAND ----------

orders = spark.table(f"{bronze}.orders")

duplicados = orders.count() - orders.dropDuplicates(["order_id"]).count()
registrar("orders", "Pedidos duplicados por order_id (regla defensiva)", duplicados, "descartar")
registrar("orders", "days_since_prior_order nulo = primer pedido del cliente", orders.filter(F.col("days_since_prior_order").isNull()).count(), "marcar")

dias = F.array(*[F.lit(d) for d in ["Domingo", "Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado"]])

orders_limpio = (orders
    .dropDuplicates(["order_id"])
    .withColumn("dia_semana", F.element_at(dias, F.col("order_dow") + 1))   # element_at empieza en 1
    .withColumn("franja_horaria",
        F.when(F.col("order_hour_of_day") < 6,  "Madrugada")
         .when(F.col("order_hour_of_day") < 12, "Mañana")
         .when(F.col("order_hour_of_day") < 18, "Tarde")
         .otherwise("Noche"))
    .withColumn("es_primer_pedido", F.col("days_since_prior_order").isNull())
    .withColumn("days_since_prior_order", F.col("days_since_prior_order").cast("int"))
    .withColumn("fecha_proceso", F.current_timestamp()))

escribir(orders_limpio, "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Order items (tabla de hechos)
# MAGIC **Inner join** con orders: los archivos fuente fueron cortados en 1.048.575 filas (límite de Excel), por lo que la mayoría de los detalles no tiene su pedido. Se descartan y se registra cuántos.

# COMMAND ----------

detalle = spark.table(f"{bronze}.order_products").dropDuplicates(["order_id", "product_id"])
pedidos = spark.table(f"{silver}.orders").select("order_id", "user_id", "order_dow", "dia_semana", "order_hour_of_day", "franja_horaria")
productos = spark.table(f"{silver}.products").select("product_id", "product_name", "aisle", "department")

order_items = (detalle
    .join(pedidos, "order_id", "inner")
    .join(productos, "product_id", "left")
    .withColumn("fecha_proceso", F.current_timestamp()))

total_detalle = detalle.count()
total_items   = order_items.count()
registrar("order_items", "Detalle sin pedido en orders (archivo fuente truncado)", total_detalle - total_items, "descartar")

escribir(order_items, "order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bitácora de calidad

# COMMAND ----------

esquema_calidad = StructType([
    StructField("tabla", StringType()), StructField("regla", StringType()),
    StructField("registros_afectados", LongType()), StructField("accion", StringType()),
])
df_calidad = spark.createDataFrame(calidad, esquema_calidad).withColumn("fecha_proceso", F.current_timestamp())
escribir(df_calidad, "calidad_datos")
display(spark.table(f"{silver}.calidad_datos"))

# COMMAND ----------

dbutils.notebook.exit("OK - Transform Silver")
