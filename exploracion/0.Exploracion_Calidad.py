# Databricks notebook source
# MAGIC %md
# MAGIC # 0. Exploración y Calidad de Datos (Data Profiling)
# MAGIC
# MAGIC Antes de construir un ETL, un ingeniero de datos **investiga** los datos. Este notebook aplica un método de **6 preguntas** que sirve para cualquier dataset:
# MAGIC
# MAGIC | # | Pregunta | Técnica |
# MAGIC |---|---|---|
# MAGIC | 1 | ¿Se lee bien? | Mirar el archivo crudo (`dbutils.fs.head`) antes de leerlo con Spark |
# MAGIC | 2 | ¿Cuántas filas/columnas y de qué tipo? | `count()`, `printSchema()` |
# MAGIC | 3 | ¿Dónde hay nulos y por qué? | Conteo de nulos por columna |
# MAGIC | 4 | ¿Hay duplicados? | `count()` vs `dropDuplicates().count()` y duplicados por llave |
# MAGIC | 5 | ¿Los valores tienen sentido? | `describe()`, rangos, `distinct()`, búsqueda de patrones raros con regex |
# MAGIC | 6 | ¿Las tablas se relacionan bien? | Joins `left_anti` para encontrar huérfanos |
# MAGIC
# MAGIC Para cada problema se decide una acción: **corregir**, **descartar**, **marcar** o **documentar**.
# MAGIC
# MAGIC > Este notebook es de análisis: NO forma parte del workflow productivo.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.removeAll()
dbutils.widgets.text("storageName", "adlssmartdatadev0811")
storage = dbutils.widgets.get("storageName")

def ruta_raw(archivo: str = "") -> str:
    return f"abfss://raw@{storage}.dfs.core.windows.net/{archivo}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 1: ¿Se lee bien?
# MAGIC **Nunca leas un archivo con Spark sin mirarlo antes en crudo.** Así descubres el separador, si tiene encabezado, el formato de fechas y decimales, etc.

# COMMAND ----------

display(dbutils.fs.ls(ruta_raw()))

# COMMAND ----------

# Primeros 300 caracteres de cada archivo, tal cual están en disco
for archivo in ["orders.csv", "order_products__prior.csv", "products.csv", "aisles.json"]:
    print(f"========== {archivo}")
    print(dbutils.fs.head(ruta_raw(archivo), 300))
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Qué observas?**
# MAGIC - El separador es **`;`** y no `,`.
# MAGIC - Cada línea de `products.csv` termina en `;` → habrá una **columna fantasma**.
# MAGIC - `days_since_prior_order` trae decimales (`15.0`) y a veces viene **vacío**.
# MAGIC
# MAGIC Veamos qué pasa si leemos **sin** indicar el separador (el error típico):

# COMMAND ----------

df_mal = spark.read.option("header", True).csv(ruta_raw("products.csv"))
print("Columnas detectadas:", df_mal.columns)
display(df_mal.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC Todo quedó en **una sola columna**. Ahora la lectura correcta:

# COMMAND ----------

def leer_csv(archivo: str, inferir: bool = True):
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .option("inferSchema", inferir)   # Spark adivina los tipos (cuesta una pasada extra sobre los datos)
            .csv(ruta_raw(archivo)))

orders    = leer_csv("orders.csv")
op_prior  = leer_csv("order_products__prior.csv")
op_train  = leer_csv("order_products__train.csv")
products  = leer_csv("products.csv")
aisles      = spark.read.option("multiLine", True).json(ruta_raw("aisles.json"))
departments = spark.read.option("multiLine", True).json(ruta_raw("departments.json"))

print("Columnas de products:", products.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC La columna `_c4` es la **columna fantasma**: Spark le pone `_c<posición>` a las columnas sin nombre en el encabezado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 2: ¿Cuántas filas, columnas y de qué tipo?

# COMMAND ----------

tablas = {
    "orders": orders, "order_products_prior": op_prior, "order_products_train": op_train,
    "products": products, "aisles": aisles, "departments": departments,
}

for nombre, df in tablas.items():
    print(f"{nombre:25} filas: {df.count():>10,}   columnas: {len(df.columns)}")

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Qué observas?**
# MAGIC - Tres archivos tienen **exactamente 1.048.575 filas**. Ese número es el **límite de filas de Excel** (2^20 - 1). Es una señal de que los archivos fueron abiertos y guardados en Excel, que **cortó** los datos.
# MAGIC - `days_since_prior_order` se infirió como `double` por el `.0`.
# MAGIC
# MAGIC 💡 **Lección:** un conteo "redondo" o sospechoso siempre merece una segunda mirada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 3: ¿Dónde hay nulos y por qué?

# COMMAND ----------

def contar_nulos(df):
    """Devuelve una fila con la cantidad de nulos de cada columna."""
    return df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])

for nombre, df in tablas.items():
    print(f"--- {nombre}")
    display(contar_nulos(df))

# COMMAND ----------

# ¿Los nulos de days_since_prior_order son un error o tienen sentido?
nulos_dias  = orders.filter(F.col("days_since_prior_order").isNull()).count()
clientes    = orders.select("user_id").distinct().count()
primer_pedido_nulo = orders.filter((F.col("order_number") == 1) & F.col("days_since_prior_order").isNull()).count()

print(f"Nulos en days_since_prior_order: {nulos_dias:,}")
print(f"Clientes distintos:              {clientes:,}")
print(f"Nulos que son el pedido #1:      {primer_pedido_nulo:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusión:** los nulos coinciden exactamente con los primeros pedidos de cada cliente. **No es un error**: el primer pedido no tiene un pedido anterior.
# MAGIC → Acción: **marcar** con una bandera `es_primer_pedido` en Silver.
# MAGIC
# MAGIC En `products` hay nulos en `aisle_id` y `department_id` → los investigamos en la Pregunta 5.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 4: ¿Hay duplicados?

# COMMAND ----------

def duplicados(df, llave: str):
    total       = df.count()
    filas_dup   = total - df.dropDuplicates().count()                       # filas 100% idénticas
    llaves_dup  = df.groupBy(llave).count().filter("count > 1").count()     # misma llave repetida
    return total, filas_dup, llaves_dup

for nombre, df, llave in [("orders", orders, "order_id"), ("products", products, "product_id"),
                          ("aisles", aisles, "aisle_id"), ("departments", departments, "department_id")]:
    t, f, l = duplicados(df, llave)
    print(f"{nombre:12} filas: {t:>10,}  filas duplicadas: {f}  llaves duplicadas ({llave}): {l}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusión:** no hay duplicados. Aun así, en Silver aplicaremos `dropDuplicates` por llave como **regla defensiva** (si mañana llega un archivo con duplicados, el ETL no se rompe).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 5: ¿Los valores tienen sentido?

# COMMAND ----------

display(orders.select("order_dow", "order_hour_of_day", "days_since_prior_order", "order_number").describe())

# COMMAND ----------

display(orders.groupBy("eval_set").count())
display(departments.orderBy("department_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC - `order_dow` entre 0 y 6 ✅ (día de la semana), `order_hour_of_day` entre 0 y 23 ✅, `days_since_prior_order` entre 0 y 30 ✅.
# MAGIC - Existe un departamento llamado **`missing`** (id 21) → lo usaremos para los productos que no se puedan reparar.
# MAGIC
# MAGIC Ahora buscamos **productos sospechosos** con varias reglas a la vez:

# COMMAND ----------

sospechosos = products.filter(
    F.col("aisle_id").isNull()                         # sin pasillo
    | F.col("department_id").isNull()                  # sin departamento
    | F.col("_c4").isNotNull()                         # algo cayó en la columna fantasma
    | F.col("product_name").rlike("&#[0-9]+")          # códigos HTML como &#39; (apóstrofo)
    | (F.length("product_name") <= 3)                  # nombres demasiado cortos
)
print("Productos sospechosos:", sospechosos.count())
display(sospechosos)

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Qué observas?**
# MAGIC - Nombres como `L&#39` o `Reg`: el nombre original tenía un `;` o un código HTML que **partió la fila**.
# MAGIC - En 2 casos, el resto de la información quedó en `_c4` con el formato `nombre,pasillo,departamento` → **se puede reparar**.
# MAGIC - El resto no tiene pasillo ni departamento → se **marcan** y se asignan al departamento `missing`.
# MAGIC - Además, reemplazaremos `&#39;` por `'` en todos los nombres (**corregir**).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pregunta 6: ¿Las tablas se relacionan bien? (integridad referencial)
# MAGIC Un **left_anti join** devuelve las filas de la izquierda que **NO** tienen pareja en la derecha: los "huérfanos".

# COMMAND ----------

order_products = op_prior.unionByName(op_train)

total_detalle      = order_products.count()
huerfanos_pedido   = order_products.join(orders, "order_id", "left_anti").count()
huerfanos_producto = order_products.join(products, "product_id", "left_anti").count()
prod_sin_pasillo   = products.filter(F.col("aisle_id").isNotNull()).join(aisles, "aisle_id", "left_anti").count()

print(f"Detalles de pedido totales:          {total_detalle:,}")
print(f"Detalles cuyo pedido NO está en orders: {huerfanos_pedido:,} ({huerfanos_pedido/total_detalle:.1%})")
print(f"Detalles cuyo producto NO existe:    {huerfanos_producto:,}")
print(f"Productos con pasillo inexistente:   {prod_sin_pasillo:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusión:** cerca del **70 % de los detalles no tiene su pedido** en `orders`. Es consecuencia del corte de Excel (Pregunta 2): `orders` quedó incompleto.
# MAGIC → Acción: en Silver hacemos **inner join** (solo detalles con pedido válido), **registramos** cuántos se descartan en `silver.calidad_datos` y lo **documentamos** en el README.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de hallazgos y decisiones
# MAGIC
# MAGIC | # | Hallazgo | Acción | Dónde |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Separador `;` | **Corregir** la lectura: `.option("sep", ";")` | Ingesta (Bronze) |
# MAGIC | 2 | Columna fantasma `_c4` en products | **Conservar** como `columna_extra` para reparar | Bronze → Silver |
# MAGIC | 3 | Archivos cortados en 1.048.575 filas (límite de Excel) | **Documentar** | README |
# MAGIC | 4 | Nulos en `days_since_prior_order` = primer pedido | **Marcar** `es_primer_pedido` | Silver |
# MAGIC | 5 | Productos corruptos (`L&#39`, `Reg`…) | **Corregir** los reparables; **marcar** y asignar `missing` al resto | Silver |
# MAGIC | 6 | Códigos HTML `&#39;` en nombres | **Corregir** → `'` | Silver |
# MAGIC | 7 | ~70 % de detalles sin pedido | **Descartar** con inner join + **registrar** métrica | Silver + `calidad_datos` |
# MAGIC | 8 | Sin duplicados | Regla **defensiva** `dropDuplicates` | Silver |
