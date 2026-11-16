# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Preparación del Ambiente
# MAGIC **Proyecto:** ETL Medallion - Instacart (Trabajo Final DataSmart)
# MAGIC
# MAGIC Este notebook deja listo el ambiente (dev o prod) antes de correr el ETL:
# MAGIC 1. **External Locations**: autorizan a Unity Catalog a usar cada contenedor del Data Lake con la Managed Identity (`cred-smartdata`).
# MAGIC 2. **Catálogo**: `catalog_dev` o `catalog_prod`, con su *managed location* en el contenedor `metastore`.
# MAGIC 3. **Schemas**: `bronze`, `silver`, `golden` (las capas del medallion).
# MAGIC 4. **Tablas externas** (Delta) con su `LOCATION` en el contenedor de su capa.
# MAGIC
# MAGIC > La capa **raw** NO es un schema: son los archivos fuente (CSV/JSON) en el contenedor `raw`, leídos vía Managed Identity. No se usa DBFS ni Volúmenes.
# MAGIC
# MAGIC Todo es **parametrizado** con widgets: el mismo notebook sirve para dev y prod, solo cambian los parámetros que envía el workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("storageName", "adlssmartdatadev0811")   # storage account del ambiente
dbutils.widgets.text("catalogo", "catalog_dev")              # catalog_dev | catalog_prod
dbutils.widgets.text("ambiente", "dev")                      # dev | prod (sufijo de las external locations)
dbutils.widgets.text("credencial", "cred-smartdata")         # storage credential (Managed Identity)
dbutils.widgets.dropdown("reset", "false", ["false", "true"])  # true = borra catálogo y datos para empezar de cero

# COMMAND ----------

storage    = dbutils.widgets.get("storageName")
catalogo   = dbutils.widgets.get("catalogo")
ambiente   = dbutils.widgets.get("ambiente")
credencial = dbutils.widgets.get("credencial")
reset      = dbutils.widgets.get("reset") == "true"

def ruta(contenedor: str, carpeta: str = "") -> str:
    """Arma la URL abfss de un contenedor (y carpeta opcional) del storage del ambiente."""
    return f"abfss://{contenedor}@{storage}.dfs.core.windows.net/{carpeta}"

print(f"Ambiente: {ambiente} | Storage: {storage} | Catálogo: {catalogo} | Reset: {reset}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. External Locations
# MAGIC Una por contenedor. El nombre lleva el ambiente (`exlt-dev-raw`, `exlt-prod-raw`) porque dev y prod comparten el **mismo metastore** y los nombres deben ser únicos.

# COMMAND ----------

contenedores = ["raw", "bronze", "silver", "golden", "metastore"]

for c in contenedores:
    spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-{ambiente}-{c}`
        URL '{ruta(c)}'
        WITH (STORAGE CREDENTIAL `{credencial}`)
        COMMENT 'Contenedor {c} del ambiente {ambiente} (acceso por Managed Identity)'
    """)
    print(f"OK -> exlt-{ambiente}-{c}")

display(spark.sql("SHOW EXTERNAL LOCATIONS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reset opcional
# MAGIC Solo se ejecuta si `reset = true`. Borra el catálogo (tablas lógicas) **y** los archivos físicos de bronze/silver/golden.
# MAGIC Ojo: al borrar una tabla **externa**, Unity Catalog NO borra sus archivos; por eso se limpian con `dbutils.fs.rm`.

# COMMAND ----------

if reset:
    spark.sql(f"DROP CATALOG IF EXISTS {catalogo} CASCADE")
    for c in ["bronze", "silver", "golden"]:
        for item in dbutils.fs.ls(ruta(c)):
            dbutils.fs.rm(item.path, True)
        print(f"Contenedor {c} limpio")
else:
    print("Reset desactivado: se conservan catálogo y datos (todo usa IF NOT EXISTS).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Catálogo y Schemas

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {catalogo}
    MANAGED LOCATION '{ruta("metastore", catalogo)}'
    COMMENT 'Catálogo medallion del ambiente {ambiente} - Proyecto Instacart'
""")

for esquema in ["bronze", "silver", "golden"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{esquema} COMMENT 'Capa {esquema}'")

display(spark.sql(f"SHOW SCHEMAS IN {catalogo}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tablas Bronze
# MAGIC Datos tal como vienen de raw + columnas de auditoría (`archivo_origen`, `ingestion_date`).
# MAGIC `products` guarda `columna_extra` a propósito: ahí llegan los datos desplazados de los productos corruptos, para repararlos en Silver.

# COMMAND ----------

def crear_tabla(esquema: str, tabla: str, columnas: str, particion: str = None):
    """Crea una tabla Delta externa con LOCATION en el contenedor de su capa."""
    particion_sql = f"PARTITIONED BY ({particion})" if particion else ""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalogo}.{esquema}.{tabla} ({columnas})
        USING DELTA
        {particion_sql}
        LOCATION '{ruta(esquema, tabla)}'
    """)
    print(f"OK -> {catalogo}.{esquema}.{tabla}")

crear_tabla("bronze", "orders", """
    order_id INT, user_id INT, eval_set STRING, order_number INT,
    order_dow INT, order_hour_of_day INT, days_since_prior_order DOUBLE,
    archivo_origen STRING, ingestion_date TIMESTAMP
""", particion="eval_set")

crear_tabla("bronze", "order_products", """
    order_id INT, product_id INT, add_to_cart_order INT, reordered INT,
    origen STRING, archivo_origen STRING, ingestion_date TIMESTAMP
""", particion="origen")

crear_tabla("bronze", "products", """
    product_id INT, product_name STRING, aisle_id INT, department_id INT,
    columna_extra STRING, archivo_origen STRING, ingestion_date TIMESTAMP
""")

crear_tabla("bronze", "aisles", """
    aisle_id INT, aisle STRING, archivo_origen STRING, ingestion_date TIMESTAMP
""")

crear_tabla("bronze", "departments", """
    department_id INT, department STRING, archivo_origen STRING, ingestion_date TIMESTAMP
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tablas Silver
# MAGIC Datos limpios, reparados, enriquecidos y unidos. `calidad_datos` registra cada regla de calidad aplicada (trazabilidad).

# COMMAND ----------

crear_tabla("silver", "products", """
    product_id INT, product_name STRING, aisle_id INT, aisle STRING,
    department_id INT, department STRING, producto_reparado BOOLEAN,
    fecha_proceso TIMESTAMP
""")

crear_tabla("silver", "orders", """
    order_id INT, user_id INT, eval_set STRING, order_number INT,
    order_dow INT, dia_semana STRING, order_hour_of_day INT, franja_horaria STRING,
    days_since_prior_order INT, es_primer_pedido BOOLEAN, fecha_proceso TIMESTAMP
""")

crear_tabla("silver", "order_items", """
    order_id INT, user_id INT, product_id INT, product_name STRING,
    aisle STRING, department STRING, add_to_cart_order INT, reordered INT,
    order_dow INT, dia_semana STRING, order_hour_of_day INT, franja_horaria STRING,
    fecha_proceso TIMESTAMP
""", particion="department")

crear_tabla("silver", "calidad_datos", """
    tabla STRING, regla STRING, registros_afectados BIGINT, accion STRING,
    fecha_proceso TIMESTAMP
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tablas Golden
# MAGIC Agregados listos para el dashboard.

# COMMAND ----------

crear_tabla("golden", "kpi_departamentos", """
    department STRING, aisle STRING, total_items BIGINT, total_pedidos BIGINT,
    total_recompras BIGINT, tasa_recompra DOUBLE, fecha_proceso TIMESTAMP
""")

crear_tabla("golden", "kpi_habitos_compra", """
    order_dow INT, dia_semana STRING, order_hour_of_day INT, franja_horaria STRING,
    total_pedidos BIGINT, promedio_items DOUBLE, fecha_proceso TIMESTAMP
""")

crear_tabla("golden", "top_productos_recompra", """
    ranking INT, product_id INT, product_name STRING, department STRING,
    veces_comprado BIGINT, veces_recomprado BIGINT, tasa_recompra DOUBLE,
    fecha_proceso TIMESTAMP
""")

crear_tabla("golden", "segmentacion_clientes", """
    user_id INT, total_pedidos INT, promedio_dias_entre_pedidos DOUBLE,
    total_items BIGINT, tasa_recompra DOUBLE, segmento STRING,
    fecha_proceso TIMESTAMP
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validación final

# COMMAND ----------

for esquema in ["bronze", "silver", "golden"]:
    print(f"--- {catalogo}.{esquema}")
    display(spark.sql(f"SHOW TABLES IN {catalogo}.{esquema}"))
