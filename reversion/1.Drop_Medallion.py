# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión del ambiente (rollback)
# MAGIC Deshace todo lo creado por el proceso: catálogo (schemas y tablas), archivos Delta de bronze/silver/golden y, opcionalmente, las External Locations.
# MAGIC
# MAGIC **No toca** el contenedor `raw` (la fuente) ni la Storage Credential.
# MAGIC Por seguridad solo se ejecuta si el widget `confirmar` = `SI`.

# COMMAND ----------

dbutils.widgets.text("storageName", "adlssmartdatadev0811")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.dropdown("borrar_external_locations", "false", ["false", "true"])
dbutils.widgets.dropdown("confirmar", "NO", ["NO", "SI"])

storage  = dbutils.widgets.get("storageName")
catalogo = dbutils.widgets.get("catalogo")
ambiente = dbutils.widgets.get("ambiente")
borrar_el = dbutils.widgets.get("borrar_external_locations") == "true"
confirmar = dbutils.widgets.get("confirmar") == "SI"

def ruta(contenedor: str) -> str:
    return f"abfss://{contenedor}@{storage}.dfs.core.windows.net/"

if not confirmar:
    dbutils.notebook.exit("Reversión cancelada: cambia el widget 'confirmar' a SI para ejecutar.")

# COMMAND ----------

# 1. Catálogo con todo su contenido
spark.sql(f"DROP CATALOG IF EXISTS {catalogo} CASCADE")
print(f"OK -> catálogo {catalogo} eliminado")

# 2. Archivos físicos (las tablas externas no borran sus archivos al hacer DROP)
for c in ["bronze", "silver", "golden"]:
    for item in dbutils.fs.ls(ruta(c)):
        dbutils.fs.rm(item.path, True)
    print(f"OK -> contenedor {c} limpio")

# 3. External Locations (opcional)
if borrar_el:
    for c in ["raw", "bronze", "silver", "golden", "metastore"]:
        spark.sql(f"DROP EXTERNAL LOCATION IF EXISTS `exlt-{ambiente}-{c}` FORCE")
        print(f"OK -> exlt-{ambiente}-{c} eliminada")

# COMMAND ----------

dbutils.notebook.exit(f"OK - Ambiente {ambiente} revertido")
