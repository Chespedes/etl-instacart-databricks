# Databricks notebook source
# MAGIC %md
# MAGIC # 5. Grants (seguridad por capas)
# MAGIC Principio de **mínimo privilegio**: los consumidores de negocio (dashboard, analistas) solo pueden **leer la capa Golden**.
# MAGIC Bronze y Silver quedan reservadas al equipo de ingeniería (dueño del catálogo).
# MAGIC
# MAGIC Unity Catalog exige permisos en los 3 niveles: `USE CATALOG` → `USE SCHEMA` → `SELECT`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("grupo_lectura", "account users")   # grupo integrado de Databricks: todos los usuarios de la cuenta

catalogo = dbutils.widgets.get("catalogo")
grupo    = dbutils.widgets.get("grupo_lectura")

# COMMAND ----------

sentencias = [
    f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{grupo}`",
    f"GRANT USE SCHEMA ON SCHEMA {catalogo}.golden TO `{grupo}`",
    f"GRANT SELECT ON SCHEMA {catalogo}.golden TO `{grupo}`",
]

for s in sentencias:
    spark.sql(s)
    print("OK ->", s)

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalogo}.golden"))

# COMMAND ----------

dbutils.notebook.exit(f"OK - Grants aplicados a {grupo}")
