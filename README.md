# ETL Medallion con Azure Databricks - Instacart

> Trabajo Final - Ingeniería de Datos con Databricks (DataSmart)
> Autor: Johan Céspedes

*(Borrador en construcción: se completa a medida que avanza el proyecto.)*

## 1. Descripción del proyecto

ETL con arquitectura **Medallion** (Raw → Bronze → Silver → Golden) sobre **Azure Databricks + Unity Catalog**, usando datos de pedidos de supermercado en línea (dataset Instacart, Kaggle). El ETL está escrito en **PySpark**, se despliega de **desarrollo a producción** con **GitHub Actions** y se ejecuta como workflow en producción.

**Fuente de datos:** [Supermarket/Superstore dataset bundle - Kaggle](https://www.kaggle.com/datasets/amunsentom/supermarket-superstore-dataset-bundle)

## 2. Arquitectura

*(Pendiente: diagrama)*

## 3. Servicios utilizados

| Servicio | Recurso | Uso |
|---|---|---|
| Resource Group | `rg-smartdata-tf` | Agrupa todos los recursos del proyecto |
| ADLS Gen2 (dev) | `adlssmartdatadev0811` | Data Lake de desarrollo: contenedores raw, bronze, silver, golden, metastore |
| ADLS Gen2 (prod) | `adlssmartdataprod0811` | Data Lake de producción |
| Access Connector for Azure Databricks | `ac-smartdata-tf` | **Managed Identity** para acceder al Data Lake (sin llaves ni secretos) |
| Azure Databricks (dev) | `dbw-smartdata-dev` | Desarrollo de notebooks (Premium, Unity Catalog) |
| Azure Databricks (prod) | `dbw-smartdata-prod` | Ejecución del workflow productivo |
| Unity Catalog - Metastore propio | `metastore-smartdata-eastus2` (raíz: `abfss://ucmetastore@adlssmartdatadev0811...`) | Metastore creado manualmente desde la consola de cuenta (reemplaza el creado por defecto), asignado a los workspaces dev y prod |
| Unity Catalog | `cred-smartdata`, `exlt-{ambiente}-{contenedor}`, `catalog_dev` / `catalog_prod` | Gobierno de datos, permisos y linaje |
| GitHub Actions | *(pendiente)* | CI/CD dev → prod |

**Región:** East US 2 para todos los recursos (el Resource Group guarda sus metadatos en East US).

## 4. Seguridad: conexión con Managed Identity

Databricks accede al Data Lake únicamente mediante **Managed Identity**:

```
Databricks → Unity Catalog → Storage Credential (cred-smartdata)
           → Access Connector (ac-smartdata-tf) → RBAC "Storage Blob Data Contributor" → ADLS Gen2
```

- No se usan account keys, SAS tokens, DBFS ni Volúmenes para la capa raw.
- Rol asignado con **mínimo privilegio**: solo `Storage Blob Data Contributor` sobre cada storage account.

## 5. Datos y calidad de datos

*(Pendiente: diccionario de datos y hallazgos de calidad)*

## 6. Estructura del repositorio

*(Pendiente)*

## 7. CI/CD

*(Pendiente)*

## 8. Decisiones técnicas

| Decisión | Motivo |
|---|---|
| Dos storage accounts (dev y prod) | Aislar los datos de producción de los de desarrollo |
| External locations con sufijo de ambiente (`exlt-dev-raw`) | Dev y prod comparten el mismo metastore regional; los nombres deben ser únicos |
| Workspaces en tier **Premium** | Unity Catalog requiere Premium; el tier Trial no está disponible en suscripciones Azure Free Trial |
| Databricks Runtime **17.3 LTS** en dev y prod | Versión usada en el curso; misma versión en ambos ambientes para evitar diferencias de comportamiento |
| Cluster **single node** de 4 cores con auto-terminación (20 min) | Cuota de la suscripción gratuita (4 vCPU) y control de costos; un solo cluster encendido a la vez |

## 9. Bitácora de problemas y soluciones

| # | Problema | Causa | Solución |
|---|---|---|---|
| 1 | Falla al crear el workspace: *"Trial workspace creation is not supported for Azure Free Trial"* | El tier Trial de Databricks no se permite en suscripciones Free Trial | Se creó el workspace con tier **Premium** (costos cubiertos por el crédito de Azure, manteniendo el límite de gasto) |
| 2 | Cluster terminado: `CLOUD_PROVIDER_RESOURCE_STOCKOUT` | Sin capacidad de VMs `Standard_DS3_v2` en East US 2 | Se cambió el nodo a `Standard_D4ds_v5` (4 cores, 16 GB) |
| 3 | No se podía entrar a la consola de cuenta (`accounts.azuredatabricks.net`) | La cuenta personal de Outlook es un usuario *externo* del tenant | Se creó un usuario nativo del tenant con rol **Global Administrator** para administrar la cuenta de Databricks |
| 4 | Error al crear el metastore: *"Azure Managed Identity Credential with Access Connector Id ... could not be found"* | El usuario administrador (Entra ID) no tenía rol RBAC sobre los recursos de Azure | Se le asignó el rol **Reader** sobre `rg-smartdata-tf`. Se configuró como Metastore Admin la cuenta de trabajo |

## 10. Evidencias

*(Pendiente: capturas en la carpeta `evidencias/`)*
