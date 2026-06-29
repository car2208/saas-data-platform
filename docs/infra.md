# Infraestructura — Terraform para onboarding de tenants

## Recursos provisionados por Terraform

Para incorporar un nuevo tenant a la plataforma SaaS, Terraform automatiza la creación de la infraestructura necesaria en Azure y Databricks. El módulo de onboarding provisiona los siguientes recursos:

1. **Schemas en Unity Catalog.** Se crea un schema por capa de datos (`bronze_<tenant>`, `silver_<tenant>`, `gold_<tenant>`) dentro del catálogo correspondiente al ambiente (`saas_dev`, `saas_qa` o `saas_prod`).

2. **Estructura inicial en ADLS Gen2 (opcional).** Se crean los directorios base para las capas de datos y las zonas de cuarentena. Aunque Spark puede generar estas rutas automáticamente al escribir el primer Delta Lake, precrearlas facilita la organización y la gobernanza del Data Lake.

3. **Permisos sobre Unity Catalog.** Se asignan los privilegios necesarios al Service Principal utilizado por los pipelines de ingestión, aplicando permisos diferenciados según la capa de datos.

4. **Secretos en Databricks Secret Scope.** Se almacenan las credenciales de acceso a las fuentes operacionales del tenant (MongoDB, APIs, etc.). Se asume que el Secret Scope ya existe y es administrado por la plataforma.

---

## Módulo de Terraform

```hcl
variable "tenant_id" {
  type        = string
  description = "Código del tenant (ej. sv, gt, hn)"
}

variable "environment" {
  type        = string
  description = "Ambiente: dev, qa o prod"
}

variable "catalog_name" {
  type        = string
  default     = "saas"
  description = "Prefijo del catálogo de Unity Catalog"
}

variable "storage_account_id" {
  type = string
}

variable "filesystem_name" {
  type = string
}

variable "pipeline_service_principal" {
  type = string
}

variable "secret_scope_name" {
  type = string
}

variable "tenant_source_connection_string" {
  type      = string
  sensitive = true
}

locals {
  catalog_full = "${var.catalog_name}_${var.environment}"

  layers = ["bronze", "silver", "gold"]

  schema_privileges = {
    bronze = ["USE_SCHEMA", "SELECT"]
    silver = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
    gold   = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
}

# ----------------------------
# Unity Catalog Schemas
# ----------------------------

resource "databricks_schema" "tenant_schemas" {
  for_each     = toset(local.layers)

  catalog_name = local.catalog_full
  name         = "${each.value}_${var.tenant_id}"

  comment = "Schema ${each.value} para el tenant ${var.tenant_id}"
}

# ----------------------------
# ADLS Gen2 (estructura inicial)
# ----------------------------

resource "azurerm_storage_data_lake_gen2_path" "layer_paths" {
  for_each = toset(local.layers)

  storage_account_id = var.storage_account_id
  filesystem_name    = var.filesystem_name

  path     = "${each.value}/${var.tenant_id}"
  resource = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "quarantine_paths" {
  for_each = toset(["bronze", "silver"])

  storage_account_id = var.storage_account_id
  filesystem_name    = var.filesystem_name

  path     = "${each.value}_quarantine/${var.tenant_id}"
  resource = "directory"
}

# ----------------------------
# Unity Catalog Grants
# ----------------------------

resource "databricks_grants" "schema_grants" {
  for_each = databricks_schema.tenant_schemas

  schema = each.value.id

  grant {
    principal  = var.pipeline_service_principal
    privileges = local.schema_privileges[split("_", each.value.name)[0]]
  }
}

# ----------------------------
# Secreto del tenant
# ----------------------------

resource "databricks_secret" "tenant_source_credentials" {
  scope = var.secret_scope_name

  key = "${var.tenant_id}_source_connection"

  string_value = var.tenant_source_connection_string
}
```

---

## Uso del módulo

```hcl
module "tenant_sv" {
  source = "./modules/tenant_onboarding"

  tenant_id   = "sv"
  environment = "dev"

  storage_account_id              = azurerm_storage_account.datalake.id
  filesystem_name                 = "saasdata"

  pipeline_service_principal = "spn-saas-pipeline"

  secret_scope_name               = "saas-secrets"
  tenant_source_connection_string = var.sv_connection_string
}
```

---

## Flujo de onboarding

Para incorporar un nuevo tenant basta con invocar nuevamente el módulo indicando un nuevo `tenant_id`.

Terraform comparará el estado deseado con la infraestructura existente y creará únicamente los recursos que aún no existan, garantizando un despliegue idempotente y consistente entre ambientes.

El resultado para un tenant `sv` en el ambiente `dev` sería:

* **Catálogo:** `saas_dev`
* **Schemas:** `bronze_sv`, `silver_sv`, `gold_sv`
* **Directorios ADLS:** `bronze/sv`, `silver/sv`, `gold/sv`
* **Directorios de cuarentena:** `bronze_quarantine/sv`, `silver_quarantine/sv`
* **Permisos:** asignados automáticamente al Service Principal del pipeline según la capa.
* **Secreto:** `sv_source_connection`
