# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Create Elasticsearch Secrets
# MAGIC
# MAGIC This utility notebook creates the `elasticsearch` Databricks secret scope and populates the three secrets required by the workshop pipeline.
# MAGIC
# MAGIC | Secret | Purpose |
# MAGIC |--------|---------|
# MAGIC | `username` | Elasticsearch username (basic auth) |
# MAGIC | `password` | Elasticsearch password (basic auth) |
# MAGIC | `api_key` | Elasticsearch API key (serverless / cloud auth) |
# MAGIC
# MAGIC **Secrets are:**
# MAGIC - Stored encrypted in the Databricks Secret Store — never visible in notebook output
# MAGIC - Accessible at runtime via `dbutils.secrets.get("elasticsearch", "<key>")`
# MAGIC - Governed by ACLs that restrict which principals can read them
# MAGIC
# MAGIC > **Run once** per workspace. Re-running is safe — scope creation is idempotent and existing secret values will be overwritten.

# COMMAND ----------

# DBTITLE 1,Widget inputs
dbutils.widgets.text("username", "", "Elasticsearch Username")
dbutils.widgets.text("password", "", "Elasticsearch Password")
dbutils.widgets.text("api_key",  "", "Elasticsearch API Key")
dbutils.widgets.text("group_name", "", "Group Name")

# COMMAND ----------

# DBTITLE 1,CLI reference
# MAGIC %md
# MAGIC ## SDK and CLI Reference
# MAGIC
# MAGIC The cells below use the **Databricks Python SDK** (`WorkspaceClient`), which works natively on Serverless compute. The equivalent Databricks CLI commands are shown here for reference — run them from a local terminal with the Databricks CLI installed and configured.
# MAGIC
# MAGIC ```sh
# MAGIC # 1. Create the secret scope
# MAGIC databricks secrets create-scope elasticsearch
# MAGIC
# MAGIC # 2. Store the three secrets
# MAGIC databricks secrets put-secret elasticsearch username --string-value "<value>"
# MAGIC databricks secrets put-secret elasticsearch password --string-value "<value>"
# MAGIC databricks secrets put-secret elasticsearch api_key  --string-value "<value>"
# MAGIC
# MAGIC # 3. Grant READ access to the group
# MAGIC databricks secrets put-acl elasticsearch "Elasticsearch Users" READ
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Gather values
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# --- Collect widget values ---
username_val = dbutils.widgets.get("username")
password_val = dbutils.widgets.get("password")
api_key_val  = dbutils.widgets.get("api_key")
group_name   = dbutils.widgets.get("group_name")


# COMMAND ----------

# DBTITLE 1,Create Scope
# --- Step 1: Create the scope ---
try:
    w.secrets.create_scope(scope="elasticsearch")
    print("Created scope: elasticsearch")
except Exception as e:
    if "already exists" in str(e).lower():
        print("Scope 'elasticsearch' already exists — skipping creation")
    else:
        raise



# COMMAND ----------

# DBTITLE 1,Ceate secrets
# --- Step 2: Set the three secrets ---
# for key, val in [("username", username_val), ("password", password_val), ("api_key", api_key_val)]:
#     w.secrets.put_secret(scope="elasticsearch", key=key, string_value=val)
#     print(f"Set secret: {key}")

w.secrets.put_secret(scope="elasticsearch", key="api_key", string_value=api_key_val)

# COMMAND ----------

# DBTITLE 1,Set ACLs
from databricks.sdk.service.workspace import AclPermission

# --- Step 3: Grant READ ACL to the group ---
w.secrets.put_acl(scope="elasticsearch", principal=group_name, permission=AclPermission.READ)
print(f"ACL set: '{group_name}' -> READ")

print("\nAll done.")

# COMMAND ----------

# DBTITLE 1,Verify
# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Run the cell below to confirm the scope, secrets, and ACL were created correctly.

# COMMAND ----------

# DBTITLE 1,List secrets and ACLs
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print("=== Secrets in scope 'elasticsearch' ===")
for s in w.secrets.list_secrets(scope="elasticsearch"):
    print(f"  {s.key}")

print("")
print("=== ACLs on scope 'elasticsearch' ===")
for a in w.secrets.list_acls(scope="elasticsearch"):
    print(f"  {a.principal} -> {a.permission.value}")