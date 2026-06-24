# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "elasticsearch8",
# ]
# ///
# MAGIC %md
# MAGIC # Writing Enriched Data to Elasticsearch
# MAGIC
# MAGIC **Block 5 — Elasticsearch Sink**
# MAGIC
# MAGIC This is the final step in our pipeline: writing enriched content to Elasticsearch so it can be searched by downstream applications.
# MAGIC
# MAGIC **Two approaches covered in this notebook:**
# MAGIC
# MAGIC | Approach | Best For | Mechanism |
# MAGIC |----------|----------|-----------|
# MAGIC | **es-hadoop Spark Connector** | Large-scale batch writes, streaming pipelines | Spark's `.write` / `.writeStream` API with ES-specific options |
# MAGIC | **Python `elasticsearch` Client** | Lower-volume writes, fine-grained control | Official Python SDK with `helpers.bulk()` |
# MAGIC
# MAGIC **Key architectural principle:** Delta tables remain the **primary store** (source of truth). Elasticsearch serves as a **search index** that can be rebuilt at any time from the Delta tables.
# MAGIC
# MAGIC > **Prerequisite:** The `elasticsearch-spark` (es-hadoop) connector must be installed on the cluster as a Maven library.

# COMMAND ----------

# DBTITLE 1,Load shared config
# MAGIC %run ./_resources/Config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secrets
# MAGIC Store sensitive information in Databricks secrets.  
# MAGIC 1. Set from commandline or API only.
# MAGIC 1. ACLs are set on secrets to restrict their visibility to only those who need them.
# MAGIC 3. Secrets can be retrieved in Python code.  
# MAGIC
# MAGIC
# MAGIC To set secrets:
# MAGIC - `databricks secrets create-scope <secret-scope>`
# MAGIC - `databricks secrets put-secret <secret-scope> <secret-name>` (you will be prompted to enter the value on the command-line)
# MAGIC
# MAGIC To set ACLs on secret scopes:
# MAGIC - `databricks secrets put-acl <secret-scope> <principal> <permission>`
# MAGIC
# MAGIC To retrieve secret values:
# MAGIC - `secretvalue = dbutils.secrets.get(<secret-scope>, <secret-name>)`
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Retrieve secrets
# Read secrets
api_key = dbutils.secrets.get("elasticsearch", "api_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data to be written

# COMMAND ----------

# DBTITLE 1,Retrieve data to be written to Elasticsearch
df_enriched = spark.table("03_gold_enriched_documents")
display(df_enriched)

# COMMAND ----------

# DBTITLE 1,Format data and actions
# Bulk write from DataFrame
import json

rows = json.loads(df_enriched.toPandas().to_json(orient="records"))
actions = [
    {
        "_index": elasticsearch_index,
        "_source": row
    } for row in rows
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 1 -- Python `elasticsearch` Client
# MAGIC
# MAGIC The official Python Elasticsearch client is a better fit when you need:
# MAGIC
# MAGIC - **Lower-volume writes** where Spark parallelism isn't necessary
# MAGIC - **Fine-grained control** over document structure, routing, or custom pipelines
# MAGIC - **Index management operations** (create/delete indices, update mappings, etc.)
# MAGIC
# MAGIC The trade-off is that all work happens on the driver node, so it won't scale as well for very large datasets. The Spark connector distributes writes across executors, while the Python client runs entirely on the driver. For our document volumes in this workshop, both approaches work fine.

# COMMAND ----------

# MAGIC %pip install elasticsearch8

# COMMAND ----------

# DBTITLE 1,Write to Elasticsearch
from elasticsearch8 import Elasticsearch, helpers

# Connect to Elasticsearch
es = Elasticsearch(
    f"https://{elasticsearch_host}:{elasticsearch_port}",
    # basic_auth=(
    #     username,
    #     password
    # ),
    api_key=api_key,
    verify_certs=True
)

# Verify connection
print(es.info())

success, errors = helpers.bulk(es, actions)
print(f"Indexed {success} documents, {len(errors)} errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approach 2 -- es-hadoop Spark Connector
# MAGIC
# MAGIC The es-hadoop connector is the best choice for large-scale batch writes and streaming pipelines:
# MAGIC
# MAGIC - Uses Spark's native `.write` API with Elasticsearch-specific options
# MAGIC - Distributes writes across all Spark executors for parallelism
# MAGIC - Supports **document-level upserts** via `es.mapping.id` -- if a document with the same ID already exists, it gets updated rather than duplicated
# MAGIC - Works with both batch and structured streaming via `foreachBatch`
# MAGIC
# MAGIC The es-hadoop connector treats ES as just another Spark data sink. You already know `.write.format(...)` from writing to Delta -- same pattern, different format string.
# MAGIC
# MAGIC > **_Note_** - Installing Maven libraries is not supported on Databricks Serverless compute.  To use this approach, you must use a Classic compute cluster.  You must also match the cluster versions with the available Maven libraries.  
# MAGIC >
# MAGIC > The nearest match for Elasticsearch `8.19.16` is the library `org.elasticsearch:elasticsearch-spark-30_2.12:8.9.0` which is compatible with Databricks Runtime `16.4 LTS (includes Apache Spark 3.5.2, Scala 2.12)`.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Write to Elasticsearch
# Write to Elasticsearch
from pyspark.sql.functions import col, md5, get_json_object

# The Maven driver does not support a variant data type.  Convert it to a string.
(df_enriched
    .withColumn("category", get_json_object(col("category").cast("string"), "$.response[0]"))
    .withColumn("doc_id", md5(col("fileName")))
    .write
    .format("org.elasticsearch.spark.sql")
    .option("es.nodes", elasticsearch_host)
    .option("es.port", elasticsearch_port)
    .option("es.net.ssl", "true")
    # .option("es.net.http.auth.user", elasticsearch_username)
    # .option("es.net.http.auth.pass", elasticsearch_password)
    .option("es.net.http.header.Authorization", f"ApiKey {api_key}")
    .option("es.nodes.wan.only", "true")
    .option("es.nodes.discovery", "false")
    .option("es.index.auto.create", "true")
    .option("es.mapping.id", "doc_id")
    .mode("overwrite")
    .save(elasticsearch_index))

print(f"Written {df_enriched.count()} documents to Elasticsearch index '{elasticsearch_index}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Configuration Options Explained
# MAGIC
# MAGIC | Option | Value | Why |
# MAGIC |--------|-------|-----|
# MAGIC | `es.nodes.wan.only` | `true` | **Required** when Elasticsearch is behind a load balancer or in a different network (e.g., Elastic Cloud). Prevents the connector from trying to discover and connect to internal node addresses. |
# MAGIC | `es.mapping.id` | `doc_id` | Maps a DataFrame column to the Elasticsearch document `_id`. This enables **idempotent upserts** -- re-running the write updates existing documents rather than creating duplicates. |
# MAGIC | `es.index.auto.create` | `true` | Automatically creates the index if it doesn't exist. Convenient for development; in production you may want to pre-create the index with a specific mapping. |
# MAGIC | `mode("append")` | -- | Adds documents to the existing index. Use `"overwrite"` to **replace the entire index** (deletes and recreates it). |
# MAGIC
# MAGIC > **Note:** The `es.mapping.id` option is the key to making writes idempotent. Without it, every run would create duplicate documents. With it, the connector performs an upsert based on the `doc_id` column.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Write with `foreachBatch`
# MAGIC
# MAGIC For real-time pipelines, use structured streaming with `foreachBatch` to write micro-batches to Elasticsearch as new data arrives in the Delta table.
# MAGIC
# MAGIC > **Presenter note:** This is the same `foreachBatch` pattern students may have seen with other sinks. The callback receives a static DataFrame (the micro-batch) and a batch ID, so we can reuse the exact same write logic from the batch example.

# COMMAND ----------

# DBTITLE 1,forEachBatch pattern
def write_to_elasticsearch(batch_df, batch_id):
    """Write a micro-batch to Elasticsearch"""
    (batch_df
        .withColumn("category", get_json_object(col("category").cast("string"), "$.response[0]"))
        .withColumn("doc_id", md5(col("fileName")))
        .write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", elasticsearch_host)
        .option("es.port", elasticsearch_port)
        .option("es.net.ssl", "true")
        # .option("es.net.http.auth.user", elasticsearch_username)
        # .option("es.net.http.auth.pass", elasticsearch_password)
        .option("es.net.http.header.Authorization", f"ApiKey {api_key}")
        .option("es.nodes.wan.only", "true")
        .option("es.nodes.discovery", "false")
        .option("es.index.auto.create", "true")
        .option("es.mapping.id", "doc_id")
        .mode("append")
        .save(elasticsearch_index))
    print(f"Batch {batch_id}: wrote {batch_df.count()} documents")

# Streaming write
query = (spark.readStream
    .table("03_gold_enriched_documents")
    .writeStream
    .foreachBatch(write_to_elasticsearch)
    .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/{personal_volume}/checkpoints/es_sink")
    .trigger(availableNow=True)
    .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture Recommendation
# MAGIC
# MAGIC ```
# MAGIC +-----------------+        +-------------------+        +-------------------+
# MAGIC |  Raw Documents  | -----> |   Delta Tables    | -----> |  Elasticsearch    |
# MAGIC |  (SharePoint,   |        |  (Source of Truth) |        |  (Search Index)   |
# MAGIC |   file stores)  |        |                   |        |                   |
# MAGIC +-----------------+        +-------------------+        +-------------------+
# MAGIC                             Primary Store                 Derived / Rebuildable
# MAGIC ```
# MAGIC
# MAGIC **Why this separation matters:**
# MAGIC
# MAGIC 1. **Delta tables are the source of truth.** All data lineage, versioning (time travel), and governance live here.
# MAGIC 2. **Elasticsearch is a search index, not a database.** It is optimized for full-text search and fast retrieval, but it should not be your only copy of the data.
# MAGIC 3. **You can rebuild the ES index at any time** by re-reading from Delta. This makes recovery from index corruption or schema changes straightforward.
# MAGIC 4. **The `foreachBatch` streaming pattern keeps ES in sync automatically.** As new or updated documents land in the Delta table, they flow through to Elasticsearch with minimal latency.
# MAGIC
# MAGIC > **The medallion architecture:** Delta is the gold layer; Elasticsearch is a serving layer on top of it. If ES goes down or gets corrupted, you haven't lost any data -- just re-sync from Delta.