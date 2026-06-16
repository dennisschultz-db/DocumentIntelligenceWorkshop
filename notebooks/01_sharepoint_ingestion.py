# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # Block 2: SharePoint Ingestion
# MAGIC **Day 1 | 9:45 - 11:00 AM**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Agenda
# MAGIC | Section | Duration |
# MAGIC |---|---|
# MAGIC | Guided: Connecting to SharePoint | 30 min |
# MAGIC | Guided: Auto Loader for Incremental Processing | 30 min |
# MAGIC | Hands-on Exercise | 15 min |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Guided Section: Connecting to SharePoint (30 min)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Introduction to the SharePoint Connector
# MAGIC
# MAGIC Databricks provides a **native Beta connector for SharePoint Online** (available on DBR 17.3+) through **Lakeflow Connect**. This connector gives you first-class support for reading files directly from SharePoint document libraries into Spark DataFrames.
# MAGIC
# MAGIC **Key capabilities:**
# MAGIC - **OAuth authentication** managed via a Unity Catalog connection (`sharepoint_conn`) — no secrets in notebooks
# MAGIC - **Binary file reads** — ingest PDFs, PPTX, DOCX, and other file types as binary content
# MAGIC - **Incremental processing** with Auto Loader — only process new or modified files
# MAGIC - **Metadata access** via the `_sharepoint_metadata` column (Runtime 18+)
# MAGIC
# MAGIC **What this replaces in your current architecture:**
# MAGIC - Custom Lambda functions calling the Microsoft Graph API
# MAGIC - EventBridge rules for scheduling sync jobs
# MAGIC - Manual tracking of which files have already been processed
# MAGIC
# MAGIC All of that collapses into a few lines of Spark code.
# MAGIC
# MAGIC ![Lakeflow Connect for SharePoint](./images/sharepoint_connector.png)
# MAGIC
# MAGIC > **Presenter note:** Walk through the architecture diagram. Emphasize that the Unity Catalog connection stores the OAuth credentials centrally — no more rotating secrets in Lambda environment variables. The connection is created once by an admin and referenced by name in all notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Batch Read from SharePoint
# MAGIC
# MAGIC The simplest way to read files from SharePoint is a batch read using `spark.read` with the `binaryFile` format. Each file becomes a row in the resulting DataFrame with its path, modification time, size, and binary content.
# MAGIC
# MAGIC > **Presenter note:** Replace `TENANT` and `SITE` with the actual tenant name and SharePoint site before running. The connection `sharepoint_conn` must already exist in Unity Catalog.

# COMMAND ----------

# ============================================================
# Batch read binary files from SharePoint
# ============================================================
# NOTE: Replace TENANT with your SharePoint tenant name
#       Replace SITE with your SharePoint site name
# ============================================================

df = (spark.read
    .format("binaryFile")
    .option("databricks.connection", "sharepoint_conn")
    .option("recursiveFileLookup", True)
    .option("pathGlobFilter", "*.{pdf,pptx,docx}")
    .load("https://TENANT.sharepoint.com/sites/SITE/Shared%20Documents"))

display(df.select("path", "modificationTime", "length"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. SQL Approach with `read_files()`
# MAGIC
# MAGIC If you prefer SQL, Databricks also exposes SharePoint reads through the `read_files()` table-valued function. This is especially useful for analysts who want to explore SharePoint content without writing Python.
# MAGIC
# MAGIC > **Presenter note:** This is the same operation as cell 2, just expressed in SQL. Point out that the connection name and options are passed as named parameters. This also works in dashboards and scheduled SQL queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SQL approach: read binary files from SharePoint
# MAGIC -- NOTE: Replace TENANT with your SharePoint tenant name
# MAGIC --       Replace SITE with your SharePoint site name
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT path, length, modificationTime
# MAGIC FROM read_files(
# MAGIC   'https://TENANT.sharepoint.com/sites/SITE/Shared%20Documents',
# MAGIC   `databricks.connection` => 'sharepoint_conn',
# MAGIC   format => 'binaryFile',
# MAGIC   pathGlobFilter => '*.{pdf,pptx,docx}',
# MAGIC   schemaEvolutionMode => 'none'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. File Type Filtering with `pathGlobFilter`
# MAGIC
# MAGIC The `pathGlobFilter` option lets you control which file types are ingested at the source — before any data is loaded into memory.
# MAGIC
# MAGIC **Common patterns:**
# MAGIC
# MAGIC | Pattern | What it matches |
# MAGIC |---|---|
# MAGIC | `*.pdf` | Only PDF files |
# MAGIC | `*.pptx` | Only PowerPoint files |
# MAGIC | `*.docx` | Only Word documents |
# MAGIC | `*.{pdf,pptx,docx}` | PDFs, PowerPoints, and Word docs |
# MAGIC | `*.{pdf,pptx,docx,xlsx}` | Add Excel spreadsheets too |
# MAGIC
# MAGIC **What this replaces:**
# MAGIC
# MAGIC In your current Lambda-based pipeline, you likely have folder-exclusion logic or file-extension checks in Python code. With `pathGlobFilter`, that logic moves into the connector itself — fewer lines of code, fewer bugs, and filtering happens before data transfer.
# MAGIC
# MAGIC > **Presenter note:** Ask the audience what file types they currently ingest. Some teams may also need `.xlsx` or `.csv`. The glob pattern is flexible — you can add any extension to the curly-brace list.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. SharePoint Metadata Exploration
# MAGIC
# MAGIC Starting with **Databricks Runtime 18+**, the SharePoint connector exposes a special `_sharepoint_metadata` struct column that contains rich metadata from the SharePoint API.
# MAGIC
# MAGIC **Available fields include:**
# MAGIC - `mime_type` — the MIME type of the file (e.g., `application/pdf`)
# MAGIC - `created_by_email` — email of the user who uploaded the file
# MAGIC - `last_modified_by_name` — display name of the last modifier
# MAGIC - `parent_path` — the folder path within the SharePoint document library
# MAGIC
# MAGIC This metadata lets you filter, tag, and route documents based on who created them, where they live, or what type they are — all without parsing filenames.
# MAGIC
# MAGIC > **Presenter note:** This is a Runtime 18+ feature. If the cluster is on an earlier runtime, the `_sharepoint_metadata` column will not be available. Check the cluster runtime version before running this cell.

# COMMAND ----------

# ============================================================
# Explore SharePoint metadata (requires Runtime 18+)
# NOTE: Replace TENANT with your SharePoint tenant name
#       Replace SITE with your SharePoint site name
# ============================================================

df_meta = (spark.read
    .format("binaryFile")
    .option("databricks.connection", "sharepoint_conn")
    .load("https://TENANT.sharepoint.com/sites/SITE/Shared%20Documents")
    .select("path", "length", "_sharepoint_metadata"))

display(df_meta)

# COMMAND ----------

# Filter by metadata — e.g., only PDF files based on MIME type
df_meta.filter("_sharepoint_metadata.mime_type = 'application/pdf'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Guided Section: Auto Loader for Incremental Processing (30 min)

# COMMAND ----------

# DBTITLE 1,Cell 14
# MAGIC %md
# MAGIC ### 6. Auto Loader Introduction
# MAGIC
# MAGIC **Auto Loader** (`cloudFiles` format) is Databricks' built-in solution for incrementally processing new files as they arrive. When combined with the SharePoint connector, it replaces the need for:
# MAGIC
# MAGIC - **EventBridge rules** that trigger on a schedule
# MAGIC - **Lambda functions** that poll the Graph API for changes
# MAGIC - **Custom DynamoDB tables** that track which files have been processed
# MAGIC
# MAGIC All of that is handled automatically by Auto Loader's checkpoint mechanism.
# MAGIC
# MAGIC ![Lakeflow Connect Architecture](./images/lakeflow_connect_unified.png)
# MAGIC
# MAGIC **How it works:**
# MAGIC
# MAGIC | Run | Behavior | What gets processed |
# MAGIC |---|---|---|
# MAGIC | First run | **Full crawl** (`includeExistingFiles=True`) | ALL existing files in the SharePoint library |
# MAGIC | Subsequent runs | **Incremental** | Only new or modified files since last checkpoint |
# MAGIC
# MAGIC > **Presenter note:** Emphasize the operational simplicity here. Their current architecture has at least three AWS services (EventBridge, Lambda, DynamoDB) doing what Auto Loader does out of the box. The checkpoint location is just a path — no infrastructure to manage.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Streaming Read with Auto Loader
# MAGIC
# MAGIC The code below sets up a streaming pipeline that:
# MAGIC 1. Reads all existing files on the first run (full crawl)
# MAGIC 2. On subsequent runs, processes only new/modified files (incremental)
# MAGIC 3. Writes everything to a Delta table (`workshop.default.raw_documents`)
# MAGIC 4. Uses `trigger(availableNow=True)` to process all available files and then stop
# MAGIC
# MAGIC > **Presenter note:** Walk through each option. `availableNow=True` is key — it processes everything available right now and stops, which is ideal for scheduled jobs. For real-time ingestion, you would use `trigger(processingTime="5 minutes")` instead.

# COMMAND ----------

# ============================================================
# Auto Loader: streaming read from SharePoint
# NOTE: Replace TENANT with your SharePoint tenant name
#       Replace SITE with your SharePoint site name
# ============================================================

df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.includeExistingFiles", True)
    .option("databricks.connection", "sharepoint_conn")
    .option("pathGlobFilter", "*.{pdf,pptx,docx}")
    .load("https://TENANT.sharepoint.com/sites/SITE/Shared%20Documents"))

# Write to a Delta table with checkpoint tracking
(df_stream.writeStream
    .option("checkpointLocation", "/Volumes/workshop/default/checkpoints/raw_docs")
    .trigger(availableNow=True)
    .toTable("workshop.default.raw_documents"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Full vs. Incremental Crawl
# MAGIC
# MAGIC Understanding how Auto Loader manages state is critical for building reliable pipelines.
# MAGIC
# MAGIC **First run (Full Crawl):**
# MAGIC - `includeExistingFiles=True` tells Auto Loader to process **all existing files** in the SharePoint library
# MAGIC - Every file is read, converted to a row, and written to the Delta table
# MAGIC - A checkpoint is saved recording what has been processed
# MAGIC
# MAGIC **Subsequent runs (Incremental):**
# MAGIC - Auto Loader reads the checkpoint to determine what has already been processed
# MAGIC - Only **new or modified files** since the last run are picked up
# MAGIC - The checkpoint is updated after each successful run
# MAGIC
# MAGIC **Key points:**
# MAGIC - The **checkpoint location** (`/Volumes/workshop/default/checkpoints/raw_docs`) is where Auto Loader stores its state
# MAGIC - If you delete the checkpoint, the next run becomes a full crawl again
# MAGIC - You do **not** need to build custom tracking logic — Auto Loader handles it
# MAGIC - This is exactly what your Lambda + DynamoDB tracker was doing, but with zero custom code
# MAGIC
# MAGIC > **Presenter note:** Ask the audience how they currently track which files have been processed. Most teams have a DynamoDB table or a metadata database for this. Point out that the checkpoint replaces all of that. If someone asks about reprocessing, explain that deleting the checkpoint forces a full re-crawl.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Hands-on Exercise (15 min)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now it's your turn!
# MAGIC
# MAGIC In this exercise, you will practice reading and exploring document files using Spark's `binaryFile` format. We will use a pre-loaded Volume as the data source so that the exercises work even if SharePoint connectivity is not yet configured.
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Run each exercise cell below
# MAGIC 2. Look for `TODO` comments where you need to fill in code
# MAGIC 3. Compare your results with your neighbor
# MAGIC
# MAGIC **Data source:** `/Volumes/workshop/default/documents/` contains sample PDF, PPTX, and DOCX files.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exercise 1: Read Documents from the Workshop Volume
# MAGIC
# MAGIC Read all binary files from the pre-loaded Volume and inspect their paths, modification times, and sizes.

# COMMAND ----------

# ============================================================
# Exercise 1: Read documents from the workshop Volume
# ============================================================

df = (spark.read
    .format("binaryFile")
    .load("/Volumes/workshop/default/documents/"))

display(df.select("path", "modificationTime", "length"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exercise 2: Filter to Only PDF Files
# MAGIC
# MAGIC Using the DataFrame from Exercise 1, filter the results to include only `.pdf` files.

# COMMAND ----------

# ============================================================
# Exercise 2: Filter to only PDF files
# TODO: Add a filter for .pdf files only
# ============================================================

df_pdf = df.filter("path LIKE '%.pdf'")
display(df_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exercise 3: Count Files by Extension
# MAGIC
# MAGIC Extract the file extension from each path and count how many files exist for each type.

# COMMAND ----------

# ============================================================
# Exercise 3: Count files by extension
# ============================================================
from pyspark.sql.functions import regexp_extract

df_types = (df
    .withColumn("extension", regexp_extract("path", r"\.([^.]+)$", 1))
    .groupBy("extension")
    .count()
    .orderBy("count", ascending=False))

display(df_types)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Recap
# MAGIC
# MAGIC In this block you learned:
# MAGIC
# MAGIC 1. **SharePoint connector** — read files directly from SharePoint using `binaryFile` format with a Unity Catalog connection
# MAGIC 2. **File filtering** — use `pathGlobFilter` to select specific file types at the source
# MAGIC 3. **Metadata** — access SharePoint metadata via `_sharepoint_metadata` (Runtime 18+)
# MAGIC 4. **Auto Loader** — incrementally process new files without custom tracking infrastructure
# MAGIC 5. **Full vs. Incremental** — first run does a full crawl; subsequent runs only pick up changes
# MAGIC
# MAGIC **Next up:** Block 3 — Document Parsing & Chunking