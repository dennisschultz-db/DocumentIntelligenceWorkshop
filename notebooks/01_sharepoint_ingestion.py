# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "pypdf",
# ]
# ///
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

# MAGIC %run ./_resources/Config

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Batch Read from SharePoint
# MAGIC
# MAGIC The simplest way to read files from SharePoint is a batch read using `spark.read` with the `binaryFile` format. Each file becomes a row in the resulting DataFrame with its path, modification time, size, and binary content.
# MAGIC
# MAGIC > **Presenter note:** Replace `TENANT` and `SITE` with the actual tenant name and SharePoint site before running. The connection `sharepoint_conn` must already exist in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Read SharePoint - Python
# ============================================================
# Batch read binary files from SharePoint
# ============================================================
from pyspark.sql.functions import regexp_extract, substring

df = (spark.read
    .format("binaryFile")
    .option("databricks.connection", sharepoint_connector)
    .option("recursiveFileLookup", True)
    .load(f"{sharepoint_site_url}/Shared%20Documents")
    .withColumn("fileName", regexp_extract("path", r"([^/]+)$", 1))
)

display(df.select("fileName", "path", "modificationTime", "length", substring("content", 1, 100).alias("content_preview")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. File Type Filtering with `pathGlobFilter`
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

# COMMAND ----------

# DBTITLE 1,Read SharePoint - pathGlobFilter
# ============================================================
# Batch read binary files from SharePoint
# ============================================================

df = (spark.read
    .format("binaryFile")
    .option("databricks.connection", sharepoint_connector)
    .option("recursiveFileLookup", True)
    .option("pathGlobFilter", "*.pdf")
    .load(f"{sharepoint_site_url}/Shared%20Documents")
    .withColumn("fileName", regexp_extract("path", r"([^/]+)$", 1))
)

display(df.select("fileName", "path", "modificationTime", "length", substring("content", 1, 100).alias("content_preview")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. SharePoint Metadata Exploration
# MAGIC
# MAGIC The SharePoint connector exposes a special `_sharepoint_metadata` struct column that contains rich metadata from the SharePoint API.
# MAGIC
# MAGIC **Available fields include:**
# MAGIC - `mime_type` — the MIME type of the file (e.g., `application/pdf`)
# MAGIC - `created_by_email` — email of the user who uploaded the file
# MAGIC - `last_modified_by_name` — display name of the last modifier
# MAGIC - `parent_path` — the folder path within the SharePoint document library
# MAGIC
# MAGIC This metadata lets you filter, tag, and route documents based on who created them, where they live, or what type they are — all without parsing filenames.
# MAGIC

# COMMAND ----------

# DBTITLE 1,SharePoint Metadata
# ============================================================
# Explore SharePoint metadata
# ============================================================

df_meta = (spark.read
    .format("binaryFile")
    .option("databricks.connection", sharepoint_connector)
    .load(f"{sharepoint_site_url}/Shared%20Documents")
    .select("path", "length", "_sharepoint_metadata"))

display(df_meta)

# COMMAND ----------

# DBTITLE 1,Filter on SharePoint Metadata
# Filter by metadata — e.g., only PDF files based on MIME type
df_meta.filter("_sharepoint_metadata.mime_type = 'application/pdf'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Guided Section: Auto Loader for Incremental Processing (30 min)

# COMMAND ----------

# DBTITLE 1,Cell 14
# MAGIC %md
# MAGIC ### 5. Auto Loader Introduction
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Streaming Read with Auto Loader
# MAGIC
# MAGIC The code below sets up a streaming pipeline that:
# MAGIC 1. Reads all existing files on the first run (full crawl)
# MAGIC 2. On subsequent runs, processes only new/modified files (incremental)
# MAGIC 3. Writes everything to a Delta table (`01_bronze_raw_documents`)
# MAGIC 4. Uses `trigger(availableNow=True)` to process all available files and then stop
# MAGIC
# MAGIC > `availableNow=True` processes everything available right now and stops, which is ideal for scheduled jobs. 
# MAGIC
# MAGIC >For real-time ingestion, you would use something like `trigger(processingTime="5 minutes")` instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Detect page count per PDF
# MAGIC
# MAGIC Knowing the number of pages in PDF documents will be important when parsing documents in the next module.
# MAGIC
# MAGIC The notebook `./_resources/Utilities` defines a Pandas User Defined Function (UDF) that uses the `pypdf` library to determine the page count for PDF documents. 
# MAGIC - Bad/encrypted files return `-1` so they surface in downstream filters without killing the job
# MAGIC - Non-PDF documents also return `-1`

# COMMAND ----------

# MAGIC %run ./_resources/Utilities

# COMMAND ----------

# DBTITLE 1,Autoload files from SharePoint
# ============================================================
# Auto Loader: streaming read from SharePoint
# ============================================================

df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.includeExistingFiles", True)
    .option("databricks.connection", sharepoint_connector)
    .option("pathGlobFilter", "*.{pdf,pptx,docx}")
    .load(f"{sharepoint_site_url}/Shared%20Documents")
    .withColumn("fileName", regexp_extract("path", r"([^/]+)$", 1))
    .withColumn("page_count", pdf_page_count("content"))
)

# Write to a Delta table with checkpoint tracking
query = (df_stream.writeStream
    .option("checkpointLocation", f"/Volumes/{catalog}/{schema}/{personal_volume}/checkpoints/raw_docs")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.01_bronze_raw_documents"))

# COMMAND ----------

# DBTITLE 1,Examine the Bronze table
display(spark.read.table(f"{catalog}.{schema}.01_bronze_raw_documents")
        .select("fileName", "path", "modificationTime", "length", substring("content", 1, 100)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental ingestion
# MAGIC 1.  Drag a new file (pdf, pptx, or doc) into SharePoint
# MAGIC 1.  Rerun the cell `Autoload files from SharePoint`
# MAGIC 1.  Rerun the cell `Examine the Bronze table`.  Note there is one additional row.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Full vs. Incremental Crawl
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
# MAGIC - The **checkpoint location** (`/Volumes/{catalog}/{schema}/{volume}/checkpoints/raw_docs`) is where Auto Loader stores its state
# MAGIC - If you delete the checkpoint, the next run becomes a full crawl again
# MAGIC - You do **not** need to build custom tracking logic — Auto Loader handles it
# MAGIC
# MAGIC > How do you currently track which files have been processed? Most teams have a DynamoDB table or a metadata database for this. The checkpoint replaces all of that.

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