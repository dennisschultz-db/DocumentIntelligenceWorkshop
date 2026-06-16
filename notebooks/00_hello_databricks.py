# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Welcome to the Document Processing Workshop
# MAGIC
# MAGIC ## Workshop Objectives
# MAGIC
# MAGIC Over the next two days, we will **prove out Databricks as a replacement and augmentation** for your current AWS-based document processing system. By the end of this workshop, you will have a working pipeline that demonstrates how a single platform can replace a complex web of microservices, queues, and orchestration layers.
# MAGIC
# MAGIC **What you will walk away with:**
# MAGIC - A working document processing pipeline built entirely on Databricks
# MAGIC - Hands-on experience with Unity Catalog, Delta Lake, Auto Loader, and Lakeflow Declarative Pipelines
# MAGIC - A clear understanding of how Databricks maps to (and simplifies) your current architecture
# MAGIC - Concrete evidence to support a build-vs-migrate decision
# MAGIC
# MAGIC ## Two-Day Agenda Overview
# MAGIC
# MAGIC | Block | Time | Topic |
# MAGIC |-------|------|-------|
# MAGIC | **Day 1, Block 1** | 8:00 - 9:30 | **Foundations** — Platform tour, Delta Lake, Medallion Architecture (you are here) |
# MAGIC | **Day 1, Block 2** | 10:00 - 12:00 | **Ingestion** — SharePoint connector, Auto Loader, Bronze layer |
# MAGIC | **Day 1, Block 3** | 1:00 - 3:00 | **Parsing & Enrichment** — Document parsing, LLM enrichment, Silver/Gold layers |
# MAGIC | **Day 1, Block 4** | 3:30 - 5:00 | **Orchestration** — Lakeflow Declarative Pipelines, end-to-end pipeline |
# MAGIC | **Day 2, Block 5** | 8:00 - 10:00 | **Search & Serving** — Elasticsearch sync, vector search, serving endpoints |
# MAGIC | **Day 2, Block 6** | 10:30 - 12:00 | **Production Readiness** — Monitoring, governance, cost analysis |
# MAGIC | **Day 2, Block 7** | 1:00 - 3:00 | **Advanced Topics** — Custom models, feedback loops, edge cases |
# MAGIC | **Day 2, Block 8** | 3:30 - 5:00 | **Wrap-Up** — Architecture review, roadmap discussion, next steps |
# MAGIC
# MAGIC ## What We Will Build
# MAGIC
# MAGIC Our end-to-end pipeline follows this flow:
# MAGIC
# MAGIC **SharePoint Ingestion --> Document Parsing --> LLM Enrichment --> Elasticsearch**
# MAGIC
# MAGIC Specifically:
# MAGIC 1. **Ingest** documents from SharePoint into Unity Catalog Volumes
# MAGIC 2. **Parse** PDFs, Word docs, and images to extract text and structured content
# MAGIC 3. **Enrich** parsed content using LLMs for summarization, classification, and tagging
# MAGIC 4. **Publish** enriched documents to Elasticsearch for search and retrieval
# MAGIC
# MAGIC All of this will run as a single, declarative pipeline on Databricks — no queues, no microservices, no glue code.
# MAGIC
# MAGIC > **Presenter note:** Emphasize that participants do not need prior Databricks experience. Everything will be guided step by step. Encourage questions at any point.

# COMMAND ----------

# MAGIC %md
# MAGIC # Architecture Comparison: Current State vs. What We Will Build
# MAGIC
# MAGIC ## Your Current System
# MAGIC
# MAGIC Your existing document processing architecture is a distributed system built on AWS with many moving parts:
# MAGIC
# MAGIC | Component | Count | Role |
# MAGIC |-----------|-------|------|
# MAGIC | **SQS Queues** | 13+ | Message passing between stages, backpressure management, retry queues |
# MAGIC | **Microservices** | 8+ | Parsing Service, Enrichment Service, Tagging Service, Embedding Service, Conversion Service, and others |
# MAGIC | **Step Functions** | Multiple | Orchestration of multi-step document workflows |
# MAGIC | **DynamoDB** | Multiple tables | State management, deduplication tracking, processing status |
# MAGIC | **Lambda Triggers** | Many | Event-driven glue between S3, SQS, and services |
# MAGIC | **S3 Buckets** | Multiple | Staging areas between processing stages |
# MAGIC
# MAGIC **Pain points you have told us about:**
# MAGIC - Debugging failures requires tracing through multiple queues and services
# MAGIC - Adding a new processing step means creating a new service, queue, and Lambda trigger
# MAGIC - State management across DynamoDB tables is fragile and hard to reason about
# MAGIC - Reprocessing requires manually replaying messages through queues
# MAGIC - Monitoring is scattered across CloudWatch, X-Ray, and custom dashboards
# MAGIC
# MAGIC ## What We Will Build Today
# MAGIC
# MAGIC A **single Databricks platform** with a **declarative pipeline** that replaces all of the above:
# MAGIC
# MAGIC | Current AWS Component | Databricks Replacement |
# MAGIC |----------------------|----------------------|
# MAGIC | S3 staging buckets | Unity Catalog Volumes + Delta tables |
# MAGIC | SQS queues (13+) | Delta tables (each stage writes to a table; next stage reads) |
# MAGIC | Lambda triggers | Auto Loader (automatic incremental file detection) |
# MAGIC | Step Functions | Lakeflow Declarative Pipelines (single Python file) |
# MAGIC | DynamoDB state tables | Delta table metadata + checkpoints |
# MAGIC | 8+ microservices | Notebook functions within a single pipeline |
# MAGIC | CloudWatch + X-Ray | Built-in pipeline monitoring + Databricks SQL dashboards |
# MAGIC
# MAGIC ![Databricks Data Intelligence Platform](/Volumes/workshop/default/images/platform_overview.png)
# MAGIC
# MAGIC > **Presenter note:** Spend time on this comparison. The goal is not to criticize their current architecture — it was built for good reasons — but to show how a platform approach collapses operational complexity. Ask participants which pain points resonate most with their day-to-day experience.

# COMMAND ----------

# MAGIC %md
# MAGIC # Workspace Tour
# MAGIC
# MAGIC Let's get familiar with the Databricks workspace. If you have not already, log in to your workshop environment now.
# MAGIC
# MAGIC ## Key Navigation Areas
# MAGIC
# MAGIC ### 1. Workspace Browser (left sidebar)
# MAGIC The **Workspace** section is your file system for notebooks, libraries, and other assets. Think of it like a shared drive where all your code lives. You can organize notebooks into folders, and everything is version-controlled automatically.
# MAGIC
# MAGIC ### 2. Catalog Explorer (left sidebar --> "Catalog")
# MAGIC The **Catalog Explorer** is where you browse all your data assets — tables, views, volumes, functions, and models. This is powered by **Unity Catalog**, which we will use extensively throughout the workshop.
# MAGIC
# MAGIC ### 3. SQL Editor (left sidebar --> "SQL Editor")
# MAGIC A dedicated SQL editing experience for writing and running queries against your data. Similar to tools like DBeaver or DataGrip, but built into the platform with access to all your governed data.
# MAGIC
# MAGIC ### 4. Compute (left sidebar --> "Compute")
# MAGIC This is where you manage your compute resources — clusters and SQL warehouses. For this workshop, we have pre-configured clusters for you, so you should not need to create any new ones.
# MAGIC
# MAGIC ## Unity Catalog Hierarchy
# MAGIC
# MAGIC Unity Catalog organizes data in a **three-level namespace**:
# MAGIC
# MAGIC ```
# MAGIC Catalog
# MAGIC   --> Schema (also called Database)
# MAGIC       --> Tables        (structured data in Delta format)
# MAGIC       --> Views         (virtual tables defined by queries)
# MAGIC       --> Volumes       (unstructured files — PDFs, images, etc.)
# MAGIC       --> Functions     (user-defined functions)
# MAGIC       --> Models        (ML models registered in Unity Catalog)
# MAGIC ```
# MAGIC
# MAGIC For this workshop, our hierarchy looks like:
# MAGIC ```
# MAGIC workshop                        <-- Catalog
# MAGIC   --> default                    <-- Schema
# MAGIC       --> documents (Volume)     <-- Where raw files live
# MAGIC       --> images (Volume)        <-- Workshop images
# MAGIC       --> bronze_documents       <-- Table (we will create this)
# MAGIC       --> silver_parsed          <-- Table (we will create this)
# MAGIC       --> gold_enriched          <-- Table (we will create this)
# MAGIC ```
# MAGIC
# MAGIC ## What Is a Notebook?
# MAGIC
# MAGIC You are looking at a **Databricks notebook** right now. Here is what you need to know:
# MAGIC
# MAGIC - **Cells** are the building blocks. Each cell can contain code, SQL, markdown, or shell commands.
# MAGIC - **Multi-language support**: The default language for this notebook is Python, but you can switch any cell to SQL (`%sql`), Scala (`%scala`), R (`%r`), or shell (`%sh`) using magic commands.
# MAGIC - **Compute attachment**: A notebook must be attached to a compute resource (cluster) to run code. Check the top-right of this notebook to see which cluster you are connected to.
# MAGIC - **Run cells** by pressing `Shift+Enter` or clicking the play button. Run all cells from the "Run" menu at the top.
# MAGIC
# MAGIC ![Databricks Platform Architecture](/Volumes/workshop/default/images/platform_aws_governance.png)
# MAGIC
# MAGIC > **Presenter note:** Do a live walkthrough of the workspace. Open Catalog Explorer and show the `workshop` catalog. Navigate into the `default` schema and show the Volumes. Have participants follow along and confirm they can see the same assets. If anyone cannot see the catalog, check their permissions before proceeding.

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Lake: The Foundation of Everything
# MAGIC
# MAGIC ## What Is Delta Lake?
# MAGIC
# MAGIC **Delta Lake** is an open-source storage layer that brings reliability and performance to data lakes. Every table in Databricks is a Delta table by default. Think of it as "a database that stores data as files" — you get the best of both worlds.
# MAGIC
# MAGIC ## Key Capabilities
# MAGIC
# MAGIC ### ACID Transactions
# MAGIC Every write to a Delta table is **atomic, consistent, isolated, and durable**. This means:
# MAGIC - No partial writes — a write either fully succeeds or fully fails
# MAGIC - Concurrent readers and writers do not interfere with each other
# MAGIC - No more corrupted data from failed jobs or concurrent access
# MAGIC
# MAGIC Compare this to writing raw files to S3, where a failed job can leave partial files that downstream consumers pick up, causing cascading errors.
# MAGIC
# MAGIC ### Time Travel
# MAGIC Delta Lake automatically versions every change. You can:
# MAGIC - Query a table **as it was at any point in time** (`SELECT * FROM table TIMESTAMP AS OF '2024-01-01'`)
# MAGIC - Roll back to a previous version if something goes wrong (`RESTORE TABLE table TO VERSION AS OF 5`)
# MAGIC - Audit every change made to a table (`DESCRIBE HISTORY table`)
# MAGIC
# MAGIC This replaces the need for DynamoDB state tables to track "what version of the document was processed." The history is built into the table itself.
# MAGIC
# MAGIC ### Schema Enforcement and Evolution
# MAGIC Delta tables enforce a schema on write. If upstream data changes shape unexpectedly, the write **fails loudly** instead of silently corrupting your data. When you intentionally want to change the schema, Delta supports **schema evolution** — adding new columns without breaking existing queries.
# MAGIC
# MAGIC ## Why Delta Lake Replaces S3 + SQS
# MAGIC
# MAGIC In your current architecture, S3 buckets are "dumb storage" and SQS queues are the mechanism for signaling that new data is available. This creates several problems:
# MAGIC - Queue messages can be lost, duplicated, or arrive out of order
# MAGIC - You need separate state management to track what has been processed
# MAGIC - Reprocessing means re-sending messages through queues
# MAGIC
# MAGIC With Delta Lake:
# MAGIC - **No message queues needed.** Each pipeline stage writes to a Delta table. The next stage reads from it using Auto Loader or streaming, which automatically detects new data.
# MAGIC - **Built-in exactly-once semantics.** Delta's transaction log ensures no duplicates.
# MAGIC - **Reprocessing is trivial.** Just rerun the pipeline — Delta tables are the source of truth, not ephemeral queue messages.
# MAGIC
# MAGIC > **Presenter note:** This is a key "aha moment" for the group. The idea that the table itself replaces both the storage AND the messaging layer is the fundamental shift. Draw the contrast clearly: in their current system, they have S3 (storage) + SQS (signaling) + DynamoDB (state). In Databricks, a Delta table does all three.

# COMMAND ----------

# MAGIC %md
# MAGIC # Medallion Architecture: Mapping to Your Document Pipeline
# MAGIC
# MAGIC The **Medallion Architecture** is a design pattern for organizing data in a lakehouse. It uses three layers — Bronze, Silver, and Gold — to progressively refine raw data into business-ready assets.
# MAGIC
# MAGIC This maps directly to your document processing flow.
# MAGIC
# MAGIC ## Bronze Layer: Raw Ingestion
# MAGIC
# MAGIC **What it holds:** Raw documents exactly as they arrive from SharePoint — PDFs, Word docs, images, metadata. Nothing is transformed or parsed yet.
# MAGIC
# MAGIC **What it replaces in your current system:**
# MAGIC - S3 staging bucket where raw documents land
# MAGIC - The initial SQS queue that triggers processing
# MAGIC - DynamoDB records tracking "document received" status
# MAGIC
# MAGIC **Key properties:**
# MAGIC - Append-only (raw data is never modified)
# MAGIC - Full fidelity (nothing is dropped or transformed)
# MAGIC - Serves as the system of record for all incoming documents
# MAGIC
# MAGIC ## Silver Layer: Parsed and Structured Content
# MAGIC
# MAGIC **What it holds:** Extracted text, identified images, structured metadata, document sections. The raw bytes have been converted into queryable, structured data.
# MAGIC
# MAGIC **What it replaces in your current system:**
# MAGIC - Parsing Service (PDF/Word text extraction)
# MAGIC - Conversion Service (format normalization)
# MAGIC - The SQS queues between ingestion and parsing
# MAGIC - DynamoDB records tracking "document parsed" status
# MAGIC
# MAGIC **Key properties:**
# MAGIC - Cleaned and deduplicated
# MAGIC - Conformed to a standard schema
# MAGIC - Queryable with SQL
# MAGIC
# MAGIC ## Gold Layer: Enriched and Business-Ready
# MAGIC
# MAGIC **What it holds:** LLM-generated summaries, document classifications, topic tags, embeddings, and any other enrichments that make the content ready for search and retrieval.
# MAGIC
# MAGIC **What it replaces in your current system:**
# MAGIC - Enrichment Service (LLM summarization)
# MAGIC - Tagging Service (classification and labeling)
# MAGIC - Embedding Service (vector generation)
# MAGIC - The SQS queues between parsing and enrichment
# MAGIC - DynamoDB records tracking "document enriched" status
# MAGIC
# MAGIC **Key properties:**
# MAGIC - Business-ready and consumption-optimized
# MAGIC - Powers downstream applications (Elasticsearch, dashboards, APIs)
# MAGIC - Aggregated and enriched
# MAGIC
# MAGIC ## The Full Picture
# MAGIC
# MAGIC ```
# MAGIC SharePoint --> [Auto Loader] --> Bronze (raw docs)
# MAGIC                                    |
# MAGIC                                    v
# MAGIC                               Silver (parsed text, images, metadata)
# MAGIC                                    |
# MAGIC                                    v
# MAGIC                               Gold (summaries, tags, embeddings)
# MAGIC                                    |
# MAGIC                                    v
# MAGIC                               Elasticsearch (search & retrieval)
# MAGIC ```
# MAGIC
# MAGIC Each arrow is a **Delta table read/write** — no queues, no triggers, no microservices.
# MAGIC
# MAGIC > **Presenter note:** Walk through the diagram and map each layer back to their existing services. Ask participants: "How many services does your current pipeline use between raw document and enriched output?" The answer is typically 5-6. With Medallion Architecture, it is three Delta tables and a single pipeline definition.

# COMMAND ----------

# MAGIC %md
# MAGIC # Auto Loader: Incremental File Processing
# MAGIC
# MAGIC ## The Problem It Solves
# MAGIC
# MAGIC In your current system, when a new document lands in S3, an EventBridge rule fires a Lambda function, which sends a message to an SQS queue, which triggers a downstream service. That is **three components** just to say "hey, there is a new file."
# MAGIC
# MAGIC **Auto Loader** replaces all of that with a single line of configuration.
# MAGIC
# MAGIC ## How It Works
# MAGIC
# MAGIC Auto Loader (`cloudFiles` in Spark) monitors a file location and automatically processes new files as they arrive:
# MAGIC
# MAGIC ```python
# MAGIC # This is what an Auto Loader read looks like (we will build this in Block 2)
# MAGIC spark.readStream.format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "binaryFile")
# MAGIC     .load("/Volumes/workshop/default/documents/")
# MAGIC ```
# MAGIC
# MAGIC **Key features:**
# MAGIC - **Incremental processing**: Only processes new files since the last run — no re-scanning
# MAGIC - **Exactly-once semantics**: Checkpoint-based tracking ensures every file is processed exactly once
# MAGIC - **Scalable**: Handles millions of files efficiently using file notification mode
# MAGIC - **Schema inference**: Automatically detects file schemas and evolves as needed
# MAGIC
# MAGIC ## Why This Matters for Your Use Case
# MAGIC
# MAGIC - **No more EventBridge rules** to set up and maintain
# MAGIC - **No more Lambda triggers** that can fail silently or hit concurrency limits
# MAGIC - **No more SQS dead-letter queues** for failed processing — Auto Loader retries automatically
# MAGIC - **Built-in checkpointing** replaces the DynamoDB state tracking you maintain today
# MAGIC
# MAGIC > **Presenter note:** Keep this section brief — it is a conceptual introduction. We will get hands-on with Auto Loader in Block 2. The key takeaway is: Auto Loader replaces the entire EventBridge + Lambda + SQS trigger chain with a single Spark configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakeflow Declarative Pipelines: Replacing Step Functions
# MAGIC
# MAGIC ## The Problem It Solves
# MAGIC
# MAGIC Your current orchestration uses **AWS Step Functions** to coordinate the flow between microservices. Each step in the state machine calls a service, checks for success or failure, handles retries, and routes to the next step. The Step Function JSON definitions are complex, hard to test, and tightly coupled to the AWS execution model.
# MAGIC
# MAGIC **Lakeflow Declarative Pipelines** (formerly Delta Live Tables / DLT) replaces all of this with a single Python file.
# MAGIC
# MAGIC ## How It Works
# MAGIC
# MAGIC Instead of defining a state machine with explicit transitions, you **declare the tables you want** and the transformations that produce them. The system figures out the execution order, dependencies, retries, and monitoring automatically.
# MAGIC
# MAGIC ```python
# MAGIC # This is what a Lakeflow Declarative Pipeline looks like (we will build this in Block 4)
# MAGIC import dlt
# MAGIC
# MAGIC @dlt.table(comment="Raw documents from SharePoint")
# MAGIC def bronze_documents():
# MAGIC     return (spark.readStream.format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "binaryFile")
# MAGIC         .load("/Volumes/workshop/default/documents/"))
# MAGIC
# MAGIC @dlt.table(comment="Parsed document content")
# MAGIC def silver_parsed():
# MAGIC     return dlt.read_stream("bronze_documents").withColumn("text", parse_document("content"))
# MAGIC
# MAGIC @dlt.table(comment="Enriched with LLM summaries and tags")
# MAGIC def gold_enriched():
# MAGIC     return dlt.read_stream("silver_parsed").withColumn("summary", call_llm("text"))
# MAGIC ```
# MAGIC
# MAGIC **Key features:**
# MAGIC - **Automatic dependency management**: The system knows `gold_enriched` depends on `silver_parsed`, which depends on `bronze_documents`. No explicit wiring needed.
# MAGIC - **Built-in retries and error handling**: Failed records are automatically quarantined in expectations (data quality rules), not lost.
# MAGIC - **Built-in monitoring**: A visual pipeline graph shows the status of every table, data quality metrics, and processing throughput.
# MAGIC - **Incremental by default**: Each run only processes new data, not the entire dataset.
# MAGIC
# MAGIC ## Why This Matters for Your Use Case
# MAGIC
# MAGIC - **One Python file** replaces Step Function JSON + Lambda glue code + SQS routing
# MAGIC - **Adding a new processing step** means adding a new function — not deploying a new service, queue, and trigger
# MAGIC - **Testing is simple** — it is just Python functions you can unit test
# MAGIC - **Visibility is immediate** — the pipeline UI shows exactly where data is flowing and where problems occur
# MAGIC
# MAGIC > **Presenter note:** Again, keep this brief — we will build the full pipeline in Block 4. The key insight is the shift from **imperative orchestration** (Step Functions: "do this, then that, handle this error") to **declarative pipelines** (Lakeflow: "here are the tables I want and how they are derived"). This is a fundamental simplification.

# COMMAND ----------

# MAGIC %md
# MAGIC # Hands-On: Let's Verify Our Environment
# MAGIC
# MAGIC Now let's run some code to make sure everything is set up correctly. Run each cell below by pressing **Shift+Enter** or clicking the play button.
# MAGIC
# MAGIC > **Presenter note:** Walk around the room and make sure everyone can execute cells. This is often where cluster attachment issues surface. If someone cannot run cells, check that their cluster is running and that the notebook is attached to it.

# COMMAND ----------

# Hello, Databricks! Let's make sure our notebook can execute Python code.
print("Hello, Databricks!")

# COMMAND ----------

# MAGIC %md
# MAGIC If you see `Hello, Databricks!` printed above, your notebook is connected to a running cluster and ready to go.
# MAGIC
# MAGIC Now let's explore the data catalog. The next cell uses **`%sql`** — a magic command that tells Databricks to run this cell as SQL instead of Python.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's see what catalogs are available in our workspace
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %md
# MAGIC You should see the **`workshop`** catalog in the list above (among others). This is the catalog we will use for all workshop activities.
# MAGIC
# MAGIC Now let's look inside it to see what schemas are available.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switch to the workshop catalog and list its schemas
# MAGIC USE CATALOG workshop;
# MAGIC SHOW SCHEMAS

# COMMAND ----------

# MAGIC %md
# MAGIC You should see the **`default`** schema. This is where our tables and volumes live.
# MAGIC
# MAGIC Next, let's look at the files we will be processing. **Volumes** in Unity Catalog are how Databricks manages unstructured files (PDFs, images, etc.) — similar to S3 buckets but with governance and access control built in.

# COMMAND ----------

# Let's list the documents available in our workshop Volume
display(dbutils.fs.ls("/Volumes/workshop/default/documents/"))

# COMMAND ----------

# MAGIC %md
# MAGIC The files listed above are the documents we will process throughout this workshop. These represent the types of documents that flow through your current pipeline — PDFs, Word documents, and other file types.
# MAGIC
# MAGIC Finally, let's run a simple query to confirm our SQL warehouse is working correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's verify we can query data
# MAGIC SELECT 'Workshop is ready!' AS status, current_timestamp() AS checked_at

# COMMAND ----------

# MAGIC %md
# MAGIC # Block 1 Complete
# MAGIC
# MAGIC ## What We Covered
# MAGIC
# MAGIC - **Workshop objectives** and the two-day agenda
# MAGIC - **Architecture comparison**: your 13+ queues and 8+ microservices vs. a single Databricks pipeline
# MAGIC - **Workspace navigation**: Catalog Explorer, SQL Editor, Compute, and notebooks
# MAGIC - **Delta Lake**: ACID transactions, time travel, and why it replaces S3 + SQS
# MAGIC - **Medallion Architecture**: Bronze (raw) --> Silver (parsed) --> Gold (enriched)
# MAGIC - **Auto Loader**: Incremental file processing that replaces EventBridge + Lambda triggers
# MAGIC - **Lakeflow Declarative Pipelines**: Single Python file that replaces Step Functions orchestration
# MAGIC - **Hands-on verification**: Confirmed our environment is working
# MAGIC
# MAGIC ## Up Next: Block 2 — Ingestion (10:00 - 12:00)
# MAGIC
# MAGIC In the next block, we will:
# MAGIC 1. Connect to SharePoint and pull documents into Databricks
# MAGIC 2. Set up Auto Loader for incremental file processing
# MAGIC 3. Build the Bronze layer of our Medallion Architecture
# MAGIC
# MAGIC Take a short break and we will reconvene at 10:00.
# MAGIC
# MAGIC > **Presenter note:** Use the break to check in with participants individually. Ask if the architecture comparison resonated. Gauge the room's comfort level with notebooks and SQL — this will inform how much scaffolding to provide in Block 2.