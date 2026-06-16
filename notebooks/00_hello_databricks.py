# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "pypdf",
# ]
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Welcome to the Document Processing Workshop
# MAGIC
# MAGIC ## Workshop Objectives
# MAGIC
# MAGIC Over the next two days, we will **explore how Databricks can consolidate and enhance** your document processing capabilities. By the end of this workshop, you will have a working pipeline that demonstrates how a unified platform approach can streamline operations and reduce operational overhead.
# MAGIC
# MAGIC **What you will walk away with:**
# MAGIC - A working document processing pipeline built entirely on Databricks
# MAGIC - Hands-on experience with Unity Catalog, Delta Lake, Auto Loader, and Lakeflow Declarative Pipelines
# MAGIC - A clear understanding of how Databricks capabilities map to your current architecture
# MAGIC - Concrete evidence to inform your platform strategy going forward
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

# DBTITLE 1,Cell 2
# MAGIC %md
# MAGIC # Architecture Comparison: Current State and the Databricks Approach
# MAGIC
# MAGIC ## Your Current System
# MAGIC
# MAGIC Your existing document processing architecture is a well-designed distributed system built on AWS. It has served the team well, handling complex document workflows at scale:
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
# MAGIC **Opportunities that a platform approach can address:**
# MAGIC - Simplify debugging with a single, unified view of the entire pipeline
# MAGIC - Reduce the overhead of adding new processing steps
# MAGIC - Consolidate state management into the storage layer itself
# MAGIC - Make reprocessing straightforward with built-in time travel and checkpointing
# MAGIC - Centralize monitoring in a single pane of glass
# MAGIC
# MAGIC ## What We Will Build Today
# MAGIC
# MAGIC A **single Databricks platform** with a **declarative pipeline** that consolidates these components:
# MAGIC
# MAGIC | Current AWS Component | Databricks Approach |
# MAGIC |----------------------|----------------------|
# MAGIC | S3 staging buckets | Unity Catalog Volumes + Delta tables |
# MAGIC | SQS queues (13+) | Delta tables (each stage writes to a table; next stage reads) |
# MAGIC | Lambda triggers | Auto Loader (automatic incremental file detection) |
# MAGIC | Step Functions | Lakeflow Declarative Pipelines (single Python file) |
# MAGIC | DynamoDB state tables | Delta table metadata + checkpoints |
# MAGIC | 8+ microservices | Notebook functions within a single pipeline |
# MAGIC | CloudWatch + X-Ray | Built-in pipeline monitoring + Databricks SQL dashboards |
# MAGIC
# MAGIC ![Databricks Data Intelligence Platform](./images/platform_overview.png)
# MAGIC
# MAGIC > **Presenter note:** Spend time on this comparison. Acknowledge that their current architecture was built thoughtfully and has been effective. The goal is to show how a platform approach can reduce operational overhead by consolidating many components into fewer, more integrated ones. Ask participants which consolidation opportunities are most interesting to them.

# COMMAND ----------

# DBTITLE 1,Cell 3
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
# MAGIC ![Databricks Platform Architecture](./images/platform_aws_governance.png)
# MAGIC
# MAGIC > **Presenter note:** Do a live walkthrough of the workspace. Open Catalog Explorer and show the `workshop` catalog. Navigate into the `default` schema and show the Volumes. Have participants follow along and confirm they can see the same assets. If anyone cannot see the catalog, check their permissions before proceeding.

# COMMAND ----------

# DBTITLE 1,Cell 4
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
# MAGIC - Built-in protection against data corruption from failed jobs or concurrent access
# MAGIC
# MAGIC ### Time Travel
# MAGIC Delta Lake automatically versions every change. You can:
# MAGIC - Query a table **as it was at any point in time** (`SELECT * FROM table TIMESTAMP AS OF '2024-01-01'`)
# MAGIC - Roll back to a previous version if something goes wrong (`RESTORE TABLE table TO VERSION AS OF 5`)
# MAGIC - Audit every change made to a table (`DESCRIBE HISTORY table`)
# MAGIC
# MAGIC This means version tracking is built directly into the storage layer — no need for separate state management to track "what version of the document was processed."
# MAGIC
# MAGIC ### Schema Enforcement and Evolution
# MAGIC Delta tables enforce a schema on write. If upstream data changes shape unexpectedly, the write **fails loudly** instead of silently corrupting your data. When you intentionally want to change the schema, Delta supports **schema evolution** — adding new columns without breaking existing queries.
# MAGIC
# MAGIC ## How Delta Lake Unifies Storage, Signaling, and State
# MAGIC
# MAGIC One of the most powerful aspects of Delta Lake is that a single table serves multiple roles that traditionally require separate components:
# MAGIC
# MAGIC - **Storage + signaling in one.** Each pipeline stage writes to a Delta table. The next stage reads from it using Auto Loader or streaming, which automatically detects new data — no separate messaging layer needed.
# MAGIC - **Built-in exactly-once semantics.** Delta's transaction log ensures no duplicates without requiring external deduplication logic.
# MAGIC - **Straightforward reprocessing.** Just rerun the pipeline — Delta tables are the durable source of truth with full history.
# MAGIC
# MAGIC This consolidation is one of the key benefits of the platform approach: fewer moving parts means fewer potential failure points and simpler operational management.
# MAGIC
# MAGIC > **Presenter note:** This is a key insight for the group. The idea that the table itself can serve as storage, signaling layer, and state tracker is a fundamental simplification. In a traditional architecture, these are three separate systems (object store, message queue, state database). With Delta Lake, they converge into one.

# COMMAND ----------

# DBTITLE 1,Cell 5
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
# MAGIC **Databricks equivalent of:**
# MAGIC - S3 staging bucket where raw documents land
# MAGIC - The initial queue that triggers processing
# MAGIC - State records tracking "document received" status
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
# MAGIC **Databricks equivalent of:**
# MAGIC - Parsing Service (PDF/Word text extraction)
# MAGIC - Conversion Service (format normalization)
# MAGIC - Queues between ingestion and parsing
# MAGIC - State records tracking "document parsed" status
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
# MAGIC **Databricks equivalent of:**
# MAGIC - Enrichment Service (LLM summarization)
# MAGIC - Tagging Service (classification and labeling)
# MAGIC - Embedding Service (vector generation)
# MAGIC - Queues between parsing and enrichment
# MAGIC - State records tracking "document enriched" status
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
# MAGIC Each arrow is a **Delta table read/write** — the data flow is managed entirely through table operations.
# MAGIC
# MAGIC > **Presenter note:** Walk through the diagram and map each layer to the corresponding components in their existing system. The goal is to show how the Medallion Architecture provides a clear, structured way to organize the same processing stages they already have, with fewer operational components to manage.

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %md
# MAGIC # Auto Loader: Incremental File Processing
# MAGIC
# MAGIC ## What Auto Loader Brings to the Table
# MAGIC
# MAGIC Detecting and processing new files as they arrive is a common need in any document pipeline. **Auto Loader** provides this capability as a built-in, fully managed feature — automatic incremental file detection with a single line of configuration.
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
# MAGIC ## Benefits for Document Processing
# MAGIC
# MAGIC - **Simplified event detection** — no need to configure separate event rules, triggers, or queues for new file arrival
# MAGIC - **Built-in retry logic** — transient failures are handled automatically without dead-letter queue management
# MAGIC - **Managed checkpointing** — state tracking for which files have been processed is handled by the framework
# MAGIC - **Scales seamlessly** — handles everything from a handful of files to millions without configuration changes
# MAGIC
# MAGIC > **Presenter note:** Keep this section brief — it is a conceptual introduction. We will get hands-on with Auto Loader in Block 2. The key takeaway is that Auto Loader consolidates file detection, event handling, and state tracking into a single managed capability.

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %md
# MAGIC # Lakeflow Declarative Pipelines: Replacing Step Functions
# MAGIC
# MAGIC ## A Declarative Approach to Orchestration
# MAGIC
# MAGIC Orchestrating multi-step data workflows is one of the more complex aspects of any pipeline. **Lakeflow Declarative Pipelines** offer a different paradigm: instead of defining explicit state transitions and routing logic, you **declare the desired outcome** and let the framework handle execution order, dependencies, and error handling.
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
# MAGIC ## Benefits for Document Processing
# MAGIC
# MAGIC - **Single Python file** defines the entire pipeline — easy to read, version, and review
# MAGIC - **Adding a new processing step** means adding a new function — minimal overhead
# MAGIC - **Testable** — standard Python functions that you can unit test with familiar tools
# MAGIC - **Full visibility** — the pipeline UI shows exactly where data is flowing and where any issues occur
# MAGIC
# MAGIC > **Presenter note:** Keep this brief — we will build the full pipeline in Block 4. The key insight is the shift from **imperative orchestration** (explicitly defining each step, transition, and error path) to **declarative pipelines** (declaring the desired tables and their derivations, letting the framework manage the rest).

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

# MAGIC %md
# MAGIC ## Utilities
# MAGIC "include" the Utilities notebook to set variables and define reusable routines.

# COMMAND ----------

# MAGIC %run ./_resources/Utilities

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's see what catalogs are available in our workspace
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %md
# MAGIC You should see the workshop catalog in the list above (among others). This is the catalog we will use for all workshop activities.
# MAGIC
# MAGIC Now let's look inside it to see what schemas are available.

# COMMAND ----------

# Switch to the workshop catalog and list its schemas
spark.sql(f"USE CATALOG {catalog}");
spark.sql('SHOW SCHEMAS').display();

# COMMAND ----------

# MAGIC %md
# MAGIC You should see the **`00_shared`** schema. This contains tables and volumes that have been provided.
# MAGIC
# MAGIC Next, let's look at the files we will be processing. **Volumes** in Unity Catalog are how Databricks manages unstructured files (PDFs, images, etc.) — similar to S3 buckets but with governance and access control built in.

# COMMAND ----------

# Let's list the documents available in our workshop Volume
display(test_documents_volume)
display(dbutils.fs.ls(f"{test_documents_volume}"))

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

# DBTITLE 1,Cell 18
# MAGIC %md
# MAGIC # Block 1 Complete
# MAGIC
# MAGIC ## What We Covered
# MAGIC
# MAGIC - **Workshop objectives** and the two-day agenda
# MAGIC - **Architecture overview**: how Databricks consolidates distributed components into a unified platform
# MAGIC - **Workspace navigation**: Catalog Explorer, SQL Editor, Compute, and notebooks
# MAGIC - **Delta Lake**: ACID transactions, time travel, and why it replaces S3 + SQS
# MAGIC - **Medallion Architecture**: Bronze (raw) --> Silver (parsed) --> Gold (enriched)
# MAGIC - **Auto Loader**: Managed incremental file processing with built-in state tracking
# MAGIC - **Lakeflow Declarative Pipelines**: Declarative pipeline definition in a single Python file
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
# MAGIC > **Presenter note:** Use the break to check in with participants individually. Ask which capabilities are most exciting to them and what questions they have. Gauge the room's comfort level with notebooks and SQL — this will inform how much scaffolding to provide in Block 2.