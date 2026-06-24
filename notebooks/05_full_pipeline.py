# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///


# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %md
# MAGIC # Block 5: Assembling the Declarative Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Introduction to Spark Declarative Pipelines
# MAGIC
# MAGIC **Spark Declarative Pipelines** (formerly Delta Live Tables / DLT) is the single most important capability we will show you in this workshop. Everything we have built so far -- SharePoint ingestion, document parsing, LLM enrichment, Elasticsearch sync -- comes together here.
# MAGIC
# MAGIC
# MAGIC **What the platform handles automatically:**
# MAGIC - **Dependency management** -- the system knows which tables depend on which, and executes them in the right order
# MAGIC - **Retries and error handling** -- failed records are quarantined, not lost; transient failures are retried automatically
# MAGIC - **Checkpointing** -- every streaming source tracks its position; no custom state management needed
# MAGIC - **Incremental processing** -- each run only processes new data, not the entire dataset
# MAGIC - **Schema evolution** -- upstream schema changes are handled gracefully, not silently dropped
# MAGIC
# MAGIC
# MAGIC ![Bronze Tables Pipeline](./images/bronze_tables_pipeline.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pipeline Concepts
# MAGIC
# MAGIC Before we look at the code, let's understand the building blocks of a Lakeflow Declarative Pipeline:
# MAGIC
# MAGIC ### 1. Streaming Table (`@dp.table`)
# MAGIC - **Purpose:** Exactly-once ingestion from streaming sources
# MAGIC - **Use case:** Ingesting raw documents from SharePoint via Auto Loader
# MAGIC - **Behavior:** Append-only. New files are added; existing rows are never modified.
# MAGIC - **Think of it as:** Your Bronze layer -- the landing zone for raw data
# MAGIC
# MAGIC ### 2. Materialized View (`@dp.materialized_view`)
# MAGIC - **Purpose:** Recomputed as needed when upstream data changes
# MAGIC - **Use case:** Parsing, enrichment, and transformation stages
# MAGIC - **Behavior:** The system tracks dependencies and only recomputes when inputs change
# MAGIC - **Think of it as:** Your Silver and Gold layers -- progressively refined views of the data
# MAGIC
# MAGIC ### 3. Expectations (`@dp.expect_or_drop`)
# MAGIC - **Purpose:** Data quality constraints applied declaratively
# MAGIC - **Use case:** Dropping empty documents, enforcing file size limits, validating parsed output
# MAGIC - **Behavior:** Rows that fail the expectation are dropped (or flagged, or halt the pipeline, depending on the variant)
# MAGIC - **Think of it as:** Input validation that currently lives scattered across your microservices
# MAGIC
# MAGIC ### 4. ForEachBatch Sink (`@dp.foreach_batch_sink`)
# MAGIC - **Purpose:** Custom sink for writing to external systems
# MAGIC - **Use case:** Writing enriched documents to Elasticsearch
# MAGIC - **Behavior:** Receives micro-batches of data and writes them using your custom logic
# MAGIC - **Think of it as:** Your Elasticsearch writer service -- but embedded in the pipeline
# MAGIC
# MAGIC ### 5. Append Flow (`@dp.append_flow`)
# MAGIC - **Purpose:** Directs streaming data from a source to a sink
# MAGIC - **Use case:** Connecting the enriched documents table to the Elasticsearch sink
# MAGIC - **Behavior:** Streams data continuously (or in triggered batches) from source to sink
# MAGIC - **Think of it as:** The final SQS queue that routes enriched documents to Elasticsearch
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## The Complete Pipeline -- Mapping to Your Architecture
# MAGIC
# MAGIC Here is the full pipeline DAG. Each box is a function in our Python file. Each arrow is an automatic dependency that the system manages for us.
# MAGIC
# MAGIC ```
# MAGIC SharePoint -----> Auto Loader -----> raw_documents (Streaming Table)
# MAGIC                                            |
# MAGIC                                            v
# MAGIC                                   parsed_documents (Materialized View)
# MAGIC                                            |
# MAGIC                                            v
# MAGIC                                   document_text (Materialized View)
# MAGIC                                            |
# MAGIC                                            v
# MAGIC                                   enriched_documents (Materialized View)
# MAGIC                                            |
# MAGIC                                            v
# MAGIC                                   elasticsearch_sink 
# MAGIC ```
# MAGIC
# MAGIC Now let's map each stage to the AWS services it replaces:
# MAGIC
# MAGIC | Pipeline Stage | Current (AWS) | Databricks Equivalent |
# MAGIC |---|---|---|
# MAGIC | **Ingestion** | SharePoint sync Lambda + Graph API polling + S3 staging | Auto Loader + SharePoint connector |
# MAGIC | **Orchestration** | 13 SQS queues + EventBridge rules + dead-letter queues | Implicit -- pipeline DAG handles ordering |
# MAGIC | **Parsing** | Parsing Service + Conversion Service + Step Functions | `ai_parse_document` in materialized view |
# MAGIC | **Enrichment** | Enrichment Service + Tagging Service + Embedding Service | AI Functions (`ai_query`, `ai_classify`, `ai_extract`) in materialized view |
# MAGIC | **State Management** | DynamoDB tables tracking document status at each stage | Delta Lake (ACID transactions, time travel, automatic versioning) |
# MAGIC | **Monitoring** | CloudWatch metrics + X-Ray traces + custom dashboards | Pipeline event log + system tables + built-in pipeline UI |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## The Complete Pipeline Definition
# MAGIC
# MAGIC The Spark Declarative Pipeline contains the **entire pipeline** -- from SharePoint ingestion through Elasticsearch output in a set of organized Python files.
# MAGIC
# MAGIC Each function is a stage. Each decorator tells the system what kind of table to create and what quality constraints to enforce. The system handles everything else: ordering, retries, checkpointing, monitoring.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC The Spark Declarative Pipeline is contained in the **`bundles/vantage-pipeline`** folder.  Walk through the files to see how the concepts covered so far are leveraged in the declarative pipeline framework.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Data Quality with Expectations
# MAGIC
# MAGIC One of the most powerful features of Lakeflow Declarative Pipelines is **built-in data quality enforcement**. Instead of scattering validation logic across microservices, you declare your expectations right next to the table definition.
# MAGIC
# MAGIC ### Three Levels of Enforcement
# MAGIC
# MAGIC | Decorator | Behavior | Use When |
# MAGIC |---|---|---|
# MAGIC | `@dp.expect("name", "condition")` | **Warn** but keep the row. Metrics are logged but no data is dropped. | You want visibility into quality issues without blocking the pipeline |
# MAGIC | `@dp.expect_or_drop("name", "condition")` | **Silently drop** rows that fail the condition. Dropped counts are tracked in metrics. | You want to filter out bad data automatically (e.g., empty documents, oversized files) |
# MAGIC | `@dp.expect_or_fail("name", "condition")` | **Halt the entire pipeline** if any row fails. | The condition is critical and no downstream processing should occur with bad data |
# MAGIC
# MAGIC ### Examples from Our Pipeline
# MAGIC
# MAGIC In the `raw_documents` table above, we use two `@dp.expect_or_drop` expectations:
# MAGIC
# MAGIC ```python
# MAGIC @dp.expect_or_drop("has_content", "content IS NOT NULL")
# MAGIC @dp.expect_or_drop("valid_size", "length > 0 AND length < 104857600")
# MAGIC ```
# MAGIC
# MAGIC - **`has_content`**: Drops any row where the file content is null (e.g., a file that could not be read)
# MAGIC - **`valid_size`**: Drops files that are empty (0 bytes) or larger than 100MB (likely corrupted or unsupported)
# MAGIC
# MAGIC ### Quality Metrics in the Pipeline UI
# MAGIC
# MAGIC Every expectation generates metrics that are visible in the pipeline UI:
# MAGIC - **Total rows processed** per table
# MAGIC - **Rows passed** vs. **rows dropped** for each expectation
# MAGIC - **Pass rate percentage** over time
# MAGIC - **Trend charts** to spot degrading data quality before it becomes a problem
# MAGIC
# MAGIC This replaces the scattered CloudWatch metrics and custom DynamoDB counters you currently use to track document processing success rates.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Architecture Question: Single Pipeline vs. Per-Source
# MAGIC
# MAGIC A common question at this point is: "What if we have multiple data sources? Do we need a separate pipeline for each one?"
# MAGIC
# MAGIC The answer depends on your needs, but here is the recommended approach:
# MAGIC
# MAGIC ### Option A: Single Pipeline with Shared Downstream
# MAGIC Use `@dp.append_flow` to merge multiple ingestion sources into shared downstream stages:
# MAGIC
# MAGIC ```
# MAGIC SharePoint -----> raw_docs_sharepoint ----+
# MAGIC                                            |
# MAGIC                                            +-----> parsed_documents -----> enriched -----> ES
# MAGIC                                            |
# MAGIC SQL Server -----> raw_docs_sqlserver  ----+
# MAGIC ```
# MAGIC
# MAGIC - Each source gets its own ingestion table (with source-specific connectors and expectations)
# MAGIC - Downstream stages (parsing, enrichment, ES sink) are shared
# MAGIC - `@dp.append_flow` directs multiple streams into a single downstream table
# MAGIC
# MAGIC ### Option B: Per-Source Pipelines
# MAGIC Duplicate the notebook for each source with different connection settings:
# MAGIC - `pipeline_sharepoint.py` with SharePoint connector
# MAGIC - `pipeline_sqlserver.py` with JDBC connector
# MAGIC - Each pipeline is independent and can be scheduled/monitored separately
# MAGIC
# MAGIC ### Recommendation
# MAGIC
# MAGIC **Shared downstream stages with separate ingestion tables per source.** This gives you:
# MAGIC - **Source isolation** -- a problem with one source does not block others
# MAGIC - **Shared logic** -- parsing and enrichment code is written once
# MAGIC - **Single monitoring view** -- one pipeline UI shows the full picture
# MAGIC - **Independent scaling** -- ingestion tables can process at different rates
# MAGIC
# MAGIC This mirrors a pattern you already use: your current microservices have shared downstream processing (enrichment, Elasticsearch) fed by multiple upstream triggers. The difference is that this pattern is expressed in 10 lines of code instead of 10 infrastructure components.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Monitoring
# MAGIC
# MAGIC Once the pipeline is running, monitoring is built in -- no additional infrastructure to set up.
# MAGIC
# MAGIC ### 1. Pipeline Visualization UI (DAG View)
# MAGIC The pipeline UI shows a **visual DAG** of every table and its dependencies:
# MAGIC - Green = healthy, processing normally
# MAGIC - Yellow = running, currently processing data
# MAGIC - Red = failed, needs attention
# MAGIC - Each node shows row counts, processing time, and data quality metrics
# MAGIC
# MAGIC This replaces the mental model you currently build by cross-referencing CloudWatch dashboards, SQS queue depths, and DynamoDB scan results.
# MAGIC
# MAGIC ### 2. Event Log
# MAGIC Every pipeline run generates a detailed **event log** with:
# MAGIC - Start/stop times for each table update
# MAGIC - Rows processed, bytes written
# MAGIC - Errors and warnings with full stack traces
# MAGIC - Expectation pass/fail counts
# MAGIC
# MAGIC The event log is queryable as a Delta table, so you can build SQL dashboards on top of it.
# MAGIC
# MAGIC ### 3. Data Quality Metrics
# MAGIC For every expectation you define, the pipeline tracks:
# MAGIC - **Pass rate** -- what percentage of rows meet the expectation
# MAGIC - **Drop count** -- how many rows were dropped (for `expect_or_drop`)
# MAGIC - **Trend over time** -- is data quality improving or degrading?
# MAGIC
# MAGIC This is data quality monitoring that your current system simply does not have. Today, if your parsing service silently fails on a new document format, you discover it when a user complains that search results are incomplete. With expectations, you discover it in the pipeline UI before the user ever notices.
# MAGIC
# MAGIC ### 4. System Tables for Programmatic Monitoring
# MAGIC Databricks exposes pipeline metrics in **system tables** that you can query with SQL:
# MAGIC
# MAGIC ```sql
# MAGIC -- Example: Get pipeline run history
# MAGIC SELECT * FROM system.lakeflow.pipeline_events
# MAGIC WHERE pipeline_id = 'your-pipeline-id'
# MAGIC ORDER BY timestamp DESC
# MAGIC ```
# MAGIC
# MAGIC This enables you to build custom alerts, integrate with PagerDuty or Slack, or feed metrics into your existing monitoring stack.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section Recap
# MAGIC
# MAGIC In this section we assembled the complete Lakeflow Declarative Pipeline. Here is what we covered:
# MAGIC
# MAGIC 1. **Pipeline concepts** -- Streaming Tables, Materialized Views, Expectations, ForEachBatch Sinks, and Append Flows
# MAGIC 2. **The complete pipeline** -- five stages from SharePoint ingestion through Elasticsearch output, all in one Python file
# MAGIC 3. **Data quality** -- three levels of expectations (`expect`, `expect_or_drop`, `expect_or_fail`) with built-in metrics
# MAGIC 4. **Deployment** -- save a notebook, create a pipeline, click Run
# MAGIC 5. **Architecture patterns** -- single pipeline with shared downstream stages vs. per-source pipelines
# MAGIC 6. **Monitoring** -- DAG visualization, event logs, quality metrics, and system tables