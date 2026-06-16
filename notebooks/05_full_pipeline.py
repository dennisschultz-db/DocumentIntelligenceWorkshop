# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///


# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %md
# MAGIC # Block 5 (Part 2): Assembling the Declarative Pipeline
# MAGIC **Day 2 | 10:05 - 10:45 AM**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Introduction to Lakeflow Declarative Pipelines
# MAGIC
# MAGIC **Lakeflow Declarative Pipelines** (formerly Delta Live Tables / DLT) is the single most important capability we will show you in this workshop. Everything we have built so far -- SharePoint ingestion, document parsing, LLM enrichment, Elasticsearch sync -- comes together here in **one Python file**.
# MAGIC
# MAGIC Let that sink in: **a single Python file replaces your ENTIRE microservices architecture.**
# MAGIC
# MAGIC No SQS queues. No Step Functions. No Lambda triggers. No DynamoDB state tables. No CloudWatch alarm chains. One file. One pipeline. One place to monitor, debug, and evolve.
# MAGIC
# MAGIC **What the platform handles automatically:**
# MAGIC - **Dependency management** -- the system knows which tables depend on which, and executes them in the right order
# MAGIC - **Retries and error handling** -- failed records are quarantined, not lost; transient failures are retried automatically
# MAGIC - **Checkpointing** -- every streaming source tracks its position; no custom state management needed
# MAGIC - **Incremental processing** -- each run only processes new data, not the entire dataset
# MAGIC - **Schema evolution** -- upstream schema changes are handled gracefully, not silently dropped
# MAGIC
# MAGIC ![Lakeflow Pipeline Architecture](./images/managed_ingestion_arch.png)
# MAGIC
# MAGIC ![Bronze Tables Pipeline](./images/bronze_tables_pipeline.png)
# MAGIC
# MAGIC > **Presenter note:** This is the climax of the workshop. Pause here and let the audience absorb the claim. Then say: "Let me prove it to you." Everything that follows is the proof. By the end of this section, they should be able to look at one Python file and trace the entire document processing pipeline from SharePoint ingestion through Elasticsearch output. Contrast this explicitly with the effort required to trace a document through their current system: Lambda trigger, SQS queue, parsing service, another SQS queue, enrichment service, another SQS queue, Elasticsearch writer -- any one of which can fail silently.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pipeline Concepts
# MAGIC
# MAGIC Before we look at the code, let's understand the four building blocks of a Lakeflow Declarative Pipeline:
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
# MAGIC > **Presenter note:** Walk through each concept and ask the audience to mentally map it to their current architecture. Streaming Table = the S3 landing zone + SQS trigger. Materialized View = the parsing/enrichment services. Expectations = the validation logic scattered across Lambda functions. ForEachBatch Sink = the Elasticsearch writer. The mapping should be intuitive because we designed the pipeline to mirror their existing flow.

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
# MAGIC                                   elasticsearch_sink (ForEachBatch)
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
# MAGIC **Services eliminated:** ~20+ AWS components collapsed into 5 Python functions in a single file.
# MAGIC
# MAGIC > **Presenter note:** This is the table to linger on. Go row by row. For each row, ask: "Who maintains this in your current system?" and "What happens when it breaks at 2 AM?" Then point to the Databricks column and say: "This is managed by the platform." The emotional impact should be relief -- not that their current system is bad, but that it does not have to be this hard.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## The Complete Pipeline Definition
# MAGIC
# MAGIC This is the star of the show. The cell below contains the **entire pipeline** -- from SharePoint ingestion through Elasticsearch output -- in a single Python file.
# MAGIC
# MAGIC Read it top to bottom. Each function is a stage. Each decorator tells the system what kind of table to create and what quality constraints to enforce. The system handles everything else: ordering, retries, checkpointing, monitoring.
# MAGIC
# MAGIC > **Presenter note:** Walk through this code slowly. Do NOT rush. This is the payoff for the entire workshop. Read each section header aloud and remind the audience which AWS services it replaces. When you get to the AI Functions in Stage 4, point out that LLM calls are just SQL expressions -- no API client libraries, no retry logic, no token management. When you get to Stage 5, point out that the Elasticsearch sink is embedded in the pipeline, not a separate service.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# ============================================================
# STAGE 1: INGEST -- Raw documents from SharePoint
# Replaces: Lambda + Graph API + S3 staging + SQS trigger
# ============================================================
@dp.table(
    comment="Raw binary documents ingested from SharePoint via Auto Loader"
)
@dp.expect_or_drop("has_content", "content IS NOT NULL")
@dp.expect_or_drop("valid_size", "length > 0 AND length < 104857600")  # 100MB max
def raw_documents():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("databricks.connection", "sharepoint_conn")
        .option("pathGlobFilter", "*.{pdf,pptx,docx}")
        .load("https://TENANT.sharepoint.com/sites/SITE/Shared%20Documents")
        .select("path", "content", "modificationTime", "length"))

# ============================================================
# STAGE 2: PARSE -- Extract text, images, tables from documents
# Replaces: Parsing Service + Conversion Service + Step Functions
# ============================================================
@dp.materialized_view(
    comment="Parsed document content with extracted elements"
)
def parsed_documents():
    return (spark.read.table("raw_documents")
        .withColumn("parsed", expr("""
            ai_parse_document(content, MAP(
                'version', '2.0',
                'imageOutputPath', '/Volumes/workshop/default/slide_images/'
            ))
        """))
        .select(
            "path", "modificationTime",
            expr("parsed:elements").alias("elements"),
            expr("parsed:metadata").alias("doc_metadata")
        ))

# ============================================================
# STAGE 3: FLATTEN -- Extract and structure text content
# ============================================================
@dp.materialized_view(
    comment="Flattened document text ready for enrichment"
)
def document_text():
    return (spark.read.table("parsed_documents")
        .selectExpr(
            "path", "modificationTime",
            "explode(elements) as element"
        )
        .selectExpr(
            "path", "modificationTime",
            "element.type as element_type",
            "element.content as content"
        )
        .filter("element_type IN ('text', 'title', 'section_header', 'table')"))

# ============================================================
# STAGE 4: ENRICH -- LLM classification, summarization, extraction
# Replaces: Enrichment Service + Tagging Service + Embedding Service
# ============================================================
@dp.materialized_view(
    comment="Documents enriched with LLM-generated summaries and classifications"
)
def enriched_documents():
    return (spark.read.table("document_text")
        .groupBy("path", "modificationTime")
        .agg(expr("concat_ws('\\n', collect_list(content))").alias("full_text"))
        .withColumn("summary", expr("""
            ai_query('databricks-meta-llama-3-3-70b-instruct',
                'Summarize in 3-5 sentences: ' || left(full_text, 4000),
                modelParameters => named_struct('max_tokens', 500),
                failOnError => false)
        """))
        .withColumn("category", expr("""
            ai_classify(
                left(full_text, 2000),
                ARRAY('report', 'presentation', 'memo', 'proposal', 'technical', 'financial', 'marketing', 'legal', 'other')
            )
        """))
        .withColumn("extracted_metadata", expr("""
            ai_extract(
                left(full_text, 4000),
                ARRAY('title', 'author', 'key_topics'),
                MAP('instructions', 'Extract these fields from the document.')
            )
        """)))

# ============================================================
# STAGE 5: SINK -- Write to Elasticsearch
# Replaces: Custom ES writer service
# ============================================================
ES_HOST = "https://YOUR-ES-ENDPOINT.com"
ES_INDEX = "documents"

@dp.foreach_batch_sink(name="elasticsearch_sink")
def write_to_elasticsearch(df, batch_id):
    (df.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", ES_HOST)
        .option("es.port", "443")
        .option("es.net.ssl", "true")
        .option("es.nodes.wan.only", "true")
        .option("es.index.auto.create", "true")
        .option("es.mapping.id", "doc_id")
        .mode("append")
        .save(ES_INDEX))

@dp.append_flow(target="elasticsearch_sink")
def es_output_flow():
    return spark.readStream.table("enriched_documents")

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
# MAGIC > **Presenter note:** Show the pipeline UI if possible (screenshot or live demo). The visual quality metrics are very compelling -- especially the pass/fail rates per expectation. Ask the audience: "How do you currently know if your parsing service is silently failing on certain document types?" With expectations, that answer is one click away in the pipeline UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Deploying and Running the Pipeline
# MAGIC
# MAGIC Now that we have the pipeline code, here is how to deploy and run it:
# MAGIC
# MAGIC ### Step 1: Save this notebook as a pipeline source
# MAGIC This notebook (or a standalone `.py` file with the same code) becomes the **pipeline source**. No packaging, no Docker containers, no deployment scripts.
# MAGIC
# MAGIC ### Step 2: Create a Lakeflow pipeline in the Databricks UI
# MAGIC 1. Navigate to **Pipelines** in the left sidebar
# MAGIC 2. Click **Create Pipeline**
# MAGIC 3. Select this notebook as the source
# MAGIC 4. Configure the pipeline settings
# MAGIC
# MAGIC ### Step 3: Pipeline Settings
# MAGIC
# MAGIC | Setting | Value | Notes |
# MAGIC |---|---|---|
# MAGIC | **Catalog** | `workshop` | Where the output tables are created |
# MAGIC | **Target Schema** | `default` | Schema within the catalog |
# MAGIC | **Compute** | Serverless or assigned cluster | Serverless is recommended for variable workloads |
# MAGIC | **Channel** | `current` or `preview` | Use `preview` for latest features |
# MAGIC
# MAGIC ### Step 4: Choose a Run Mode
# MAGIC
# MAGIC | Mode | Behavior | Best For |
# MAGIC |---|---|---|
# MAGIC | **Triggered (batch)** | Processes all available data, then stops | Scheduled runs (e.g., every hour, nightly) |
# MAGIC | **Continuous (streaming)** | Runs indefinitely, processing data as it arrives | Near-real-time requirements (sub-minute latency) |
# MAGIC
# MAGIC For a typical enterprise use case, **triggered mode on a schedule** (e.g., every 30 minutes) is likely the right starting point. Documents arrive throughout the day but do not need sub-second processing. You can always switch to continuous mode later without changing any code.
# MAGIC
# MAGIC > **Presenter note:** If you have a live workspace available, walk through creating the pipeline in the UI. Show the configuration screen. Emphasize that the entire deployment is: (1) point at a notebook, (2) set catalog and schema, (3) click Run. No CI/CD pipeline needed for the initial setup -- though of course you would add that for production. The contrast with deploying a new microservice (Docker build, ECR push, ECS task definition update, CloudFormation/Terraform) should be stark.

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
# MAGIC > **Presenter note:** This is a natural architecture discussion point. Let the audience debate for a few minutes. Some teams prefer full isolation (Option B) for operational simplicity. Others prefer shared downstream (Option A) for code reuse. There is no wrong answer, but Option A is typically better for organizations with many similar sources feeding the same enrichment and search pipeline -- which is exactly the situation for organizations with diverse document sources.

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
# MAGIC > **Presenter note:** If you have a running pipeline, show the DAG view now. Click into a table node and show the row counts and expectations metrics. Click into the event log and show a failed run with its error message. The visual impact of seeing the entire pipeline health at a glance -- versus logging into AWS Console and clicking through CloudWatch, SQS, and DynamoDB separately -- is the final proof point. End this section by saying: "This is one Python file. This is one UI. This is the entire system."

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
# MAGIC
# MAGIC **The core message:** A single Python file with five decorated functions replaces ~20 AWS components (Lambda functions, SQS queues, Step Functions, DynamoDB tables, CloudWatch alarms, and custom microservices). The pipeline is easier to write, easier to debug, easier to monitor, and easier to evolve.
# MAGIC
# MAGIC > **Presenter note:** Let this sink in. Ask the room: "How long would it take to add a new enrichment step -- say, language detection -- to your current system?" (Answer: new service, new queue, new Lambda trigger, new DynamoDB table, CloudFormation update, testing, deployment -- days to weeks.) "How long in this pipeline?" (Answer: add a `.withColumn()` call to the `enriched_documents` function -- minutes.) That contrast is the entire value proposition.