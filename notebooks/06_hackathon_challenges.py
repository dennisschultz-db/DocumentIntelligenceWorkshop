# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # Block 6: Hackathon Challenges
# MAGIC **Day 2 | 10:45 - 12:00 (1 hour 15 minutes)**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC Welcome to the hackathon! Here's how to make the most of it:
# MAGIC
# MAGIC - **Time:** You have **1 hour 15 minutes** of hands-on building time
# MAGIC - **Teams:** Work **individually or in small teams** -- your choice
# MAGIC - **Scope:** Pick **1-2 challenges** that interest you most. Depth beats breadth here
# MAGIC - **Help:** Facilitators are circulating -- raise your hand or flag us down with questions
# MAGIC - **Sharing:** You'll have **2 minutes to demo** what you built when we reconvene at 12:00
# MAGIC
# MAGIC ### Challenge Overview
# MAGIC
# MAGIC | # | Challenge | Difficulty | Focus Area |
# MAGIC |---|-----------|------------|------------|
# MAGIC | 1 | Custom Enrichment Chain | Medium | Enrichment + Tagging Services |
# MAGIC | 2 | Metadata-Driven Routing | Medium | Per-source pipeline architecture |
# MAGIC | 3 | Multimodal Slide Analysis | Hard | Image extraction + LLM enrichment |
# MAGIC | 4 | Incremental Pipeline with Change Tracking | Hard | Full vs. incremental crawl management |
# MAGIC | 5 | Elasticsearch Index Design | Medium | Search index design + querying |
# MAGIC | 6 | Data Quality and Observability | Medium | Operational concerns |
# MAGIC
# MAGIC Scroll down to find the challenge that interests you. Each one includes context on what to build, starter code, and TODO items to guide you.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 1: Custom Enrichment Chain (Medium)
# MAGIC
# MAGIC **Goal:** Build a multi-step enrichment flow: **classify -> extract entities -> summarize**
# MAGIC
# MAGIC Store intermediate results in Delta tables so you can inspect output at each stage.
# MAGIC
# MAGIC **Architecture mapping:** This maps to the **Enrichment + Tagging Services** in your target architecture -- the idea that documents flow through a chain of AI-powered transformations, each adding structured metadata.
# MAGIC
# MAGIC **What success looks like:**
# MAGIC - A `challenge1_classified` table with document categories
# MAGIC - A `challenge1_entities` table with extracted entities per category
# MAGIC - A `challenge1_summaries` table with category-aware summaries

# COMMAND ----------

# Challenge 1: Custom Enrichment Chain
# Goal: Build a 3-step enrichment pipeline with intermediate tables

# Step 1: Classify documents
# TODO: Create a table with document classifications

# Step 2: Extract entities from each category
# TODO: Use ai_extract with category-specific instructions

# Step 3: Generate targeted summaries based on category
# TODO: Use ai_query with category-aware prompts

# Starter code:
from pyspark.sql.functions import expr

df_text = spark.table("workshop.default.document_text")

# Step 1: Classify
df_classified = (df_text
    .groupBy("path")
    .agg(expr("concat_ws('\\n', collect_list(content))").alias("full_text"))
    .withColumn("category", expr("""
        ai_classify(left(full_text, 2000),
            ARRAY('report', 'presentation', 'memo', 'proposal', 'technical'))
    """)))

df_classified.write.mode("overwrite").saveAsTable("workshop.default.challenge1_classified")
display(spark.table("workshop.default.challenge1_classified"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 2: Metadata-Driven Routing (Medium)
# MAGIC
# MAGIC **Goal:** Use SharePoint metadata or file properties to route documents through different processing paths.
# MAGIC
# MAGIC For example:
# MAGIC - **PDFs** get full parsing with `ai_parse_document`
# MAGIC - **PPTXs** get slide image extraction via `imageOutputPath`
# MAGIC - **DOCXs** get text-only extraction
# MAGIC
# MAGIC **Architecture mapping:** This maps to **folder/metadata exclusion** and **per-source pipeline architecture** -- the idea that not every document should follow the same processing path.
# MAGIC
# MAGIC **What success looks like:**
# MAGIC - Documents split by file type with counts
# MAGIC - Different parsing strategies applied per type
# MAGIC - Different enrichment logic based on document content type

# COMMAND ----------

# Challenge 2: Metadata-Driven Routing
# Goal: Route documents to different processing paths based on file type

df = spark.read.format("binaryFile").load("/Volumes/workshop/default/documents/")

# Split by file type
df_pdfs = df.filter("path LIKE '%.pdf'")
df_pptx = df.filter("path LIKE '%.pptx'")
df_docx = df.filter("path LIKE '%.docx'")

print(f"PDFs: {df_pdfs.count()}, PPTXs: {df_pptx.count()}, DOCXs: {df_docx.count()}")

# TODO: Apply different parsing strategies per type
# - PDFs: full parsing with ai_parse_document
# - PPTXs: parsing with imageOutputPath for slide images
# - DOCXs: text-only parsing

# TODO: Apply different enrichment based on document type
# - Technical docs: extract architecture details, technologies mentioned
# - Financial docs: extract numbers, dates, amounts
# - Marketing docs: extract key messages, target audience

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 3: Multimodal Slide Analysis (Hard)
# MAGIC
# MAGIC **Goal:** For each slide image, use a vision model to describe visual content.
# MAGIC
# MAGIC Extract chart data, identify diagrams, detect logos/branding -- go beyond what text extraction alone can do.
# MAGIC
# MAGIC **Architecture mapping:** This maps to **image extraction + LLM enrichment** -- the idea that slide decks contain rich visual information (charts, diagrams, layouts) that text-only parsing misses entirely.
# MAGIC
# MAGIC **What success looks like:**
# MAGIC - Slide images analyzed with structured descriptions
# MAGIC - Chart/diagram data extracted from visual content
# MAGIC - A searchable index of slide visual content

# COMMAND ----------

# Challenge 3: Multimodal Slide Analysis
# Goal: Analyze slide images with a vision model

# First, ensure we have slide images from the parsing step
display(dbutils.fs.ls("/Volumes/workshop/default/slide_images/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze each slide image with Llama 4 Maverick
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_query(
# MAGIC     'databricks-llama-4-maverick',
# MAGIC     'Analyze this slide image. Describe: 1) The overall layout and design, 2) Any charts, graphs, or diagrams present, 3) Key text content visible, 4) Any logos or branding elements. Be specific and structured.',
# MAGIC     files => content
# MAGIC   ) AS slide_analysis
# MAGIC FROM read_files('/Volumes/workshop/default/slide_images/', format => 'binaryFile')
# MAGIC LIMIT 5

# COMMAND ----------

# TODO: Save the analysis results and create a searchable slide index
# TODO: Try extracting specific data from charts (e.g., "What are the numbers in this chart?")
# TODO: Compare the vision model's text extraction with ai_parse_document's text extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 4: Incremental Pipeline with Change Tracking (Hard)
# MAGIC
# MAGIC **Goal:** Build a pipeline that detects updated documents (not just new ones) and re-processes only the changed ones.
# MAGIC
# MAGIC **Architecture mapping:** This maps to **full vs. incremental crawl management** -- the idea that after the initial load, you should only re-process documents that have actually changed, not the entire corpus every time.
# MAGIC
# MAGIC **What success looks like:**
# MAGIC - A tracking table that records when each document was last processed
# MAGIC - Logic that identifies new and updated documents
# MAGIC - A MERGE statement that updates only changed records

# COMMAND ----------

# Challenge 4: Incremental Pipeline with Change Tracking
# Goal: Detect and re-process updated documents

# Approach: Use modificationTime to track changes
# 1. Maintain a "last_processed" timestamp per document
# 2. On each run, compare modificationTime to last_processed
# 3. Re-process only documents where modificationTime > last_processed

from pyspark.sql.functions import col, current_timestamp, max as spark_max

# Simulate: read all documents with their modification times
df_current = (spark.read.format("binaryFile")
    .load("/Volumes/workshop/default/documents/")
    .select("path", "modificationTime", "length", "content"))

# TODO: Create a tracking table that records when each document was last processed
# TODO: Join current files with tracking table to find new/updated documents
# TODO: Process only the changed documents
# TODO: Update the tracking table after processing

# Hint: Delta Lake's MERGE statement is perfect for this
# MERGE INTO processed_tracking USING new_files ON path
# WHEN MATCHED AND new_files.modificationTime > processed_tracking.last_processed THEN UPDATE
# WHEN NOT MATCHED THEN INSERT

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 5: Elasticsearch Index Design (Medium)
# MAGIC
# MAGIC **Goal:** Design an ES index mapping tailored to your search use cases, write enriched documents with proper field types, and query from this notebook.
# MAGIC
# MAGIC **Architecture mapping:** This maps to **search index design** -- the idea that a well-designed index mapping with the right field types and analyzers is critical for search relevance and performance.
# MAGIC
# MAGIC **What success looks like:**
# MAGIC - A thoughtful index mapping with appropriate field types
# MAGIC - Documents written to the index with proper structure
# MAGIC - Working search queries executed from the notebook

# COMMAND ----------

# Challenge 5: Elasticsearch Index Design
# Goal: Design a proper ES index and write enriched documents

%pip install elasticsearch

# COMMAND ----------

from elasticsearch import Elasticsearch

# TODO: Define an index mapping that supports their search needs
index_mapping = {
    "mappings": {
        "properties": {
            "path": {"type": "keyword"},
            "title": {"type": "text", "analyzer": "english"},
            "full_text": {"type": "text", "analyzer": "english"},
            "summary": {"type": "text", "analyzer": "english"},
            "category": {"type": "keyword"},
            "author": {"type": "keyword"},
            "key_topics": {"type": "keyword"},
            "modified_date": {"type": "date"},
            "file_type": {"type": "keyword"},
            # TODO: Add more fields as needed
            # Consider: nested objects for slide-level data
            # Consider: dense_vector for embedding-based search
        }
    }
}

# TODO: Create the index with your mapping
# TODO: Write documents to ES
# TODO: Run search queries from the notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 6: Data Quality and Observability (Medium)
# MAGIC
# MAGIC **Goal:** Add comprehensive data quality expectations to the pipeline and explore system tables for monitoring.
# MAGIC
# MAGIC **Architecture mapping:** This maps to **operational concerns** -- the idea that production pipelines need guardrails, alerting, and visibility into what's happening at each stage.
# MAGIC
# MAGIC **What success looks like:**
# MAGIC - Expectations defined for each pipeline stage
# MAGIC - System table queries that show pipeline health
# MAGIC - A clear picture of how to monitor this pipeline in production

# COMMAND ----------

# Challenge 6: Data Quality and Observability
# Goal: Build robust data quality checks

from pyspark import pipelines as dp

# TODO: Add expectations to each pipeline stage
# Example expectations:
# - raw_documents: content IS NOT NULL, length > 0, length < 100MB
# - parsed_documents: parsed IS NOT NULL, size(elements) > 0
# - enriched_documents: summary IS NOT NULL, category IS NOT NULL

# Explore system tables for monitoring

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View pipeline event log (if a pipeline has been run)
# MAGIC -- SELECT * FROM event_log(TABLE(workshop.default.raw_documents))
# MAGIC -- ORDER BY timestamp DESC
# MAGIC -- LIMIT 20
# MAGIC
# MAGIC -- View data quality metrics
# MAGIC -- SELECT * FROM event_log(TABLE(workshop.default.raw_documents))
# MAGIC -- WHERE event_type = 'flow_progress'
# MAGIC -- ORDER BY timestamp DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## When You're Done
# MAGIC
# MAGIC **Prepare a 2-minute demo** of what you built. Be ready to share:
# MAGIC
# MAGIC 1. **What you tried** -- which challenge(s) did you pick and why?
# MAGIC 2. **What worked** -- show us the output, the tables, the queries
# MAGIC 3. **What surprised you** -- anything unexpected, tricky, or particularly cool?
# MAGIC
# MAGIC We'll reconvene at **12:00** for share-outs. Each team/individual gets 2 minutes.
# MAGIC
# MAGIC ---
# MAGIC *Good luck and have fun building!*