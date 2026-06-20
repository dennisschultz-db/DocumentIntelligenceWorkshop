# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Day 2: LLM Enrichment with Databricks AI Functions
# MAGIC
# MAGIC ## Welcome Back! Day 1 Recap & Day 2 Preview
# MAGIC
# MAGIC ### What we accomplished yesterday (Day 1):
# MAGIC - **Platform Foundations**: Configured Unity Catalog, volumes, and workspace settings
# MAGIC - **SharePoint Ingestion**: Connected to SharePoint and ingested raw documents into volumes
# MAGIC - **Document Parsing**: Used Databricks Document Intelligence to parse PDFs, slides, and images into structured elements
# MAGIC
# MAGIC ### What we're building today (Day 2):
# MAGIC | Block | Topic | Time |
# MAGIC |-------|-------|------|
# MAGIC | **Block 4** | **LLM Enrichment** (this notebook) | 8:15 - 9:30 |
# MAGIC | Block 5 | Full Pipeline Assembly with DLT | 9:45 - 11:00 |
# MAGIC | Block 6 | Elasticsearch & Search Integration | 11:15 - 12:30 |
# MAGIC | Block 7 | Hackathon: Build Your Pipeline | 1:30 - 4:00 |
# MAGIC
# MAGIC ### Where we left off
# MAGIC Our parsed documents are stored in `workshop.default.parsed_documents`. Each row contains the file path and a JSON structure of parsed elements (text, titles, section headers, tables, images, etc.).
# MAGIC
# MAGIC **Today's goal**: Take those parsed documents and enrich them with AI-generated summaries, classifications, structured metadata, and image analysis -- then assemble everything into a production pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1: Databricks AI Functions for Enrichment (Guided, 30 min)
# MAGIC
# MAGIC ## Overview of Databricks AI Functions
# MAGIC
# MAGIC In a traditional architecture, enriching documents with AI requires deploying and managing multiple microservices:
# MAGIC - **Enrichment Service** -- calls an LLM to generate summaries, extract entities, etc.
# MAGIC - **Tagging Service** -- classifies documents into categories
# MAGIC - **Embedding Service** -- generates vector embeddings for search
# MAGIC
# MAGIC **Databricks AI Functions replace all of these with SQL-native function calls.** No infrastructure to manage, no endpoints to provision, no API keys to rotate.
# MAGIC
# MAGIC ### Available AI Functions
# MAGIC | Function | Purpose | Example Use Case |
# MAGIC |----------|---------|-----------------|
# MAGIC | `ai_query()` | General-purpose LLM calls with optional structured output | Summarization, Q&A, custom prompts |
# MAGIC | `ai_classify()` | Classify text into predefined categories | Document type tagging |
# MAGIC | `ai_extract()` | Extract structured fields from text | Metadata extraction (author, date, topics) |
# MAGIC | `ai_summarize()` | Generate summaries | Executive summaries |
# MAGIC | `ai_similarity()` | Compute semantic similarity between texts | Duplicate detection |
# MAGIC | `ai_translate()` | Translate text between languages | Multilingual support |
# MAGIC | `ai_gen()` | Generate text from a prompt | Content creation |
# MAGIC
# MAGIC ### Available Models
# MAGIC | Model | Strengths |
# MAGIC |-------|-----------|
# MAGIC | **Llama 3.3 70B** | Fast, cost-effective, great for classification and extraction |
# MAGIC | **Llama 4 Maverick** | Multimodal -- can process images alongside text |
# MAGIC | **Claude (Anthropic)** | Strong reasoning, long context |
# MAGIC | **GPT (OpenAI)** | General purpose, widely benchmarked |
# MAGIC
# MAGIC All models are **pay-per-token** -- no provisioned throughput or endpoint management needed.

# COMMAND ----------

# MAGIC %run ./_resources/Config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: `ai_prep_search` -- Prepare Text Data from Parsed Documents
# MAGIC
# MAGIC For some types of document enrichment, we will need to reassemble the parsed elements back into full-text documents. We'll concatenate all text-bearing elements (text, titles, section headers) per file.

# COMMAND ----------

# DBTITLE 1,Extract full text
# Assemble parsed elements into full document text, merging text and text_with_context per fileName
spark.sql("""
  CREATE OR REPLACE TABLE 03_gold_document_text AS
  SELECT
    fileName,
    concat_ws(' ', collect_list(chunk:chunk_to_retrieve::STRING)) AS text,
    concat_ws(' ', collect_list(chunk:chunk_to_embed::STRING)) AS text_with_context
  FROM (
    SELECT
      fileName,
      chunk
    FROM 02_silver_parsed_documents
      LATERAL VIEW explode(ai_prep_search(parsed):document:contents::ARRAY<VARIANT>) AS chunk
  )
  GROUP BY fileName
""")

display(spark.sql("SELECT * FROM 03_gold_document_text"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: `ai_classify` -- Categorize Documents
# MAGIC
# MAGIC `ai_classify()` is purpose-built for classification tasks. You provide the text and an array of candidate labels -- the function returns the best-matching label. No prompt engineering required.

# COMMAND ----------

# DBTITLE 1,Classify documents

# Classify documents using VARIANT chaining -- no text reassembly needed!
# ai_classify v2 accepts the parsed VARIANT directly from ai_parse_document,
# preserving document structure (titles, tables, sections) for better accuracy
display(spark.sql("""
  SELECT
    fileName,
    ai_classify(
      parsed,
      '["report", "presentation", "memo", "proposal", "technical", "financial", "marketing", "legal", "other"]',
      MAP('version', '2.0')
    ) AS category
  FROM 02_silver_parsed_documents
  LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: `ai_extract` -- Structured Field Extraction
# MAGIC
# MAGIC `ai_extract()` pulls out specific fields from unstructured text. You define the fields you want, and the function returns them as a struct.

# COMMAND ----------

# DBTITLE 1,Extract fields
# Extract structured fields directly from parsed VARIANT -- no text reassembly needed!
# ai_extract accepts the ai_parse_document VARIANT output, leveraging document structure
extract_instructions = "Extract these fields from the document. Return null if not found."

display(spark.sql(f"""
  SELECT
    fileName,
    ai_extract(
      parsed,
      '["title", "company", "product", "author", "date", "key_topics", "summary"]',
      MAP(
          'instructions', '{extract_instructions}',
          'version', '2.1',
          'enableCitations', 'true',
          'enableConfidenceScores', 'true')
    ) AS extracted
  FROM 02_silver_parsed_documents
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: `ai_query` -- Summarize Documents
# MAGIC
# MAGIC `ai_query()` is the most flexible AI function. You specify a model, a prompt (which can include your document text), and optionally a response format.
# MAGIC
# MAGIC For richer structured output, use the `responseFormat` parameter. This constrains the LLM's output to match a specific schema -- no JSON parsing required.  The `responseFormat` parameter uses Spark SQL struct syntax. The LLM is constrained to return valid output matching this schema.
# MAGIC
# MAGIC > **Note**: This will take a few seconds per row as each row makes an LLM call. We limit to 3 rows for the demo; batch processing strategies are covered later.

# COMMAND ----------

# DBTITLE 1,Summarize documents with an LLM
# Use ai_query to generate document summaries
llm_model = "databricks-meta-llama-3-3-70b-instruct"
prompt = "Analyze this document and extract structured metadata"

# Spark SQL struct type passed to responseFormat -- constrains the LLM to return this schema.
# responseFormat requires a single top-level wrapper field; ai_query unwraps it and returns
# the inner struct directly.
response_format = "STRUCT<result: STRUCT<title: STRING, author: STRING, date: STRING, key_topics: ARRAY<STRING>, summary: STRING, sentiment: STRING>>"

display(spark.sql(f"""
  SELECT
    fileName,
    ai_query(
      '{llm_model}',
      '{prompt}:\\n\\n' || left(text, 4000),
      responseFormat => '{response_format}'
    ) AS structured_analysis
  FROM 03_gold_document_text
  LIMIT 3
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Multimodal Capabilities
# MAGIC
# MAGIC **Llama 4 Maverick** is a multimodal model that can process **images alongside text**. This is critical for our use case:
# MAGIC
# MAGIC - PowerPoint slides parsed as images can be analyzed visually
# MAGIC - Charts, diagrams, and infographics can be described and indexed
# MAGIC - Handwritten notes or scanned documents get richer analysis
# MAGIC
# MAGIC **This directly addresses the core requirement**: *"send text/images to an LLM, store enriched content."*
# MAGIC
# MAGIC With `ai_query()` and the `files` parameter, we can send binary image content directly to the vision model -- no base64 encoding, no external API calls, no image hosting.

# COMMAND ----------

# DBTITLE 1,Analyze slide images
# Send slide images to a vision model for analysis
# Reads image files directly from a Volume and sends them to Llama 4 Maverick
vision_model = "databricks-llama-4-maverick"
vision_prompt = "Describe what is shown in this slide image. Extract any visible text, describe charts or diagrams, and identify key visual elements."

display(spark.sql(f"""
  SELECT
    path,
    ai_query(
      '{vision_model}',
      '{vision_prompt}',
      files => content
    ) AS image_analysis
  FROM read_files(
    '/Volumes/{catalog}/{schema}/{personal_volume}/slide_images', 
    format => 'binaryFile')
  LIMIT 3
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2: Reusable Enrichment Functions (Guided, 15 min)
# MAGIC
# MAGIC ## SQL UDFs as Reusable Enrichment Steps
# MAGIC
# MAGIC In a microservices architecture, each enrichment type is a separate service to deploy and maintain. With Databricks, each enrichment type becomes a **SQL User-Defined Function (UDF)**:
# MAGIC
# MAGIC | Microservice Approach | Databricks UDF Approach |
# MAGIC |----------------------|------------------------|
# MAGIC | Deploy a classification service | `CREATE FUNCTION classify_document(...)` |
# MAGIC | Deploy a summarization service | `CREATE FUNCTION summarize_document(...)` |
# MAGIC | Deploy an extraction service | `CREATE FUNCTION extract_metadata(...)` |
# MAGIC | Manage scaling, auth, monitoring per service | All handled by the platform |
# MAGIC
# MAGIC **Key advantages of UDFs**:
# MAGIC - Registered in Unity Catalog -- discoverable and governed
# MAGIC - Reusable across notebooks, DLT pipelines, dashboards, and SQL queries
# MAGIC - Version-controlled via the catalog
# MAGIC - No infrastructure to manage

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create UDFs that wrap built-in AI functions

# COMMAND ----------

# DBTITLE 1,UDFs for document enrichment
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION classify_document(text STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN ai_classify(
# MAGIC   left(text, 4000),
# MAGIC   '["report", "presentation", "memo", "proposal", "technical", "financial", "marketing", "legal", "other"]'
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION summarize_document(text STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN ai_summarize(
# MAGIC   left(text, 4000),
# MAGIC   max_words => 300
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION extract_document(text STRING)
# MAGIC RETURNS STRUCT<title: STRING, company: STRING, product: STRING, author: STRING, date: STRING, key_topics: ARRAY<STRING>, summary: STRING>
# MAGIC RETURN (
# MAGIC   WITH extracted AS (
# MAGIC     SELECT from_json(
# MAGIC       ai_extract(
# MAGIC         left(text, 4000),
# MAGIC         '["title", "company", "product", "author", "date", "key_topics", "summary"]',
# MAGIC         MAP(
# MAGIC           'instructions', 'Extract these fields from the document. Return null if not found.',
# MAGIC           'version', '2.1',
# MAGIC           'enableCitations', 'true',
# MAGIC           'enableConfidenceScores', 'true'
# MAGIC         )
# MAGIC       ):response::STRING,
# MAGIC       'STRUCT<title: STRUCT<value: STRING>, company: STRUCT<value: STRING>, product: STRUCT<value: STRING>, author: STRUCT<value: STRING>, date: STRUCT<value: STRING>, key_topics: STRUCT<value: STRING>, summary: STRUCT<value: STRING>>'
# MAGIC     ) AS response
# MAGIC   )
# MAGIC   SELECT named_struct(
# MAGIC     'title', response.title.value,
# MAGIC     'company', response.company.value,
# MAGIC     'product', response.product.value,
# MAGIC     'author', response.author.value,
# MAGIC     'date', response.date.value,
# MAGIC     'key_topics', CASE
# MAGIC       WHEN response.key_topics.value IS NULL THEN NULL
# MAGIC       ELSE split(response.key_topics.value, ',\\s*')
# MAGIC     END,
# MAGIC     'summary', response.summary.value
# MAGIC   )
# MAGIC   FROM extracted
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create a _Custom_ Summarization UDF
# MAGIC
# MAGIC Use `ai_query()` for unique requirements.
# MAGIC
# MAGIC > **Note**: Notice the `modelParameters` -- we set `max_tokens` to 200 to keep summaries concise and `temperature` to 0.3 for more deterministic output.

# COMMAND ----------

# DBTITLE 1,Summarization UDF using an LLM
# Create a reusable summarization function
max_tokens = 200
temperature = 0.3
llm_model = "databricks-meta-llama-3-3-70b-instruct"

spark.sql(f"""
  CREATE OR REPLACE FUNCTION summarize_document_pig_latin(text STRING)
  RETURNS STRING
  RETURN ai_query(
    '{llm_model}',
    'Provide a concise 2-3 sentence summary in pig latin:\\n\\n' || left(text, 4000),
    modelParameters => named_struct('max_tokens', {max_tokens}, 'temperature', {temperature})
  )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3: Combine Everything into Enriched Documents
# MAGIC
# MAGIC Now we can use these functions just like any built-in SQL function. They work anywhere SQL works in Databricks.
# MAGIC
# MAGIC Join all your enrichment results into a single table. This will be the input for the DLT pipeline and Elasticsearch indexing in the next blocks.
# MAGIC
# MAGIC **Goals**:
# MAGIC 1. Summarize all documents and save results to a table
# MAGIC 2. Classify documents into custom categories relevant to your use case
# MAGIC 3. Extract structured metadata fields
# MAGIC 4. Combine everything into a single enriched documents table
# MAGIC 5. (Stretch) Analyze slide images with the multimodal model
# MAGIC
# MAGIC
# MAGIC ### Batch Processing Best Practices
# MAGIC
# MAGIC When enriching a full dataset (not just `LIMIT 3`), keep these tips in mind:
# MAGIC - **Use `failOnError => false`** in `ai_query()` to prevent one bad row from failing the entire query
# MAGIC - **Truncate text** with `left(full_text, N)` to stay within model context windows
# MAGIC - **Process in batches** if you have hundreds of documents -- use `WHERE` clauses or row numbering
# MAGIC - **Start with a small `LIMIT`** to validate your prompt before running on the full dataset
# MAGIC - **Monitor costs** -- each AI function call uses pay-per-token billing

# COMMAND ----------

# DBTITLE 1,Use UDFs to produce enriched table
# MAGIC %sql
# MAGIC -- Apply the four UDFs: classify_document, summarize_document, extract_document, summarize_document_llm
# MAGIC CREATE OR REPLACE TABLE 03_gold_enriched_documents AS (
# MAGIC     WITH enriched AS (
# MAGIC         SELECT
# MAGIC             silver.fileName,
# MAGIC             from_json(
# MAGIC                 classify_document(silver.parsed),
# MAGIC                 'STRUCT<response: STRING, error_message: STRING>') AS classified_output,
# MAGIC             summarize_document(silver.parsed) AS summary,
# MAGIC             extract_document(silver.parsed) AS metadata,
# MAGIC             summarize_document_pig_latin(gold.text) AS summary_pig_latin
# MAGIC         FROM dennis_schultz.dennis_schultz.02_silver_parsed_documents silver
# MAGIC             LEFT JOIN dennis_schultz.dennis_schultz.03_gold_document_text gold
# MAGIC                 ON silver.fileName = gold.fileName
# MAGIC     )
# MAGIC     SELECT
# MAGIC         fileName,
# MAGIC         -- Extract first element from JSON string array in classified_output.response
# MAGIC         from_json(classified_output.response, 'ARRAY<STRING>')[0] AS category,
# MAGIC         classified_output.error_message AS error_message,
# MAGIC         metadata.title::STRING as title,
# MAGIC         metadata.company::STRING as company,
# MAGIC         metadata.product::STRING as product,
# MAGIC         metadata.author::STRING as author,
# MAGIC         metadata.date::STRING as date,
# MAGIC         metadata.key_topics::ARRAY<STRING> as key_topics,
# MAGIC         summary,
# MAGIC         summary_pig_latin
# MAGIC     FROM enriched
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Check the Gold table
# Verify the enriched documents table
display(spark.sql(f"SELECT * FROM 03_gold_enriched_documents"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multimodal -- Analyze Slide Images
# MAGIC
# MAGIC If you have slide images from the parsing step, send them to the vision model for analysis. This is optional -- skip if no images are available.

# COMMAND ----------

# DBTITLE 1,Analyze slide images
#  Analyze slide images with a multimodal model
# This requires slide images to be available in the volume from the parsing step
# Customize the prompt to extract what's most useful for your use case
slide_images_path = f"/Volumes/{catalog}/{schema}/{personal_volume}/slide_images"
slide_prompt = "Analyze this slide image. Extract all visible text, describe any charts or diagrams, and summarize the key message of the slide."

spark.sql(f"""
  CREATE OR REPLACE TABLE 03_gold_slide_analysis AS
  SELECT
    path,
    slide_description.result AS slide_result
  FROM (
    SELECT
      path,
      ai_query(
        '{vision_model}',
        '{slide_prompt}',
        files => content,
        failOnError => false
      ) AS slide_description
    FROM read_files('{slide_images_path}', format => 'binaryFile')
  )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Checkpoint
# MAGIC
# MAGIC At this point you should have:
# MAGIC - [x] `01_bronze_raw_documents` -- raw ingested binary content
# MAGIC - [x] `02_silver_parsed_documents` -- structured content and basic metadata 
# MAGIC - [x] `03_gold_document_text` -- reassembled full text per document
# MAGIC - [x] `03_gold_enriched_documents` -- AI-generated classifications, entities, metadata, summaries, and summaries in pig latin
# MAGIC - [x] `03_gold_slide_analysis` -- descriptions of each individual slide image extracted from PPT files.
# MAGIC
# MAGIC **Next up**: Block 5 -- Full Pipeline Assembly with Spark Declarative Pipelines, where we'll wire ingestion, parsing, and enrichment into a single production pipeline.