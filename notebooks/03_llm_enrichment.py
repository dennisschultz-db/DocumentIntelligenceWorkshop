# Databricks notebook source

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

# MAGIC %md
# MAGIC ### Step 1: Prepare Text Data from Parsed Documents
# MAGIC
# MAGIC Before we can enrich documents, we need to reassemble the parsed elements back into full-text documents. We'll concatenate all text-bearing elements (text, titles, section headers) per file.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, let's prepare our document text for enrichment
# MAGIC -- We reassemble parsed elements into full document text
# MAGIC CREATE OR REPLACE TABLE workshop.default.document_text AS
# MAGIC SELECT
# MAGIC   path,
# MAGIC   concat_ws('\n', collect_list(elem.content)) AS full_text
# MAGIC FROM workshop.default.parsed_documents
# MAGIC LATERAL VIEW explode(parsed:elements) AS elem
# MAGIC WHERE elem.type IN ('text', 'title', 'section_header')
# MAGIC GROUP BY path;
# MAGIC
# MAGIC SELECT path, length(full_text) AS text_length, left(full_text, 200) AS preview
# MAGIC FROM workshop.default.document_text

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: `ai_query` -- Summarize Documents
# MAGIC
# MAGIC `ai_query()` is the most flexible AI function. You specify a model, a prompt (which can include your document text), and optionally a response format.
# MAGIC
# MAGIC > **Presenter note**: This will take a few seconds per row as each row makes an LLM call. We limit to 3 rows for the demo; batch processing strategies are covered later.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use ai_query to generate document summaries
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     'Summarize this document in 3-5 sentences. Focus on the key topics and conclusions:\n\n' || left(full_text, 4000)
# MAGIC   ) AS summary
# MAGIC FROM workshop.default.document_text
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: `ai_classify` -- Categorize Documents
# MAGIC
# MAGIC `ai_classify()` is purpose-built for classification tasks. You provide the text and an array of candidate labels -- the function returns the best-matching label. No prompt engineering required.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Classify documents into predefined categories
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_classify(
# MAGIC     left(full_text, 2000),
# MAGIC     ARRAY('report', 'presentation', 'memo', 'proposal', 'technical', 'financial', 'marketing', 'legal', 'other')
# MAGIC   ) AS category
# MAGIC FROM workshop.default.document_text
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: `ai_extract` -- Structured Field Extraction
# MAGIC
# MAGIC `ai_extract()` pulls out specific fields from unstructured text. You define the fields you want, and the function returns them as a struct.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract structured fields from document text
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_extract(
# MAGIC     left(full_text, 4000),
# MAGIC     ARRAY('title', 'author', 'date', 'key_topics', 'summary'),
# MAGIC     MAP('instructions', 'Extract these fields from the document. Return null if not found.')
# MAGIC   ) AS extracted
# MAGIC FROM workshop.default.document_text
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: `ai_query` with Structured Output (`responseFormat`)
# MAGIC
# MAGIC For richer structured output, use `ai_query()` with the `responseFormat` parameter. This constrains the LLM's output to match a specific schema -- no JSON parsing headaches.
# MAGIC
# MAGIC > **Presenter note**: The `responseFormat` parameter uses Spark SQL struct syntax. The LLM is constrained to return valid output matching this schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use ai_query with structured output for complex metadata extraction
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     'Analyze this document and extract structured metadata:\n\n' || left(full_text, 4000),
# MAGIC     responseFormat => 'STRUCT<title:STRING, document_type:STRING, key_topics:ARRAY<STRING>, sentiment:STRING, actionable_items:ARRAY<STRING>>'
# MAGIC   ) AS structured_analysis
# MAGIC FROM workshop.default.document_text
# MAGIC LIMIT 3

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

# MAGIC %sql
# MAGIC -- Send slide images to a vision model for analysis
# MAGIC -- This reads image files directly from a Volume and sends them to Llama 4 Maverick
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_query(
# MAGIC     'databricks-llama-4-maverick',
# MAGIC     'Describe what is shown in this slide image. Extract any visible text, describe charts or diagrams, and identify key visual elements.',
# MAGIC     files => content
# MAGIC   ) AS image_analysis
# MAGIC FROM read_files('/Volumes/workshop/default/slide_images/', format => 'binaryFile')
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC > **Presenter note**: If no slide images are available from the parsing step, this cell will return an empty result or error. That's expected -- participants will generate slide images during the pipeline assembly block. You can also upload a sample image to the volume to demonstrate.

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
# MAGIC ### Create a Classification UDF

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a reusable classification function
# MAGIC CREATE OR REPLACE FUNCTION workshop.default.classify_document(text STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN ai_classify(text, ARRAY('report', 'presentation', 'memo', 'proposal', 'technical', 'financial', 'marketing', 'legal'));

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Summarization UDF
# MAGIC
# MAGIC > **Presenter note**: Notice the `modelParameters` -- we set `max_tokens` to 200 to keep summaries concise and `temperature` to 0.3 for more deterministic output.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a reusable summarization function
# MAGIC CREATE OR REPLACE FUNCTION workshop.default.summarize_document(text STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN ai_query(
# MAGIC   'databricks-meta-llama-3-3-70b-instruct',
# MAGIC   'Provide a concise 2-3 sentence summary:\n\n' || text,
# MAGIC   modelParameters => named_struct('max_tokens', 200, 'temperature', 0.3)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use UDFs on Documents
# MAGIC
# MAGIC Now we can use these functions just like any built-in SQL function. They work anywhere SQL works in Databricks.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use our custom UDFs to enrich documents
# MAGIC SELECT
# MAGIC   path,
# MAGIC   workshop.default.classify_document(left(full_text, 2000)) AS category,
# MAGIC   workshop.default.summarize_document(left(full_text, 4000)) AS summary
# MAGIC FROM workshop.default.document_text
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC > **Presenter note**: These UDFs will be reused in the DLT pipeline (Block 5). This is the power of the approach -- define once, use everywhere.

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3: Hands-on Exercise -- Enrich Your Documents (30 min)
# MAGIC
# MAGIC ## Your Turn: Enrich Your Documents
# MAGIC
# MAGIC Now it's your turn to apply LLM enrichment to the documents you ingested and parsed yesterday. Work through the exercises below, using the guided examples above as reference.
# MAGIC
# MAGIC **Goals**:
# MAGIC 1. Summarize all documents and save results to a table
# MAGIC 2. Classify documents into custom categories relevant to your use case
# MAGIC 3. Extract structured metadata fields
# MAGIC 4. (Stretch) Analyze slide images with the multimodal model
# MAGIC 5. Combine everything into a single enriched documents table
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

# MAGIC %md
# MAGIC ### Exercise 1: Summarize All Documents
# MAGIC
# MAGIC Generate a summary for every document and save the results to a table. Use `failOnError => false` to handle any problematic documents gracefully.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Exercise 1 - Summarize all documents and save to a table
# MAGIC -- Hint: Use ai_query with failOnError => false for robustness
# MAGIC -- Save the results with CREATE OR REPLACE TABLE
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workshop.default.document_summaries AS
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     'Summarize this document in 3-5 sentences. Focus on the key topics and conclusions:\n\n' || left(full_text, 4000),
# MAGIC     failOnError => false
# MAGIC   ) AS summary
# MAGIC FROM workshop.default.document_text

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Classify Documents into Custom Categories
# MAGIC
# MAGIC Choose categories that are relevant to your organization's document types. Replace the example categories below with ones that make sense for your data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Exercise 2 - Classify documents into custom categories
# MAGIC -- Replace the categories below with ones relevant to YOUR documents
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workshop.default.document_categories AS
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_classify(
# MAGIC     left(full_text, 2000),
# MAGIC     -- TODO: Replace these categories with your own
# MAGIC     ARRAY('strategy', 'operations', 'finance', 'technology', 'hr', 'legal', 'marketing', 'other')
# MAGIC   ) AS category
# MAGIC FROM workshop.default.document_text

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3: Extract Structured Metadata
# MAGIC
# MAGIC Use `ai_query` with `responseFormat` to extract structured metadata from each document. Customize the fields to match what's useful for your search and discovery needs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Exercise 3 - Extract structured metadata from documents
# MAGIC -- Customize the struct fields to match your needs
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workshop.default.document_metadata AS
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     'Extract structured metadata from this document:\n\n' || left(full_text, 4000),
# MAGIC     -- TODO: Customize the response format fields below
# MAGIC     responseFormat => 'STRUCT<title:STRING, document_type:STRING, key_topics:ARRAY<STRING>, sentiment:STRING, actionable_items:ARRAY<STRING>>',
# MAGIC     failOnError => false
# MAGIC   ) AS metadata
# MAGIC FROM workshop.default.document_text

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4 (Stretch): Multimodal -- Analyze Slide Images
# MAGIC
# MAGIC If you have slide images from the parsing step, send them to the vision model for analysis. This is optional -- skip if no images are available.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO (Stretch): Exercise 4 - Analyze slide images with a multimodal model
# MAGIC -- This requires slide images to be available in the volume from the parsing step
# MAGIC -- Customize the prompt to extract what's most useful for your use case
# MAGIC
# MAGIC -- CREATE OR REPLACE TABLE workshop.default.slide_analysis AS
# MAGIC -- SELECT
# MAGIC --   path,
# MAGIC --   ai_query(
# MAGIC --     'databricks-llama-4-maverick',
# MAGIC --     'Analyze this slide image. Extract all visible text, describe any charts or diagrams, and summarize the key message of the slide.',
# MAGIC --     files => content,
# MAGIC --     failOnError => false
# MAGIC --   ) AS slide_description
# MAGIC -- FROM read_files('/Volumes/workshop/default/slide_images/', format => 'binaryFile')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5: Combine Everything into Enriched Documents
# MAGIC
# MAGIC Join all your enrichment results into a single table. This will be the input for the DLT pipeline and Elasticsearch indexing in the next blocks.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Exercise 5 - Combine all enrichments into a single table
# MAGIC -- Join summaries, categories, and metadata into one enriched view
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workshop.default.enriched_documents AS
# MAGIC SELECT
# MAGIC   t.path,
# MAGIC   t.full_text,
# MAGIC   s.summary,
# MAGIC   c.category,
# MAGIC   m.metadata,
# MAGIC   m.metadata.title AS title,
# MAGIC   m.metadata.key_topics AS key_topics,
# MAGIC   m.metadata.sentiment AS sentiment,
# MAGIC   current_timestamp() AS enriched_at
# MAGIC FROM workshop.default.document_text t
# MAGIC LEFT JOIN workshop.default.document_summaries s ON t.path = s.path
# MAGIC LEFT JOIN workshop.default.document_categories c ON t.path = c.path
# MAGIC LEFT JOIN workshop.default.document_metadata m ON t.path = m.path

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the enriched documents table
# MAGIC SELECT * FROM workshop.default.enriched_documents

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint
# MAGIC
# MAGIC At this point you should have:
# MAGIC - [x] `workshop.default.document_text` -- reassembled full text per document
# MAGIC - [x] `workshop.default.document_summaries` -- AI-generated summaries
# MAGIC - [x] `workshop.default.document_categories` -- document classifications
# MAGIC - [x] `workshop.default.document_metadata` -- structured metadata extraction
# MAGIC - [x] `workshop.default.enriched_documents` -- combined enrichment table
# MAGIC
# MAGIC **Next up**: Block 5 -- Full Pipeline Assembly with Delta Live Tables, where we'll wire ingestion, parsing, and enrichment into a single production pipeline.
