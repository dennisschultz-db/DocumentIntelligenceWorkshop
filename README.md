# Databricks Document Processing Workshop


### Two Half-Day Sessions (8 hrs total)

---

## Pre-Workshop Setup (completed before Day-of)

- Databricks workspace provisioned with Unity Catalog enabled
- Serverless compute enabled (required for AI Functions)
- Participant accounts created with workspace access
  - Entitlements granted via group
- Previews enabled
  - Beta
    - AI Prep Search
    - AI Search: Quality Evaluation
    - Custom LLM Serving for Databricks Model Serving
    - Discover Page
    - Lakeflow Connect for Sharepoint
    - Supervisor API
    - Third Party Connectors for Agents
    - Upload Local PDFs to Genie Spaces
    - Vector Search: Full-Text Search
  - Public Preview
    - AI Classify
    - AI Extract
    - AI Query for Custom Models and External Models
    - AI Search Storage Optimized
    - Agent Frameworks: On-Behalf-Of-User Authorization
    - Enable Extended Models
    - Enhanced Python UDFs in Unity Catalog
    - External Tool Calling for Agents
    - Genie - Upload File
    - Genie Agent
    - Genie Answer Inspection
    - Lakeflow Designer
    - Lakeflow Pipelines Editor
    - Managed MCP Servers
    - Synthetic Agent Evaluation
    - Vector Search High QPS
- Settings enabled
  - Advanced -> Choose entitlements when adding principals to workspaces
- SharePoint OAuth connection pre-configured in Unity Catalog
  - This will require a Principal for access, or one of the other access modes
- DBR `16.4 LTS (includes Apache Spark 3.5.2, Scala 2.13)` cluster configured (classic cluster required for Elasticsearch Maven library installation)
  - For **Standard** access mode, you will need to whiteList the Maven coordinates used to load the library into the compute. This requires Metastore admin permissions.
  - For **Dedicated** access mode, you will need an Account-level group if more than one person is to have access.
  - Install the Elasticsearch connector library (es-hadoop) `org.elasticsearch:elasticsearch-spark-30_2.13:9.3.2` Maven library on the culster
- Network connectivity to SharePoint and Elasticsearch verified
- Git Folder to https://github.com/dennisschultz-db/DocumentIntelligenceWorkshop
  - Pre-built notebooks loaded for guided exercises
- Run `Pre-workshop Setup` notebook
  - Create schema and volumes
  - Permissions granted for participants
  - Sample documents uploaded to a Unity Catalog Volume (PDFs, PPTX, DOCX)

---

## Workshop Agenda

---

# Day 1: Foundations, Ingestion, and Parsing (8:00 - 12:00)

---

### Block 1: Foundations (8:00 - 9:30) -- 1.5 hrs

#### Databricks Platform Orientation (8:00 - 8:45) -- 45 min | Guided

> *Goal: Get all participants comfortable navigating the workspace.*

- Welcome and workshop objectives
- Architecture comparison: their current system (13+ queues, 8+ microservices) vs. what we'll build across both sessions (single platform, declarative pipeline)
- Workspace tour
  - Navigation: workspace browser, catalog explorer, SQL editor, compute
  - Unity Catalog: catalogs, schemas, tables, volumes
  - What is a notebook? Cells, languages (Python/SQL/Markdown), attaching compute
- Hands-on: Each participant opens the workspace, navigates to the pre-loaded catalog, and runs a "hello world" notebook

#### Core Concepts for Today (8:45 - 9:30) -- 45 min | Guided

> *Goal: Establish the mental model for how Databricks replaces the queue-based microservices pattern.*

- Delta Lake: what it is and why it replaces S3 + SQS as the glue between pipeline stages
- Medallion architecture (bronze/silver/gold) mapped to their flow: raw documents -> parsed content -> enriched output
- Auto Loader: how incremental file processing replaces EventBridge + Lambda triggers
- Lakeflow Declarative Pipelines: how a single Python file replaces Step Functions + orchestration services
- Live demo: show a simple 3-stage pipeline running end-to-end (pre-built)

---

### Break (9:30 - 9:45) -- 15 min

---

### Block 2: Ingestion -- SharePoint Connector (9:45 - 11:00) -- 1.25 hrs

#### Guided: Connecting to SharePoint (9:45 - 10:15) -- 30 min

> *Goal: Demonstrate that a single connection replaces the custom SharePoint sync service.*

- Walk through the pre-configured Unity Catalog connection (OAuth setup)
- Read binary files from SharePoint using `spark.read.format("binaryFile")`
- Read binary files using SQL `read_files()` function
- Demonstrate `pathGlobFilter` for file type filtering (*.pdf, *.pptx, *.docx)
- Show `_sharepoint_metadata` column (Runtime 18+): `mime_type`, `created_by_email`, `last_modified_by_name`, `parent_path`
- Discuss how metadata-based filtering replaces folder exclusion logic

#### Guided: Auto Loader for Incremental Processing (10:15 - 10:45) -- 30 min

> *Goal: Show how Auto Loader handles full and incremental crawls automatically.*

- Switch from batch `spark.read` to streaming `spark.readStream` with `cloudFiles`
- Demonstrate: first run processes all existing files (full crawl)
- Add a new file to SharePoint, show Auto Loader picks it up (incremental crawl)
- Explain checkpoint-based tracking (exactly-once, no reprocessing)
- Discuss file notification mode vs. directory listing mode for production
- **Direct answer to their question**: full crawl = first run with `includeExistingFiles=True`; incremental = every subsequent trigger

#### Hands-on Exercise (10:45 - 11:00) -- 15 min

> *Participants work through a notebook that reads files from a pre-loaded Volume (or SharePoint if connectivity allows) using both batch and streaming modes.*

- Read PDF and PPTX files from a Volume
- Inspect the schema: `path`, `modificationTime`, `length`, `content`
- Filter by file type and modification date
- Count files by type

---

### Block 3: Document Parsing (11:00 - 11:55) -- 55 min

#### Guided: ai_parse_document (11:00 - 11:25) -- 25 min

> *Goal: Show that a single SQL function replaces the Parsing Service + Conversion Service + Step Functions.*

- Introduction to `ai_parse_document` -- multimodal foundation model, no infrastructure to manage
- Parse a PDF: examine output structure (pages, elements, metadata, error_status)
- Parse a PowerPoint: show how each slide's text, tables, and figures are extracted
- Element types: text, title, section_header, table (HTML), figure, caption, page_header, page_footer
- Demo `imageOutputPath` option: render each slide/page as an image saved to a Volume
  - **Direct answer to their requirement**: "each slide outputted as an image" + "all text pulled from the slide"
- Demo `descriptionElementTypes` option: AI-generated descriptions for figures
- Show how to flatten/explode the parsed output into a tabular format

#### Hands-on Exercise (11:25 - 11:55) -- 30 min

> *Participants parse documents from the ingestion step and explore the output.*

- Use `ai_parse_document` on their ingested files (SQL and Python)
- Extract all text elements from a PowerPoint presentation
- Generate slide images using `imageOutputPath`
- Extract tables from a PDF and convert to a DataFrame
- (Stretch) Parse a complex multi-page PDF and explore element-level confidence scores

---

### Day 1 Wrap-Up (11:55 - 12:00) -- 5 min

- Recap what we covered: platform orientation, SharePoint ingestion, document parsing
- Preview Day 2: LLM enrichment, full pipeline assembly, Elasticsearch, hackathon
- Encourage participants to explore the workspace between sessions

---

# Day 2: Enrichment, Pipelines, and Hackathon (8:00 - 12:45)

---

### Day 1 Recap and Day 2 Preview (8:00 - 8:15) -- 15 min

- Quick recap: what we built on Day 1 (ingestion + parsing)
- Address any questions that came up between sessions
- Day 2 roadmap: LLM enrichment, assembling the full pipeline, Elasticsearch sink, hackathon

---

### Block 4: LLM Enrichment (8:15 - 9:30) -- 1.25 hrs

#### Guided: AI Functions for Enrichment (8:15 - 8:45) -- 30 min

> *Goal: Show that SQL-native AI functions replace the Enrichment Service + Tagging Service + Embedding Service.*

- Overview of Databricks AI Functions: `ai_query`, `ai_classify`, `ai_extract`, `ai_summarize`, `ai_similarity`
- Available foundation models (Llama 3.3, Llama 4 Maverick for multimodal, Claude, GPT)
- Demo `ai_query` with custom prompt on parsed text
  ```sql
  SELECT path, ai_query('databricks-meta-llama-3-3-70b-instruct',
      'Summarize this document in 3 sentences: ' || full_text) AS summary
  FROM parsed_documents
  ```
- Demo `ai_classify` for document categorization
- Demo `ai_extract` for structured field extraction with `responseFormat`
- Demo multimodal: send slide images to `ai_query` with Llama 4 Maverick
  - **Direct answer to their requirement**: "send text/images to an LLM, store enriched content"
- Show structured output with `responseFormat => 'STRUCT<...>'` -- no post-processing needed
- Discuss batch processing best practices: `failOnError => false`, full-dataset queries

#### Guided: Creating Reusable Enrichment Functions (8:45 - 9:00) -- 15 min

> *Goal: Show how SQL UDFs create reusable enrichment steps, similar to microservice contracts.*

- Create a SQL UDF wrapping `ai_query` for document classification
- Create a SQL UDF for entity extraction
- Show how these UDFs can be used across notebooks, pipelines, and dashboards
- Discuss how this maps to their architecture: one UDF per enrichment type vs. one microservice per enrichment type

#### Hands-on Exercise (9:00 - 9:30) -- 30 min

> *Participants enrich their parsed documents with LLM calls.*

- Summarize parsed documents using `ai_query`
- Classify documents into customer-defined categories using `ai_classify`
- Extract structured metadata (title, author, key topics, dates) using `ai_extract`
- (Stretch) Send slide images to a multimodal model for visual analysis
- Store all enriched output in a Delta table

---

### Break (9:30 - 9:45) -- 15 min

---

### Block 5: End-to-End Pipeline + Elasticsearch Sink (9:45 - 10:45) -- 1 hr

#### Guided: Writing to Elasticsearch (9:45 - 10:05) -- 20 min

> *Goal: Show the final mile -- writing enriched content to Elastic.*

- Configure the es-hadoop Spark connector (pre-installed on cluster)
- Batch write from a Delta table to an Elasticsearch index
- Show `es.mapping.id` for document-level upsert/idempotency
- Discuss: Delta table as primary store, Elasticsearch as search index (separation of storage and search)

#### Guided: Assembling the Declarative Pipeline (10:05 - 10:45) -- 40 min

> *Goal: Combine all pieces into a single Lakeflow Declarative Pipeline that replaces the entire microservices architecture.*

- Walk through a complete pipeline definition:
  - `raw_documents` -- streaming table from SharePoint via Auto Loader
  - `parsed_documents` -- materialized view using `ai_parse_document`
  - `document_text` -- materialized view that flattens/explodes elements
  - `enriched_documents` -- materialized view with LLM enrichment
  - `elasticsearch_sink` -- `@dp.foreach_batch_sink` for Elastic writes
- Data quality expectations: `@dp.expect_or_drop` for null checks, file size limits
- Deploy and run the pipeline
- Show the pipeline visualization UI (DAG view)
- Show monitoring: event log, data quality metrics, row counts per stage
- **Direct answer to their architecture question**: Single pipeline handles the common flow; additional `@dp.append_flow` sources can merge different connectors into the same pipeline. Per-source pipelines are also easy -- just duplicate the notebook with a different connection.

---

### Block 6: Hackathon (10:45 - 12:00) -- 1.25 hrs

> *Participants work independently or in small teams on challenges that map to their real-world needs. Facilitators circulate to assist.*

#### Suggested Challenges (pick 1-2)


| #   | Challenge                                                                                                                                                                  | Difficulty | Maps to Their Need                              |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- | ----------------------------------------------- |
| 1   | **Custom enrichment chain**: Build a multi-step enrichment flow -- classify, then extract entities, then summarize -- storing intermediate results in Delta tables         | Medium     | Enrichment + Tagging Services                   |
| 2   | **Metadata-driven routing**: Use SharePoint metadata or file properties to route documents through different parsing/enrichment paths                                      | Medium     | Folder/metadata exclusion, per-source pipelines |
| 3   | **Multimodal slide analysis**: For each slide image, use a vision model to describe visual content, extract chart data, and identify logos/branding                        | Hard       | Image extraction + LLM enrichment               |
| 4   | **Incremental pipeline with change tracking**: Build a pipeline that detects when a SharePoint document is updated (not just new) and re-processes only changed documents  | Hard       | Full vs. incremental crawl management           |
| 5   | **Elasticsearch index design**: Design an ES index mapping that supports their search use cases, write enriched documents, and query from a notebook                       | Medium     | Write to Elastic                                |
| 6   | **Data quality and observability**: Add comprehensive expectations to the pipeline, set up alerts on quality violations, and explore system tables for pipeline monitoring | Medium     | Operational concerns                            |


#### Resources Available During Hackathon

- Pre-built notebook templates for each challenge
- Databricks documentation links
- Sample documents of varying complexity
- Facilitators for questions and guidance

---

### Block 7: Wrap-Up and Architecture Discussion (12:00 - 12:45) -- 45 min

#### Hackathon Share-Out (12:00 - 12:15) -- 15 min

- Teams briefly demo what they built
- Discussion of challenges encountered and solutions found

#### Architecture Deep Dive (12:15 - 12:35) -- 20 min

> *Revisit the original architecture diagram with everything learned.*

- Side-by-side comparison: current AWS architecture vs. Databricks-based architecture
- Component mapping:

  | Current (AWS)                             | Databricks Equivalent                                  |
  | ----------------------------------------- | ------------------------------------------------------ |
  | SharePoint sync Lambda                    | SharePoint Connector + Auto Loader                     |
  | S3 staging buckets                        | Unity Catalog Volumes + Delta Tables                   |
  | 13 SQS queues                             | Implicit -- pipeline DAG handles ordering              |
  | Parsing Service                           | `ai_parse_document`                                    |
  | Conversion Service + Step Functions       | Lakeflow Declarative Pipeline                          |
  | Enrichment / Tagging / Embedding Services | AI Functions (`ai_query`, `ai_classify`, `ai_extract`) |
  | DynamoDB state tables                     | Delta Lake (ACID, time travel, CDF)                    |
  | Voyager Orchestrator                      | Lakeflow pipeline scheduler                            |
  | CloudWatch monitoring                     | Pipeline event log + system tables                     |
  | Elasticsearch writes                      | `foreach_batch_sink` in pipeline                       |

- Discuss: single pipeline vs. per-source pipelines
  - Recommendation: shared downstream stages (parsing, enrichment, ES sink), separate ingestion tables per source using `@dp.append_flow`
- Governance: Unity Catalog for access control, lineage, auditing

#### Open Q&A and Next Steps (12:35 - 12:45) -- 10 min

- Address remaining questions
- Discuss: what would a production migration path look like?
- Identify any gaps or features that need further investigation
- Share workshop notebooks and resources for continued exploration

---

## Pre-Loaded Workshop Artifacts

### Notebooks to Prepare

1. `00_hello_databricks` -- Workspace orientation, basic SQL/Python, catalog exploration
2. `01_sharepoint_ingestion` -- SharePoint connector examples (batch + streaming)
3. `02_document_parsing` -- `ai_parse_document` with PDF and PPTX samples
4. `03_llm_enrichment` -- AI Functions for classification, extraction, summarization
5. `04_elasticsearch_sink` -- Writing to Elasticsearch (batch + foreachBatch)
6. `05_full_pipeline` -- Complete Lakeflow Declarative Pipeline definition
7. `06_hackathon_templates` -- Starter notebooks for each hackathon challenge

### Sample Data

- 10-15 PDF documents of varying complexity (text-heavy, tables, images)
- 5-10 PowerPoint presentations (mix of text slides and visual slides)
- 3-5 Word documents
- Pre-parsed Delta table (for participants who want to skip ahead to enrichment)

### Cluster Configuration

- 16.4 LTS (includes Apache Spark 3.5.2, Scala 2.13) (for Elasticsearch Maven support)
- Libraries: `org.elasticsearch:elasticsearch-spark-30_2.13:9.3.2`, `python-pptx`, `PyMuPDF`
- Serverless SQL warehouse (for SharePoint ingestion, AI Functions in SQL editor, and everything other than Elasticsearch Maven library)
- Shared compute policy for all participants

---

## Key Databricks Features Demonstrated


| Feature                                                | Status           | Workshop Block |
| ------------------------------------------------------ | ---------------- | -------------- |
| SharePoint Connector                                   | Beta (DBR 17.3+) | Block 2        |
| Auto Loader (cloudFiles)                               | GA               | Block 2        |
| `ai_parse_document`                                    | GA               | Block 3        |
| AI Functions (`ai_query`, `ai_classify`, `ai_extract`) | Public Preview   | Block 4        |
| Multimodal LLM (Llama 4 Maverick)                      | GA               | Block 4        |
| Lakeflow Declarative Pipelines                         | GA               | Block 5        |
| `foreach_batch_sink` (Elastic write)                   | GA               | Block 5        |
| Unity Catalog Volumes                                  | GA               | Throughout     |
| Data Quality Expectations                              | GA               | Block 5        |


---

## Timing Summary

### Day 1 (8:00 - 12:00)


| Block                                    | Time          | Duration | Mode              |
| ---------------------------------------- | ------------- | -------- | ----------------- |
| 1 - Foundations                          | 8:00 - 9:30   | 1h 30m   | Guided            |
| Break                                    | 9:30 - 9:45   | 15m      | --                |
| 2 - Ingestion (SharePoint + Auto Loader) | 9:45 - 11:00  | 1h 15m   | Guided + Hands-on |
| 3 - Document Parsing                     | 11:00 - 11:55 | 55m      | Guided + Hands-on |
| Day 1 Wrap-Up                            | 11:55 - 12:00 | 5m       | Discussion        |
| **Day 1 Total**                          |               | **4h**   |                   |


### Day 2 (8:00 - 12:45)


| Block                        | Time          | Duration   | Mode              |
| ---------------------------- | ------------- | ---------- | ----------------- |
| Day 1 Recap + Day 2 Preview  | 8:00 - 8:15   | 15m        | Discussion        |
| 4 - LLM Enrichment           | 8:15 - 9:30   | 1h 15m     | Guided + Hands-on |
| Break                        | 9:30 - 9:45   | 15m        | --                |
| 5 - Pipeline + Elasticsearch | 9:45 - 10:45  | 1h         | Guided            |
| 6 - Hackathon                | 10:45 - 12:00 | 1h 15m     | Self-directed     |
| 7 - Wrap-up + Architecture   | 12:00 - 12:45 | 45m        | Discussion        |
| **Day 2 Total**              |               | **4h 45m** |                   |



|                    |                                                   |
| ------------------ | ------------------------------------------------- |
| **Workshop Total** | **~8 hrs of content across two morning sessions** |


