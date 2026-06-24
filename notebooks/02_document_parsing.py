# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Block 3: Document Parsing with `ai_parse_document`
# MAGIC **Day 1
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC
# MAGIC | Topic |
# MAGIC |---|
# MAGIC | `ai_parse_document` deep-dive |
# MAGIC | Parsing PowerPoint |
# MAGIC | Parsing documents with tables |
# MAGIC
# MAGIC ## Why This Matters
# MAGIC
# MAGIC In your current architecture, parsing a single document requires **three services working together**:
# MAGIC
# MAGIC ```
# MAGIC Parsing Service --> Conversion Service --> Step Functions (orchestration)
# MAGIC ```
# MAGIC
# MAGIC With Databricks, this entire pipeline collapses into **one SQL function call**:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT ai_parse_document(content, MAP('version', '2.0')) FROM ...
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %run ./_resources/Config

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # `ai_parse_document`
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introduction to `ai_parse_document`
# MAGIC
# MAGIC `ai_parse_document` is a built-in Databricks SQL function powered by a **multimodal foundation model**. It extracts structured content from unstructured documents with a single function call.
# MAGIC
# MAGIC ### What it replaces
# MAGIC
# MAGIC | Current Stack | Databricks Equivalent |
# MAGIC |---|---|
# MAGIC | Parsing Service | `ai_parse_document()` |
# MAGIC | Conversion Service | `ai_parse_document()` |
# MAGIC | Step Functions (orchestration) | `ai_parse_document()` |
# MAGIC | Custom infrastructure | **None -- fully managed** |
# MAGIC
# MAGIC ### Supported file types
# MAGIC
# MAGIC | Format | Notes |
# MAGIC |---|---|
# MAGIC | **PDF** | Up to 500 pages per document |
# MAGIC | **PowerPoint** | PPT and PPTX |
# MAGIC | **Word** | DOC and DOCX |
# MAGIC | **Images** | PNG, JPEG, TIFF, BMP, GIF |
# MAGIC
# MAGIC ### What you get back
# MAGIC
# MAGIC The function returns **structured VARIANT output** with element-level detail -- every paragraph, heading, table, and figure is individually identified with its type, content, confidence score, and bounding box coordinates.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Parse a PDF Document
# MAGIC
# MAGIC Let's start with the simplest possible example -- reading a PDF file and parsing it in a single query.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Parse a single pdf document
display(spark.sql("""
SELECT
  fileName,
  ai_parse_document(
      content, 
      MAP('version', '2.0')) AS parsed
FROM 01_bronze_raw_documents
WHERE fileName = "sample_report_fy2026.pdf"
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the Output Structure
# MAGIC
# MAGIC The `ai_parse_document` function returns a **VARIANT** (semi-structured JSON) with this schema:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "document": {
# MAGIC     "elements": [
# MAGIC       {
# MAGIC         "type": "text",           // Element classification
# MAGIC         "content": "...",          // The actual extracted content
# MAGIC         "confidence": 0.97,       // Model confidence (0.0 - 1.0)
# MAGIC         "bbox": { ... },          // Bounding box on the page
# MAGIC         "pageId": 0               // Which page this element is on
# MAGIC       }
# MAGIC     ],
# MAGIC     "pages": [
# MAGIC       {
# MAGIC         "pageId": 0,
# MAGIC         "imageUri": "dbfs:/..."   // Only if imageOutputPath is set
# MAGIC       }
# MAGIC     ]
# MAGIC   }
# MAGIC   "error_status": null,          // Any processing errors
# MAGIC   "metadata": {
# MAGIC     "fileName": "...",
# MAGIC     "schemaVersion": "2.0"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Element Types
# MAGIC
# MAGIC | Type | Description |
# MAGIC |---|---|
# MAGIC | `text` | Body paragraphs and general text |
# MAGIC | `title` | Document title |
# MAGIC | `section_header` | Section and subsection headings |
# MAGIC | `table` | Tables (returned as **HTML**) |
# MAGIC | `figure` | Embedded figures and charts |
# MAGIC | `caption` | Figure/table captions |
# MAGIC | `page_header` | Running headers |
# MAGIC | `page_footer` | Running footers |
# MAGIC | `footnote` | Footnotes |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Explore the Parsed Output Structure
# MAGIC
# MAGIC Use VARIANT path notation (the `:` syntax) to pull out specific parts of the parsed output.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Extract metadata from single doc
display(spark.sql("""
    WITH parsed AS (
    SELECT
        fileName,
        ai_parse_document(
            content, 
            MAP('version', '2.0')) AS doc
    FROM 01_bronze_raw_documents
    WHERE fileName = "sample_report_fy2026.pdf"
    )
    SELECT
        fileName,
        doc:metadata AS metadata,
        size(doc:document:pages::ARRAY<VARIANT>) AS num_pages,
        size(doc:document:elements::ARRAY<VARIANT>) AS num_elements
        FROM parsed
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Extract and Flatten Elements
# MAGIC
# MAGIC The real power comes from flattening the `elements` array so each content block becomes its own row.
# MAGIC We use `LATERAL VIEW explode()` to unnest the array.
# MAGIC
# MAGIC > **Key Pattern:** This is the key pattern you will use repeatedly:
# MAGIC > 1. Parse the document into VARIANT
# MAGIC > 2. Explode the elements array into rows
# MAGIC > 3. Filter by element type
# MAGIC > 4. Work with the content downstream (chunking, embedding, LLM calls)

# COMMAND ----------

# DBTITLE 1,Flatten text-type elements
display(spark.sql("""
    WITH parsed AS (
        SELECT 
            fileName, 
            ai_parse_document(
                content, 
                MAP('version', '2.0')) AS doc
        FROM 01_bronze_raw_documents
        WHERE fileName = "sample_report_fy2026.pdf"
    )
    SELECT
        fileName,
        elem:type::STRING AS element_type,
        elem:content::STRING AS content,
        elem:confidence::DOUBLE AS confidence
    FROM parsed
        LATERAL VIEW explode(doc:document:elements::ARRAY<VARIANT>) AS elem
    WHERE elem:type::STRING IN ('text', 'title', 'section_header', 'table')
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Parse a PowerPoint with Slide Images
# MAGIC
# MAGIC One of the most powerful features of `ai_parse_document` is the `imageOutputPath` option.
# MAGIC When set, each slide or page is **rendered as a PNG image** and saved to the specified Unity Catalog Volume.
# MAGIC
# MAGIC This directly addresses the requirement:
# MAGIC > *"Each slide outputted as an image"* + *"All text pulled from the slide"*
# MAGIC
# MAGIC You get **both** in a single function call.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Capture slide images and descriptions
# Parse a PPTX and output each slide as an image
display(spark.sql(f"""
    WITH parsed AS (
    SELECT 
        fileName, 
        ai_parse_document(
            content,
            MAP(
            'version', '2.0',
            'descriptionElementTypes', 'figure',
            'imageOutputPath', '/Volumes/{catalog}/{schema}/{personal_volume}/slide_images/')
        ) as doc
    FROM 01_bronze_raw_documents
    WHERE fileName = "DatabricksSHORTOverviewDeck.pptx"
    )
    SELECT
        fileName,
        elem:type::STRING AS element_type,
        (elem:bbox[0]:page_id::INT + 1)::INT AS slide_number,
        (elem:bbox[0]:coord::ARRAY<INT>)::STRING AS coordinates,
        elem:description::STRING AS description,
        elem:confidence::DOUBLE AS confidence
    FROM parsed
        LATERAL VIEW explode(doc:document:elements::ARRAY<VARIANT>) AS elem
    WHERE elem:type::STRING IN ('figure')
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Point: `imageOutputPath`
# MAGIC
# MAGIC When you set `imageOutputPath`:
# MAGIC
# MAGIC - Each **slide** (PowerPoint) or **page** (PDF) is rendered as a **PNG image**
# MAGIC - Images are saved to the specified **Unity Catalog Volume** path
# MAGIC - The `pages` array in the output includes `imageUri` fields pointing to each image
# MAGIC - This works for **both** PowerPoint and PDF files
# MAGIC
# MAGIC **This directly answers the requirement:**
# MAGIC > "Each slide outputted as an image" + "all text pulled from the slide"
# MAGIC
# MAGIC With `ai_parse_document`, you get **text extraction AND image rendering** in one call. No separate
# MAGIC Conversion Service needed.
# MAGIC
# MAGIC In **Day 2**, we will send these slide images to multimodal LLMs for visual analysis -- extracting
# MAGIC information from charts, diagrams, and layouts that text-only parsing would miss.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: View the Generated Slide Images
# MAGIC
# MAGIC Confirm the images were created by listing the output Volume.

# COMMAND ----------

# DBTITLE 1,List slide images folder
# List the generated slide images
display(dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/{personal_volume}/slide_images/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Extract Tables from Documents
# MAGIC
# MAGIC Tables are returned as **HTML strings**, preserving their row and column structure. This makes them
# MAGIC easy to parse downstream or feed directly to an LLM for structured extraction.
# MAGIC
# MAGIC > **Note:** The HTML output is well-formed. You can render it in a notebook with `displayHTML()`, parse it with BeautifulSoup, or pass it directly to an LLM and ask "extract the data from this table as JSON."

# COMMAND ----------

# DBTITLE 1,Extract Tables
display(spark.sql("""
    WITH parsed AS (
        SELECT 
            fileName, 
            ai_parse_document(
                content, 
                MAP('version', '2.0')) AS doc
        FROM 01_bronze_raw_documents
        WHERE fileName = "sample_report_fy2026.pdf"
    )
    SELECT
        fileName,
        elem:type::STRING AS element_type,
        elem:content::STRING as table_content -- Tables are returned as HTML
    FROM parsed
        LATERAL VIEW explode(doc:document:elements::ARRAY<VARIANT>) AS elem
    WHERE elem:type::STRING = 'table'
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A Note on the Python Approach
# MAGIC
# MAGIC You **can** use Python libraries for lower-level parsing if you need very specific control:
# MAGIC
# MAGIC | Library | Use Case |
# MAGIC |---|---|
# MAGIC | `python-pptx` | Programmatic PowerPoint manipulation |
# MAGIC | `PyMuPDF` (fitz) | Low-level PDF text/image extraction |
# MAGIC | `python-docx` | Word document manipulation |
# MAGIC
# MAGIC **However, `ai_parse_document` is the recommended approach because:**
# MAGIC
# MAGIC - Far less code to write and maintain
# MAGIC - Better results -- powered by a multimodal foundation model that understands layout
# MAGIC - No library installation or version management
# MAGIC - Handles edge cases (scanned PDFs, complex layouts) that rule-based parsers miss
# MAGIC - Built-in confidence scores for quality assessment
# MAGIC - Automatic image rendering with `imageOutputPath`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Recap: What We Covered
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **`ai_parse_document`** is a single SQL function that replaces your Parsing Service, Conversion Service, and Step Functions orchestration
# MAGIC 2. It returns **structured VARIANT output** with element-level classification (text, table, figure, etc.)
# MAGIC 3. Tables are extracted as **HTML**, preserving structure for downstream processing
# MAGIC 4. The **`imageOutputPath`** parameter renders slides/pages as PNG images
# MAGIC 4. The **descriptionElementsTypes** provides an AI generated description for any figures and images in the document
# MAGIC 5. The same function works from **SQL and PySpark** via `expr()`
# MAGIC
# MAGIC
# MAGIC ### What's Next
# MAGIC
# MAGIC `ai_parse_document()` has limitations.  We will explore those in the next module.