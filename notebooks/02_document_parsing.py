# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Block 3: Document Parsing with `ai_parse_document`
# MAGIC **Day 1 | 11:00 - 11:55**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC
# MAGIC | Topic | Time |
# MAGIC |---|---|
# MAGIC | Guided: `ai_parse_document` deep-dive | 25 min |
# MAGIC | Hands-on: Parse and explore your own documents | 30 min |
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
# MAGIC No infrastructure to deploy, no Step Functions to maintain, no custom container images to build.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Guided Section: `ai_parse_document`
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
# MAGIC > **Presenter Note:** Emphasize the zero-infrastructure angle. They currently maintain container images,
# MAGIC > ECS tasks, and Step Function state machines just to parse documents. All of that goes away.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Parse a PDF Document
# MAGIC
# MAGIC Let's start with the simplest possible example -- reading a PDF file and parsing it in a single query.
# MAGIC
# MAGIC > **Presenter Note:** Walk through each part of the query:
# MAGIC > - `read_files` with `binaryFile` format reads raw file bytes
# MAGIC > - `pathGlobFilter` selects only PDFs
# MAGIC > - `ai_parse_document` does all the heavy lifting
# MAGIC > - The `MAP('version', '2.0')` parameter selects the latest parser version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC FROM read_files(
# MAGIC   '/Volumes/workshop/default/documents/',
# MAGIC   format => 'binaryFile',
# MAGIC   pathGlobFilter => '*.pdf'
# MAGIC )
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding the Output Structure
# MAGIC
# MAGIC The `ai_parse_document` function returns a **VARIANT** (semi-structured JSON) with this schema:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "pages": [
# MAGIC     {
# MAGIC       "pageId": 0,
# MAGIC       "imageUri": "dbfs:/..."   // Only if imageOutputPath is set
# MAGIC     }
# MAGIC   ],
# MAGIC   "elements": [
# MAGIC     {
# MAGIC       "type": "text",           // Element classification
# MAGIC       "content": "...",          // The actual extracted content
# MAGIC       "confidence": 0.97,       // Model confidence (0.0 - 1.0)
# MAGIC       "bbox": { ... },          // Bounding box on the page
# MAGIC       "pageId": 0               // Which page this element is on
# MAGIC     }
# MAGIC   ],
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
# MAGIC > **Presenter Note:** Highlight that tables come back as HTML -- this is important because it preserves
# MAGIC > row/column structure. They can parse the HTML downstream or feed it directly to an LLM for extraction.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Explore the Parsed Output Structure
# MAGIC
# MAGIC Let's use VARIANT path notation (the `:` syntax) to pull out specific parts of the parsed output.
# MAGIC
# MAGIC > **Presenter Note:** Show how `doc:metadata`, `doc:pages`, `doc:elements` use the colon path
# MAGIC > syntax to navigate into the VARIANT structure. This is native Databricks SQL -- no UDFs needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH parsed AS (
# MAGIC   SELECT
# MAGIC     path,
# MAGIC     ai_parse_document(content, MAP('version', '2.0')) AS doc
# MAGIC   FROM read_files('/Volumes/workshop/default/documents/', format => 'binaryFile', pathGlobFilter => '*.pdf')
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT
# MAGIC   path,
# MAGIC   doc:metadata AS metadata,
# MAGIC   size(doc:pages) AS num_pages,
# MAGIC   size(doc:elements) AS num_elements
# MAGIC FROM parsed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Extract and Flatten Elements
# MAGIC
# MAGIC The real power comes from flattening the `elements` array so each content block becomes its own row.
# MAGIC We use `LATERAL VIEW explode()` to unnest the array.
# MAGIC
# MAGIC > **Presenter Note:** This is the key pattern they will use repeatedly:
# MAGIC > 1. Parse the document into VARIANT
# MAGIC > 2. Explode the elements array into rows
# MAGIC > 3. Filter by element type
# MAGIC > 4. Work with the content downstream (chunking, embedding, LLM calls)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH parsed AS (
# MAGIC   SELECT path, ai_parse_document(content, MAP('version', '2.0')) AS doc
# MAGIC   FROM read_files('/Volumes/workshop/default/documents/', format => 'binaryFile', pathGlobFilter => '*.pdf')
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT
# MAGIC   path,
# MAGIC   elem.type AS element_type,
# MAGIC   elem.content AS content,
# MAGIC   elem.confidence AS confidence
# MAGIC FROM parsed
# MAGIC LATERAL VIEW explode(doc:elements) AS elem
# MAGIC WHERE elem.type IN ('text', 'title', 'section_header', 'table')

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
# MAGIC > **Presenter Note:** This is a great moment to pause and let the audience react. Their current
# MAGIC > pipeline uses a dedicated Conversion Service to render slides as images. Here it is a single parameter.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parse a PPTX and output each slide as an image
# MAGIC SELECT
# MAGIC   path,
# MAGIC   ai_parse_document(
# MAGIC     content,
# MAGIC     MAP(
# MAGIC       'version', '2.0',
# MAGIC       'imageOutputPath', '/Volumes/workshop/default/slide_images/'
# MAGIC     )
# MAGIC   ) AS parsed
# MAGIC FROM read_files('/Volumes/workshop/default/documents/', format => 'binaryFile', pathGlobFilter => '*.pptx')
# MAGIC LIMIT 1

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
# MAGIC **This directly answers the Bain requirement:**
# MAGIC > "Each slide outputted as an image" + "all text pulled from the slide"
# MAGIC
# MAGIC With `ai_parse_document`, you get **text extraction AND image rendering** in one call. No separate
# MAGIC Conversion Service needed.
# MAGIC
# MAGIC In **Day 2**, we will send these slide images to multimodal LLMs for visual analysis -- extracting
# MAGIC information from charts, diagrams, and layouts that text-only parsing would miss.
# MAGIC
# MAGIC > **Presenter Note:** Connect this forward to Day 2. The generated images are not just for storage --
# MAGIC > they become inputs to vision models. This is the "multimodal pipeline" advantage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: View the Generated Slide Images
# MAGIC
# MAGIC Let's confirm the images were created by listing the output Volume.

# COMMAND ----------

# List the generated slide images
display(dbutils.fs.ls("/Volumes/workshop/default/slide_images/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Extract Tables from Documents
# MAGIC
# MAGIC Tables are returned as **HTML strings**, preserving their row and column structure. This makes them
# MAGIC easy to parse downstream or feed directly to an LLM for structured extraction.
# MAGIC
# MAGIC > **Presenter Note:** Show that the HTML output is well-formed. You can render it in a notebook
# MAGIC > with `displayHTML()`, parse it with BeautifulSoup, or pass it directly to an LLM and ask
# MAGIC > "extract the data from this table as JSON."

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH parsed AS (
# MAGIC   SELECT path, ai_parse_document(content, MAP('version', '2.0')) AS doc
# MAGIC   FROM read_files('/Volumes/workshop/default/documents/', format => 'binaryFile')
# MAGIC   LIMIT 3
# MAGIC )
# MAGIC SELECT
# MAGIC   path,
# MAGIC   elem.type,
# MAGIC   elem.content  -- Tables are returned as HTML
# MAGIC FROM parsed
# MAGIC LATERAL VIEW explode(doc:elements) AS elem
# MAGIC WHERE elem.type = 'table'

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
# MAGIC > **Presenter Note:** Acknowledge that some participants may have experience with these libraries.
# MAGIC > The point is not that they are bad tools -- it is that `ai_parse_document` handles 90%+ of use cases
# MAGIC > with a fraction of the code and no infrastructure overhead.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Hands-on Exercise: Parse and Explore Documents
# MAGIC ---
# MAGIC
# MAGIC **Time: 30 minutes**
# MAGIC
# MAGIC Now it is your turn. Work through the exercises below to build familiarity with `ai_parse_document`.
# MAGIC
# MAGIC | Exercise | Goal |
# MAGIC |---|---|
# MAGIC | 1 | Parse all documents and save to a Delta table |
# MAGIC | 2 | Count elements by type across all documents |
# MAGIC | 3 | Extract all text from a specific document |
# MAGIC | 4 (Stretch) | Generate slide images for a PowerPoint |
# MAGIC
# MAGIC > **Presenter Note:** Circulate the room during this section. Common stumbling points:
# MAGIC > - Participants may try to parse very large files -- remind them of the 500-page PDF limit
# MAGIC > - The `content` column from `read_files` is binary -- it cannot be displayed directly
# MAGIC > - VARIANT path notation uses `:` not `.` (e.g., `parsed:elements` not `parsed.elements`)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1: Parse All Documents and Save to a Delta Table
# MAGIC
# MAGIC This exercise uses the **PySpark API** to parse all documents in the Volume and persist the results
# MAGIC as a Delta table. This is the pattern you would use in a production pipeline.
# MAGIC
# MAGIC **What is happening here:**
# MAGIC 1. `spark.read.format("binaryFile")` reads every file as raw bytes
# MAGIC 2. `expr("ai_parse_document(...)")` calls the SQL function from PySpark
# MAGIC 3. `.saveAsTable()` persists the results as a managed Delta table in Unity Catalog
# MAGIC
# MAGIC > **Presenter Note:** This is the first time participants see `ai_parse_document` called from Python
# MAGIC > via `expr()`. Emphasize that it is the exact same function -- PySpark's `expr()` lets you call any
# MAGIC > SQL function inline.

# COMMAND ----------

# Exercise 1: Parse all documents and save to a Delta table
from pyspark.sql.functions import expr

df = (spark.read
    .format("binaryFile")
    .load("/Volumes/workshop/default/documents/"))

df_parsed = df.withColumn(
    "parsed",
    expr("ai_parse_document(content, MAP('version', '2.0'))")
)

df_parsed.write.mode("overwrite").saveAsTable("workshop.default.parsed_documents")
print("Parsed documents saved!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Count Elements by Type Across All Documents
# MAGIC
# MAGIC Now that the parsed results are in a Delta table, let's analyze what types of content the parser found.
# MAGIC This gives you a high-level inventory of your document corpus.
# MAGIC
# MAGIC **What to look for:**
# MAGIC - `text` elements should be the most common
# MAGIC - `table` elements indicate structured data you can extract
# MAGIC - `figure` elements are charts/images that may need multimodal analysis (Day 2)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 2: Count elements by type across all documents
# MAGIC SELECT
# MAGIC   elem.type AS element_type,
# MAGIC   COUNT(*) AS count
# MAGIC FROM workshop.default.parsed_documents
# MAGIC LATERAL VIEW explode(parsed:elements) AS elem
# MAGIC GROUP BY elem.type
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3: Extract All Text from a Specific Document
# MAGIC
# MAGIC Pick a document from the table and extract its readable content. This is the foundation for
# MAGIC building a RAG pipeline -- you need clean, structured text to chunk and embed.
# MAGIC
# MAGIC **TODO:** Modify the `WHERE` clause to filter for a specific document path you are interested in.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exercise 3: Extract all text from a specific document
# MAGIC -- TODO: Change the path filter to a document you're interested in
# MAGIC SELECT
# MAGIC   path,
# MAGIC   elem.type,
# MAGIC   elem.content
# MAGIC FROM workshop.default.parsed_documents
# MAGIC LATERAL VIEW explode(parsed:elements) AS elem
# MAGIC WHERE elem.type IN ('text', 'title', 'section_header')
# MAGIC ORDER BY elem.type

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4 (Stretch): Generate Slide Images for a PowerPoint
# MAGIC
# MAGIC This exercise combines everything: read a PowerPoint file, parse it with image output enabled,
# MAGIC and verify the slide images were created.
# MAGIC
# MAGIC **Key details:**
# MAGIC - `.collect()` forces Spark to execute the computation (lazy evaluation)
# MAGIC - The images will appear in the Volume path you specified
# MAGIC - Each image corresponds to one slide
# MAGIC
# MAGIC > **Presenter Note:** If no PPTX files exist in the Volume, this will return an empty result.
# MAGIC > Have a sample PPTX ready to upload if needed.

# COMMAND ----------

# Exercise 4 (Stretch): Generate slide images for a PowerPoint
from pyspark.sql.functions import expr

df_pptx = (spark.read
    .format("binaryFile")
    .load("/Volumes/workshop/default/documents/")
    .filter("path LIKE '%.pptx'")
    .limit(1))

df_with_images = df_pptx.withColumn(
    "parsed",
    expr("""ai_parse_document(content, MAP(
        'version', '2.0',
        'imageOutputPath', '/Volumes/workshop/default/slide_images/'
    ))""")
)
df_with_images.collect()  # Trigger the parsing
display(dbutils.fs.ls("/Volumes/workshop/default/slide_images/"))

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
# MAGIC 4. The **`imageOutputPath`** parameter renders slides/pages as PNG images -- directly solving the "slide as image" requirement
# MAGIC 5. The same function works from **SQL and PySpark** via `expr()`
# MAGIC 6. Results persist naturally in **Delta tables** for reuse across the pipeline
# MAGIC
# MAGIC ### Architecture Impact
# MAGIC
# MAGIC ```
# MAGIC BEFORE (Current State):
# MAGIC   S3 Upload -> Lambda -> Step Functions -> Parsing Service -> Conversion Service -> S3
# MAGIC   (6 components, multiple failure points, custom error handling)
# MAGIC
# MAGIC AFTER (Databricks):
# MAGIC   Volume -> ai_parse_document() -> Delta Table
# MAGIC   (1 function call, automatic retries, governed by Unity Catalog)
# MAGIC ```
# MAGIC
# MAGIC ### What's Next
# MAGIC
# MAGIC In the next block, we will take the parsed content and **chunk it for embedding** -- the next step
# MAGIC toward building a RAG pipeline. In Day 2, we will use the slide images with multimodal LLMs.
# MAGIC
# MAGIC > **Presenter Note:** Before moving on, ask if anyone encountered errors or unexpected results.
# MAGIC > Common issues: file format not supported, Volume path permissions, very large files timing out.
