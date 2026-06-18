from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# ============================================================
# STAGE 3: FLATTEN -- Extract and structure text content
# ============================================================
@dp.materialized_view(
    comment="Flattened document text ready for enrichment"
)
def pipeline_document_text():
    return (spark.read.table("pipeline_parsed_documents")
        .selectExpr(
            "path",
            "modificationTime",
            "explode(try_cast(elements AS ARRAY<VARIANT>)) as element"
        )
        .selectExpr(
            "path",
            "modificationTime",
            "try_cast(element:type AS STRING) as element_type",
            "try_cast(element:content AS STRING) as content"
        )
        .filter("element_type IN ('text', 'title', 'section_header', 'table')"))
