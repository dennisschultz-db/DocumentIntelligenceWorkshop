from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# ============================================================
# STAGE 3: FLATTEN -- Extract and structure text content
# ============================================================
@dp.materialized_view(
    comment="Flattened document text ready for enrichment"
)
def p03_gold_document_text():
    return (spark.sql("""
        SELECT
            fileName,
            concat_ws(' ', collect_list(chunk:chunk_to_retrieve::STRING)) AS text,
            concat_ws(' ', collect_list(chunk:chunk_to_embed::STRING)) AS text_with_context
        FROM (
            SELECT
                fileName,
                chunk
            FROM p02_silver_parsed_documents
            LATERAL VIEW explode(ai_prep_search(parsed):document:contents::ARRAY<VARIANT>) AS chunk
        )
        GROUP BY fileName
""")
    )
