from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# ============================================================
# STAGE 4: ENRICH -- LLM classification, summarization, extraction
# Replaces: Enrichment Service + Tagging Service + Embedding Service
# ============================================================
@dp.materialized_view(
    comment="Documents enriched with LLM-generated summaries and classifications"
)
def pipeline_enriched_documents():
    return (spark.read.table("pipeline_document_text")
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
                '["title", "author", "key_topics"]'
            )
        """)))
