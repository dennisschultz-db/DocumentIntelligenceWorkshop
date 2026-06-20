from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# ============================================================
# STAGE 4: ENRICH -- LLM classification, summarization, extraction
# Replaces: Enrichment Service + Tagging Service + Embedding Service
# NOTE: pipeline_enriched_documents (MV) is now replaced by
#       pipeline_enriched_documents_v2 (ST). Drop the old table manually.
# ============================================================
@dp.table(
    comment="Documents enriched with LLM-generated summaries and classifications"
)
def p03_gold_enriched_documents():
    return (spark.readStream.table("p02_silver_parsed_documents")
        .withColumn("full_text", expr("""
            concat_ws('\n',
                filter(
                    transform(
                        try_cast(parsed:document:elements AS ARRAY<VARIANT>),
                        e -> CASE WHEN try_cast(e:type AS STRING) IN ('text', 'title', 'section_header', 'table')
                                  THEN try_cast(e:content AS STRING) END
                    ),
                    x -> x IS NOT NULL
                )
            )
        """))
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
        """))
        .select("fileName", "path", "modificationTime", "full_text", "summary", "category", "extracted_metadata"))
