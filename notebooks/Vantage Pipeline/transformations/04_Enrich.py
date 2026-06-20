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
    catalog = spark.conf.get("catalog", "dennis_schultz")
    schema  = spark.conf.get("schema", "dennis_schultz")

    return (spark.sql(f"""
        WITH enriched AS (
            SELECT
                silver.fileName,
                from_json(
                    {catalog}.{schema}.classify_document(silver.parsed),
                    'STRUCT<response: STRING, error_message: STRING>') AS classified_output,
                {catalog}.{schema}.summarize_document(silver.parsed) AS summary,
                {catalog}.{schema}.extract_document(silver.parsed) AS metadata,
                {catalog}.{schema}.summarize_document_pig_latin(gold.text) AS summary_pig_latin
            FROM p02_silver_parsed_documents silver
                LEFT JOIN p03_gold_document_text gold
                    ON silver.fileName = gold.fileName
        )
        SELECT
            fileName,
            -- Extract first element from JSON string array in classified_output.response
            from_json(classified_output.response, 'ARRAY<STRING>')[0] AS category,
            classified_output.error_message AS error_message,
            metadata.title::STRING as title,
            metadata.company::STRING as company,
            metadata.product::STRING as product,
            metadata.author::STRING as author,
            metadata.date::STRING as date,
            metadata.key_topics::ARRAY<STRING> as key_topics,
            summary,
            summary_pig_latin
        FROM enriched
    """))
