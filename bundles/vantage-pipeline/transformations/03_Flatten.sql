-- ============================================================
-- STAGE 3: FLATTEN -- Extract and structure text content
-- ============================================================

CREATE OR REFRESH MATERIALIZED VIEW p03_gold_document_text
COMMENT "Flattened document text ready for enrichment"
AS
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
