from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# ============================================================
# STAGE 2: PARSE -- Extract text, images, tables from documents
# Replaces: Parsing Service + Conversion Service + Step Functions
# ============================================================
@dp.materialized_view(
    comment="Parsed document content with extracted elements"
)
def pipeline_parsed_documents():

    catalog = spark.conf.get("catalog", "dennis_schultz")
    schema  = spark.conf.get("schema", "default")
    volume  = spark.conf.get("volume", "my_volume")

    return (spark.read.table("pipeline_raw_documents")
        .withColumn(
            "parsed",
            expr(f"""
                ai_parse_document(
                    content,
                    MAP(
                        'version', '2.0',
                        'imageOutputPath', '/Volumes/{catalog}/{schema}/{volume}/slide_images/'
                    )
                )
        """))
        .select(
            "path",
            "modificationTime",
            expr("parsed:document:elements").alias("elements"),
            expr("parsed:metadata").alias("doc_metadata")
        ))
