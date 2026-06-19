from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# ============================================================
# STAGE 2: PARSE -- Extract text, images, tables from documents
# Replaces: Parsing Service + Conversion Service + Step Functions
# NOTE: pipeline_parsed_documents (MV) is now replaced by
#       pipeline_parsed_documents_v2 (ST). Drop the old table manually.
# ============================================================
@dp.table(
    comment="Parsed binary documents ingested from SharePoint"
)
def p02_silver_parsed_documents():

    catalog = spark.conf.get("catalog", "dennis_schultz")
    schema  = spark.conf.get("schema", "default")
    volume  = spark.conf.get("volume", "my_volume")  # update to match your UC volume name

    return (spark.readStream.table("p01_bronze_raw_documents")
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
            "fileName",
            "path",
            "modificationTime",
            expr("parsed:document:elements").alias("elements"),
            expr("parsed:metadata").alias("doc_metadata")
        ))
