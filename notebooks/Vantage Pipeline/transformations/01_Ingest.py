from pyspark import pipelines as dp
from pyspark.sql.functions import regexp_extract

# ============================================================
# STAGE 1: INGEST -- Raw documents from SharePoint
# Replaces: Lambda + Graph API + S3 staging + SQS trigger
# ============================================================
@dp.table(
    comment="Raw binary documents ingested from SharePoint via Auto Loader"
)
@dp.expect_or_drop("has_content", "content IS NOT NULL")
@dp.expect_or_drop("valid_size", "length > 0 AND length < 104857600")  # 100MB max
def p01_bronze_raw_documents():

    connector_name = spark.conf.get("sharepoint.connector", "sharepoint_conn")
    site           = spark.conf.get("sharepoint.site", "sharepoint_site")

    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("databricks.connection", connector_name)
        # UC requires schema location outside the managed table directory
        .option("cloudFiles.schemaLocation", "/Volumes/dennis_schultz/dennis_schultz/my_data/autoloader_schema/")
        .option("pathGlobFilter", "*.{pdf,pptx,docx}")
        .load(site)
        .withColumn("fileName", regexp_extract("path", r"([^/]+)$", 1))
        .select("fileName", "path", "content", "modificationTime", "length"))
