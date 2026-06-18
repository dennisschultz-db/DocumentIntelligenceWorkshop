from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# ============================================================
# STAGE 5: SINK -- Write to Elasticsearch
# Replaces: Custom ES writer service
# ============================================================
ES_HOST  = spark.conf.get("elasticsearch_host", "https://YOUR-ES-ENDPOINT.com")
ES_INDEX = spark.conf.get("elasticsearch_index", "documents")
serverless_api_key = dbutils.secrets.get("elasticsearch", "serverless_api_key")

@dp.foreach_batch_sink(name="elasticsearch_sink")
def write_to_elasticsearch(df, batch_id):
    (df.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", ES_HOST)
        .option("es.port", "443")
        .option("es.net.ssl", "true")
        .option("es.net.http.header.Authorization", f"ApiKey {serverless_api_key}")
        .option("es.nodes.wan.only", "true")
        .option("es.index.auto.create", "true")
        .option("es.mapping.id", "doc_id")
        .mode("append")
        .save(ES_INDEX))

@dp.append_flow(target="elasticsearch_sink")
def es_output_flow():
    return spark.readStream.option("skipChangeCommits", "true").table("pipeline_enriched_documents")
