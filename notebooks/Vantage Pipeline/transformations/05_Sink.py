from pyspark import pipelines as dp

# ============================================================
# STAGE 5: SINK -- Write to Elasticsearch via elasticsearch-py
# Replaces: Custom ES writer service
# Requires: environment.dependencies = ["elasticsearch"] in pipeline settings
# ============================================================
# Resolve secret at module level and store in Spark conf so the
# foreach_batch handler can access it without referencing dbutils
# (dbutils cannot be serialized by cloudpickle).
spark.conf.set("_pipeline.es_api_key", dbutils.secrets.get("elasticsearch", "serverless_api_key"))

@dp.foreach_batch_sink(name="elasticsearch_sink")
def write_to_elasticsearch(df, batch_id):
    from elasticsearch import Elasticsearch, helpers
    import json

    ES_HOST  = spark.conf.get("elasticsearch_host", "https://YOUR-ES-ENDPOINT.com")
    ES_INDEX = spark.conf.get("elasticsearch_index", "documents")
    api_key  = spark.conf.get("_pipeline.es_api_key")

    # Select only columns available in p03_gold_enriched_documents
    df_clean = df.selectExpr(
        "fileName",
        "category",
        "error_message",
        "title",
        "company",
        "product",
        "author",
        "date",
        "CAST(key_topics AS STRING) AS key_topics",
        "CAST(summary AS STRING) AS summary",
        "summary_pig_latin"
    )

    es   = Elasticsearch(ES_HOST, api_key=api_key)
    docs = []
    for row in df_clean.collect():
        source = row.asDict(recursive=True)
        for field in ("summary", "key_topics"):
            if source.get(field):
                try:
                    source[field] = json.loads(source[field])
                except (ValueError, TypeError):
                    pass
        docs.append({"_index": ES_INDEX, "_id": source["fileName"], "_source": source})
    if docs:
        helpers.bulk(es, docs)


@dp.append_flow(target="elasticsearch_sink")
def es_output_flow():
    return spark.readStream.table("p03_gold_enriched_documents")
