# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC
# MAGIC ### Elasticsearch
# MAGIC
# MAGIC <img src="https://static-www.elastic.co/v3/assets/bltefdd0b53724fa2ce/blt5ebe80fb665aef6b/5ea8c8f26b62d4563b6ecec2/brand-elasticsearch-220x130.svg" width="300">
# MAGIC
# MAGIC This notebook has been adapted to use the Python library rather than installing from Maven.  This is simpler but will suffer at scale because it is not distributed.
# MAGIC
# MAGIC Instructions for the Spark driver:
# MAGIC - [Databricks docs](https://docs.databricks.com/aws/en/archive/connectors/elasticsearch)
# MAGIC - [Driver Repo]()
# MAGIC
# MAGIC 1. Launch a cluster in your workspace or choose an existing cluster.
# MAGIC 2. Once the new cluster is running, go to the "Libraries" tab of that cluster, and click "Install new" -> choose "Maven" -> enter the maven coordinates `org.elasticsearch:elasticsearch-spark-30_2.13:8.4.3` -> click "Install". If running into errors like `org.elasticsearch.hadoop.EsHadoopIllegalArgumentException: Cannot detect ES version` while the ES connection is verified, consider install newer versions that matches your ES service.
# MAGIC 3. Once the installation has finished, attach this notebook to the cluster, and run write and/or read operations against your Elasticsearch cluster
# MAGIC
# MAGIC I ran into multiple issues trying to get the right version of the driver.
# MAGIC
# MAGIC Instructions for the Python driver:
# MAGIC 1. Just pip install into a Serverless cluster.  The library used must correspond to the version of Elasticsearch used.
# MAGIC - `elasticsearch` for version 9.x and above
# MAGIC - `elasticsearch8` for version 8.x

# COMMAND ----------

# DBTITLE 1,Install Elasticsearch Python client
# MAGIC %pip install elasticsearch8 --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secrets
# MAGIC Store sensitive information in secrets.  From the command line:
# MAGIC
# MAGIC `databricks secrets create-scope elasticsearch`
# MAGIC
# MAGIC `databricks secrets put-secret elasticsearch serverless_api_key`
# MAGIC
# MAGIC `databricks secrets put-secret elasticsearch username`
# MAGIC
# MAGIC `databricks secrets put-secret elasticsearch password`

# COMMAND ----------

# DBTITLE 1,Variables
dbutils.widgets.text("elasticsearch_hostname", "", "Elasticsearch hostname")
hostname = dbutils.widgets.get("elasticsearch_hostname")
# hostname = "2f29fc8008974e30a2d548cf8745b342.eu-central-1.aws.cloud.es.io" # Vantage

port = "443"
ssl = "true"
index = "people"

# Read secrets
api_key = dbutils.secrets.get("elasticsearch", "api_key")

# COMMAND ----------

# DBTITLE 1,Make trivial test dataframe
people = spark.createDataFrame( [ ("Bilbo",     50), 
                                  ("Gandalf", 1000), 
                                  ("Thorin",   195),  
                                  ("Balin",    178), 
                                  ("Kili",      77),
                                  ("Dwalin",   169), 
                                  ("Oin",      167), 
                                  ("Gloin",    158), 
                                  ("Fili",      82), 
                                  ("Bombur",  None)
                                ], 
                                ["name", "age"] 
                              )

# COMMAND ----------

# DBTITLE 1,Prep write data
# Convert Spark DataFrame to list of dicts and bulk index
import json
rows = json.loads(people.toPandas().to_json(orient="records"))
actions = [{"_index": index, "_source": row} for row in rows]

# COMMAND ----------

# DBTITLE 1,Test connectivity to Elasticsearch serverless
import subprocess
result = subprocess.run(["nc", "-vz", hostname, port], capture_output=True, text=True)
print(result.stdout)
print(result.stderr)

# COMMAND ----------

# DBTITLE 1,Write to Elasticsearch
from elasticsearch8 import Elasticsearch
from elasticsearch8.helpers import bulk

client = Elasticsearch(
        f"https://{hostname}:{port}",
        api_key=api_key
)

# Delete existing index for overwrite behavior
if client.indices.exists(index=index):
    client.indices.delete(index=index)


success, errors = bulk(client, actions)
print(f"Successfully indexed {success} documents to '{index}'")
if errors:
    print(f"Errors: {errors}")

# COMMAND ----------

# DBTITLE 1,Read from Elasticsearch
# Read all documents from the index using the Python client
result = client.search(index=index, query={"match_all": {}}, size=1000)
hits = [hit["_source"] for hit in result["hits"]["hits"]]

# Convert to Spark DataFrame
df = spark.createDataFrame(hits)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save in Delta 

# COMMAND ----------

# DBTITLE 1,Write to Delta
# Creates a Delta table called table_name
df.write.format("delta").saveAsTable(table_name)