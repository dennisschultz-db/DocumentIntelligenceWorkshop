# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Elasticsearch
# MAGIC
# MAGIC <img src="https://static-www.elastic.co/v3/assets/bltefdd0b53724fa2ce/blt5ebe80fb665aef6b/5ea8c8f26b62d4563b6ecec2/brand-elasticsearch-220x130.svg" width="300">
# MAGIC
# MAGIC 1. Launch a cluster in your workspace or choose an existing cluster.
# MAGIC 2. Once the new cluster is running, go to the "Libraries" tab of that cluster, and click "Install new" -> choose "Maven" -> enter the maven coordinates.  The library used must correspond to the Spark version and the Scala version of the DBR being used, and the Elasticsearch runtime version.  Not all combinations are hosted by Maven Central
# MAGIC - The Databricks documentation specifies `org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3` 
# MAGIC - There appear to be no hosted versions for Spark 4.x
# MAGIC - DBR `16.4 LTS (includes Apache Spark 3.5.2, Scala 2.13)` seems to be the newest version of the DBR that can be supported by versions on Maven Central.
# MAGIC - The highest version of ES is `9.3.2`, which appears to work with ES `9.3.5`.
# MAGIC - Using `org.elasticsearch:elasticsearch-spark-30_2.13:9.3.2`
# MAGIC
# MAGIC 3. click "Install". If running into errors like `org.elasticsearch.hadoop.EsHadoopIllegalArgumentException: Cannot detect ES version` while the ES connection is verified, consider install newer versions that matches your ES service.
# MAGIC 4. Once the installation has finished, attach this notebook to the cluster, and run write and/or read operations against your Elasticsearch cluster

# COMMAND ----------

# MAGIC %md
# MAGIC **Important**: In the following cells, replace `<hosted_hostname>`, `<port>`, `<ssl>`, `<hostname>` and `<index>` with your Elasticsearch configuration

# COMMAND ----------

# DBTITLE 1,Variables
hosted_hostname = "mytestes-0b283a.es.us-east-1.aws.found.io" # managed
port = "443"
ssl = "true"
index = "people"

# COMMAND ----------

# DBTITLE 1,Test connectivity to your Elasticsearch cluster
# MAGIC %sh 
# MAGIC nc -vz hosted_hostname port

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

# DBTITLE 1,Write to Elasticsearch
# Overwrite the data each time

# NOTE: We **must** set the es.nodes.wan.only property to 'true' so that the connector will connect to the node(s) specified by the `es.nodes` parameter.
#       Without this setting, the ES connector will try to discover ES nodes on the network using a broadcast ping, which won't work.
#       We want to connect to the node(s) specified in `es.nodes`.
( people.write
  .format( "org.elasticsearch.spark.sql" )
  .option( "es.nodes",   hosted_hostname )
  .option( "es.port",    port     )
  .option( "es.net.ssl", ssl      )
  .option( "es.nodes.wan.only", "true" )
  .option( "es.net.http.auth.user", dbutils.secrets.get("elasticsearch", "username") )
  .option( "es.net.http.auth.pass", dbutils.secrets.get("elasticsearch", "password") )
  # .option("es.net.http.header.Authorization": f"ApiKey <es_api_key>",
  .mode( "overwrite" )
  .save( f"{index}" )
)

# COMMAND ----------

# DBTITLE 1,Read from Elasticsearch
# NOTE: We **must** set the es.nodes.wan.only property to 'true' so that the connector will connect to the node(s) specified by the `es.nodes` parameter.
#       Without this setting, the ES connector will try to discover ES nodes on the network using a broadcast ping, which won't work.
#       We want to connect to the node(s) specified in `es.nodes`.
df = (spark.read
      .format( "org.elasticsearch.spark.sql" )
      .option( "es.nodes",   hosted_hostname )
      .option( "es.port",    port     )
      .option( "es.net.ssl", ssl      )
      .option( "es.nodes.wan.only", "true" )
      .option( "es.net.http.auth.user", dbutils.secrets.get("elasticsearch", "username") )
      .option( "es.net.http.auth.pass", dbutils.secrets.get("elasticsearch", "password") )
      .load( f"{index}" )
     )

display(df)

# COMMAND ----------

# DBTITLE 1,Write another row
more_people = spark.createDataFrame( [ ("Aragorn",    87)
                                ], 
                                ["name", "age"] 
                              )

( more_people.write
  .format( "org.elasticsearch.spark.sql" )
  .option( "es.nodes",   hosted_hostname )
  .option( "es.port",    port     )
  .option( "es.net.ssl", ssl      )
  .option( "es.nodes.wan.only", "true" )
  .option( "es.net.http.auth.user", dbutils.secrets.get("elasticsearch", "username") )
  .option( "es.net.http.auth.pass", dbutils.secrets.get("elasticsearch", "password") )
  .mode( "append" )
  .save( f"{index}" )
)

# COMMAND ----------

# DBTITLE 1,Write to Delta
# Creates a Delta table called table_name
df.write.format("delta").saveAsTable(table_name)