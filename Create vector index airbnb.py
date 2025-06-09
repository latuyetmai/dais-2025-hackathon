# Databricks notebook source
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install  langchain
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC

# COMMAND ----------

# Restart Python after installing
dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient


# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
import os
from pyspark.sql.functions import substring_index

# COMMAND ----------

from langchain.text_splitter import RecursiveCharacterTextSplitter
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd

# Load data
df = spark.sql("""
    SELECT id, property_id, full_text
    FROM workspace.acc.airbnb
    WHERE full_text IS NOT NULL
""").toPandas()

# Split
splitter = RecursiveCharacterTextSplitter(
    separators=["\n\n", "\n", " ", ""],
    chunk_size=1000,
    chunk_overlap=200,
    length_function=len,
)

chunk_records = []
for _, row in df.iterrows():
    chunks = splitter.split_text(row["full_text"])
    for chunk in chunks:
        chunk_records.append({
            "id": row["id"],
            "property_id": row["property_id"],
            "text": chunk,
        })

# Convert to Spark DataFrame
chunk_df = spark.createDataFrame(pd.DataFrame(chunk_records)) \
                .withColumn("chunk_id", monotonically_increasing_id())

# Save to Delta table
chunk_df.write.mode("overwrite").saveAsTable("workspace.acc.airbnb_chunks")


# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.acc.airbnb_chunks
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient(disable_notice=True)

# vsc = VectorSearchClient()

vsc.create_delta_sync_index(
    endpoint_name="dais",  # must exist
    index_name="workspace.acc.airbnb_vindex",
    source_table_name="workspace.acc.airbnb_chunks",
    pipeline_type="TRIGGERED",
    primary_key="chunk_id",
    embedding_source_column="text",
    embedding_model_endpoint_name="databricks-gte-large-en",
    # metadata_columns=["property_id", "id"]
)


# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

index = vsc.get_index(
    endpoint_name="dais",
    index_name="workspace.acc.airbnb_vindex"
)

retriever = index.as_retriever(
    search_kwargs={
        "k": 5,
        "query_type": "hybrid",
        "filter": {"property_id": ["46727535", "13080625"]}
    }
)

results = retriever.search("wheelchair accessible home near Disney")

for r in results:
    print(r["text"], r["property_id"])
