# Databricks notebook source
# MAGIC %md
# MAGIC ## Set up

# COMMAND ----------

# %pip install -U -qqq langchain_core databricks_langchain langchain_community databricks-sql-connector databricks-sqlalchemy databricks-vectorsearch langchain-community
# %restart_python

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `dais-hackathon-2025`.bright_initiative.airbnb_properties_information
# MAGIC WHERE
# MAGIC   -- location = 'Chicago, Illinois, United States'
# MAGIC   LOWER(location) LIKE '%new york%'
# MAGIC   AND host_number_of_reviews > 1000
# MAGIC   AND EXISTS(reviews, review -> review ILIKE '%wheelchair%')
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH t0 AS (
# MAGIC   SELECT property_id
# MAGIC   FROM `dais-hackathon-2025`.bright_initiative.airbnb_properties_information
# MAGIC   GROUP BY property_id
# MAGIC   HAVING COUNT(reviews) > 1 
# MAGIC )
# MAGIC SELECT 
# MAGIC t0.property_id
# MAGIC , t1.*
# MAGIC FROM workspace.acc.airbnb t1
# MAGIC INNER JOIN t0 ON t1.property_id = t0.property_id
# MAGIC ORDER BY t0.property_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH raw AS (
# MAGIC SELECT 
# MAGIC   *
# MAGIC   , row_number() OVER (PARTITION BY property_id ORDER BY url DESC) AS instance
# MAGIC FROM `dais-hackathon-2025`.bright_initiative.airbnb_properties_information 
# MAGIC -- FROM workspace.acc.airbnb
# MAGIC WHERE property_id IS NOT NULL
# MAGIC -- AND EXISTS(reviws, review -> review ILIKE '%wheelchair%')
# MAGIC -- AND lower(location) LIKE '%new york%'
# MAGIC )
# MAGIC SELECT 
# MAGIC  property_id
# MAGIC  , concat_ws('\n', reviews) AS reviews
# MAGIC  , concat_ws('\n', CAST(amenities AS string)) AS amenities
# MAGIC  , concat_ws('\n', CAST(arrangement_details AS STRING)) AS arrangement_details 
# MAGIC  , description AS liting_description
# MAGIC  , concat_ws('\n', available_dates) AS available_dates
# MAGIC  , breadcrumbs
# MAGIC  , concat_ws('\n', cast(cancellation_policy as string)) AS cancellation_policy
# MAGIC  , category
# MAGIC  , concat_ws('\n', cast(category_rating as string)) AS category_ratings
# MAGIC  , currency
# MAGIC  , concat_ws('\n', description_items) AS description_iems
# MAGIC  , concat_ws('\n', details) AS details
# MAGIC  , discount
# MAGIC  , final_url
# MAGIC  , guests
# MAGIC  , concat_ws('\n', cast(highlights as string)) AS highlights
# MAGIC  , host_number_of_reviews
# MAGIC  , host_rating
# MAGIC  , host_response_rate
# MAGIC  , hosts_year
# MAGIC  , concat_ws('\n', house_rules) AS house_rules
# MAGIC  , is_guest_favorite
# MAGIC  ,is_supperhost
# MAGIC  , lat
# MAGIC  , listing_name
# MAGIC  , listing_title
# MAGIC  , location
# MAGIC  , long
# MAGIC  , name
# MAGIC  , pets_allowed
# MAGIC  , price
# MAGIC  , concat_ws('\n', cast(pricing_details as string)) AS pricing_details
# MAGIC  , property_number_of_reviews AS number_of_reviews
# MAGIC  , ratings
# MAGIC  , total_price
# MAGIC   -- ,*
# MAGIC FROM raw
# MAGIC WHERE instance = 1
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA sql;
# MAGIC
# MAGIC DROP TABLE IF EXISTS airbnb_property; 
# MAGIC CREATE OR REPLACE TABLE airbnb_property
# MAGIC AS
# MAGIC WITH raw AS (
# MAGIC SELECT 
# MAGIC   *
# MAGIC   , row_number() OVER (PARTITION BY property_id ORDER BY url DESC) AS instance
# MAGIC FROM `dais-hackathon-2025`.bright_initiative.airbnb_properties_information 
# MAGIC -- FROM workspace.acc.airbnb
# MAGIC WHERE property_id IS NOT NULL
# MAGIC -- AND EXISTS(reviws, review -> review ILIKE '%wheelchair%')
# MAGIC -- AND lower(location) LIKE '%new york%'
# MAGIC )
# MAGIC SELECT 
# MAGIC  property_id
# MAGIC  , concat_ws('\n', reviews) AS reviews
# MAGIC  , concat_ws('\n', CAST(amenities AS string)) AS amenities
# MAGIC  , concat_ws('\n', CAST(arrangement_details AS STRING)) AS arrangement_details 
# MAGIC  , description AS liting_description
# MAGIC  , concat_ws('\n', available_dates) AS available_dates
# MAGIC  , breadcrumbs
# MAGIC  , concat_ws('\n', cast(cancellation_policy as string)) AS cancellation_policy
# MAGIC  , category
# MAGIC  , concat_ws('\n', cast(category_rating as string)) AS category_ratings
# MAGIC  , currency
# MAGIC  , concat_ws('\n', description_items) AS description_iems
# MAGIC  , concat_ws('\n', details) AS details
# MAGIC  , discount
# MAGIC  , final_url
# MAGIC  , guests
# MAGIC  , concat_ws('\n', cast(highlights as string)) AS highlights
# MAGIC  , host_number_of_reviews
# MAGIC  , host_rating
# MAGIC  , host_response_rate
# MAGIC  , hosts_year
# MAGIC  , concat_ws('\n', house_rules) AS house_rules
# MAGIC  , is_guest_favorite
# MAGIC  ,is_supperhost
# MAGIC  , lat
# MAGIC  , listing_name
# MAGIC  , listing_title
# MAGIC  , location
# MAGIC  , long
# MAGIC  , name
# MAGIC  , pets_allowed
# MAGIC  , price
# MAGIC  , concat_ws('\n', cast(pricing_details as string)) AS pricing_details
# MAGIC  , property_number_of_reviews AS number_of_reviews
# MAGIC  , ratings
# MAGIC  , total_price
# MAGIC FROM raw
# MAGIC WHERE instance = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Tool

# COMMAND ----------

import ast
import re

from langchain.chains import create_sql_query_chain
from langchain_community.tools import QuerySQLDataBaseTool  ## Updated
from langchain_community.utilities import SQLDatabase
# from databricks.vector_search.client import VectorSearchClient
# from langchain_community.vectorstores import DatabricksVectorSearch
from databricks_langchain import DatabricksVectorSearch  ## Updated
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableParallel, RunnablePassthrough

# COMMAND ----------

from databricks_langchain import ChatDatabricks
from databricks.sdk import WorkspaceClient
import os
import mlflow
mlflow.langchain.autolog(silent=True)

w = WorkspaceClient()

os.environ["DATABRICKS_HOST"] = w.config.host
os.environ["DATABRICKS_TOKEN"] = w.tokens.create(comment="for model serving", lifetime_seconds=1200).token_value
os.environ["DATABRICKS_HOSTNAME"] = "dbc-581b2c6f-8577.cloud.databricks.com"
# os.environ["DATABRICKS_HOST"] = "https://dbc-eebd97f8-a4cd.cloud.databricks.com"
print(os.environ["DATABRICKS_HOST"])

# COMMAND ----------

## Get table info
db = SQLDatabase.from_databricks(
    catalog="workspace",
    schema="sql",
    engine_args={"pool_pre_ping": True},
    host=os.environ.get("DATABRICKS_HOSTNAME"),
    api_token=os.environ.get("DATABRICKS_TOKEN"),
    warehouse_id="e6d1b4d027dab190",  # sql-warehouse
)

table_info = db.get_table_info({"airbnb_property"})
print(table_info)

def get_table_info(input):
    return db.get_table_info({"airbnb_property"})

def query_database(query):
    try:
        res = db.run(query)
    except Exception as e:
        return "Could not execute query: {}".format(e)
    res = [el for sub in ast.literal_eval(res) for el in sub if el]
    res = [re.sub(r"\b\d+\b", "", string).strip() for string in res]
    return ", ".join(res)


# COMMAND ----------

def query_sql_airbnb(input):
    
    system = """You are a SQL expert and acting as a preliminary filter to table workspace.acc.airbnb_property\
    given an input question return only relevant property_id values which could relate to the question.\
    Create a syntactically correct SQL query to run with no mark up or explanation. \
    Unless otherwise specified, do not return more than 50 rows or {top_k} if requested.

    Your main task is to filter and only return the property_id column. Do not perform any aggregation \
    (like COUNT, DISTINCT COUNT, SUM, etc.) or sorting and merging with other tables, just use SELECT property_id \
    FROM workspace.acc.airbnb_property WHERE... If the input question ask you to perform other tasks rather than just filtering \
    the data, you can ignore all of these other tasks, and just return the filtered ProjectID values, DO NOT RETURN \
    any other columns in your SQL query. DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.
    Only use the following tables info: {table_info}

    When searching for disabilities or wheelchair accessible in review, try filter the data with column `reviews` and search by matching '%wheelchair%' or '%wheel chair%' such as (EXISTS(reviews, review -> review ILIKE '%wheelchair%')

    Below are some example questions and their expected SQL queries output. Only return the SQL query with no markup or explanation.

    User input: find listing with ratings more than 4 in New York for people with disabilities or wheelchair accessible
    SQL query: SELECT property_id FROM workspace.acc.airbnb_property \
    WHERE rating >= 4 \
    AND (EXISTS(reviews, review -> review ILIKE '%wheelchair%') OR EXISTS(reviews, review -> review ILIKE '%wheel chair%'))
    AND (location LIKE '%new york%' OR location LIKE '%new york%')

    User input: {input}
    SQL query:
    """

    ## Get table info
    db = SQLDatabase.from_databricks(
        catalog="workspace",
        schema="sql",
        engine_args={"pool_pre_ping": True},
        host=os.environ.get("DATABRICKS_HOSTNAME"),
        api_token=os.environ.get("DATABRICKS_TOKEN"),
        warehouse_id="e6d1b4d027dab190",  # sql-warehouse
    )

    def get_table_info(input):
        return db.get_table_info({"airbnb_property"})

    def query_database(query):
        try:
            res = db.run(query)
        except Exception as e:
            return "Could not execute query: {}".format(e)
        res = [el for sub in ast.literal_eval(res) for el in sub if el]
        res = [re.sub(r"\b\d+\b", "", string).strip() for string in res]
        return ", ".join(res)

    sql_prompt = ChatPromptTemplate.from_messages(
                [("system", system), ("human", "{input}")]
            )

    llm = ChatDatabricks(
        endpoint="databricks-meta-llama-3-3-70b-instruct", 
        temperature=0,
        target_uri="databricks",
    )

    write_query = create_sql_query_chain(llm, db, prompt=sql_prompt)
    execute_query = QuerySQLDataBaseTool(db=db)
    # execute_query = QuerySQLDataBaseTool(db=db)


    custom_chain = (
                {
                    "input": RunnablePassthrough()
                }
                | RunnablePassthrough().assign(filter_sql_id=(
                RunnableParallel(
                    {
                        "question": RunnablePassthrough() | (lambda x: x["input"]),
                        "table_info": get_table_info,
                    }
                )
                | write_query
                | execute_query
            )
        )
    )

    return custom_chain.invoke(input)


test_query = "find listing in New York for people with disabilities"
query_sql_airbnb(test_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.acc.airbnb_property
# MAGIC LIMIT 10