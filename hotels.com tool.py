# Databricks notebook source
# MAGIC %pip install -U llama-index llama-index-llms-databricks mlflow databricks_langchain databricks_langchain
# MAGIC %restart_python

# COMMAND ----------

from databricks_langchain import ChatDatabricks
from databricks.sdk import WorkspaceClient
import os

w = WorkspaceClient()

os.environ["DATABRICKS_HOST"] = w.config.host
os.environ["DATABRICKS_TOKEN"] = w.tokens.create(comment="for model serving", lifetime_seconds=1200).token_value

llm = ChatDatabricks(endpoint="databricks-llama-4-maverick")

# COMMAND ----------

llm.complete("Hello, world")

# COMMAND ----------

# MAGIC %pip install langchain_databricks
# MAGIC %restart_python

# COMMAND ----------

import pandas as pd
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_databricks import ChatDatabricks
from databricks.sdk import WorkspaceClient
import os

# configure workspace tokens
w = WorkspaceClient()
os.environ["DATABRICKS_HOST"] = w.config.host
os.environ["DATABRICKS_TOKEN"] = w.tokens.create(comment="for model serving", lifetime_seconds=1200).token_value

llm = ChatDatabricks(endpoint="databricks-llama-4-maverick")

def format_context(df: pd.DataFrame) -> str:
    """
    Converts the DataFrame into a JSON string to ensure all data is passed
    to the model without truncation. JSON is also a great format for structured data
    like you have in 'description_by_sections'.
    """
    return df.to_json(orient='records', indent=2)

def find_accessible_airbnb_properties(location: str) -> pd.DataFrame:
  """
  Queries the Bright Initiative Airbnb dataset for properties in a specific location
  that have reviews mentioning "wheelchair".
  """
  query = f"""
    SELECT
      listing_name,
      location_details,
      location,
      details,
      description_by_sections,
      reviews
    FROM `dais-hackathon-2025`.bright_initiative.airbnb_properties_information
    WHERE
      location ILIKE '%{location}%'
      AND host_number_of_reviews > 1000
      AND EXISTS(reviews, review -> review ILIKE '%wheelchair%')
    LIMIT 5
  """
  return format_context(spark.sql(query).toPandas())

# Define the prompt template for the LLM
prompt_template = PromptTemplate.from_template(
  """
  You are a helpful assistant for accessible travel. Your goal is to summarize potential Airbnb listings for a user.

  The following listing *mention* wheelchairs but may not actually be accessible. Closely review the descriptions and review,
  and then summarize the accessibility features (or lack thereof).

  Here is the JSON data:
  {context}
  """
)

llm = ChatDatabricks(endpoint="databricks-llama-4-maverick")

# This is our simple "agentic" chain
chain = (
    find_accessible_airbnb_properties
    | prompt_template
    | llm
    | StrOutputParser()
)

# Let's run the chain for Chicago!
result = chain.invoke("Chicago")

print(result)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION workspace.acc.hotel_acc(
# MAGIC     city STRING COMMENT 'Filter by city. Example: "New York"' DEFAULT "New York"
# MAGIC   )
# MAGIC returns table
# MAGIC comment 'Returns accessibility data from hotels.com for a given city. It only containts data about hotels.'
# MAGIC return(
# MAGIC   SELECT
# MAGIC     description,
# MAGIC     fine_print,
# MAGIC     property_highlights,
# MAGIC     top_reviews,
# MAGIC     house_rules,
# MAGIC     review_score
# MAGIC     hotel_id
# MAGIC     FROM `dais-hackathon-2025`.bright_initiative.booking_hotel_listings
# MAGIC     WHERE
# MAGIC       -- location ILIKE '%New York%'
# MAGIC       1=1
# MAGIC   and (
# MAGIC        description ilike '%wheelchair%'
# MAGIC        or fine_print ilike '%wheelchair%'
# MAGIC        or property_highlights ilike '%wheelchair'
# MAGIC        or concat_ws(', ', transform(top_reviews, x -> x.review)) ilike '%wheelchair%'
# MAGIC        or concat_ws(', ', transform(house_rules, x -> x.description))  ilike '%wheelchair%'
# MAGIC        -- or top_reviews[*].review ilike '%wheelchair%'
# MAGIC     )
# MAGIC   and (
# MAGIC    isnull(hotel_acc.city)
# MAGIC         OR hotel_acc.city = ''
# MAGIC         OR hotel_acc.city = 'NULL'
# MAGIC         OR city ILIKE CONCAT('%', hotel_acc.city, '%')
# MAGIC   )
# MAGIC   order by review_score desc
# MAGIC )

# COMMAND ----------

import pandas as pd
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_databricks import ChatDatabricks
from databricks.sdk import WorkspaceClient
import os

# configure workspace tokens
w = WorkspaceClient()
os.environ["DATABRICKS_HOST"] = w.config.host
os.environ["DATABRICKS_TOKEN"] = w.tokens.create(comment="for model serving", lifetime_seconds=1200).token_value

llm = ChatDatabricks(endpoint="databricks-llama-4-maverick")

# COMMAND ----------

import pandas as pd
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_databricks import ChatDatabricks
from databricks.sdk import WorkspaceClient
import os

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as f

def find_accessible_hotel_properties(input: tuple[str, str]) -> pd.DataFrame:
    city = input[0]
    country = input[1]
    #country = "US"
    cols_to_check = [
        "description",
        "fine_print",
        #"popular_facilities",
        "property_highlights",
        #"top_reviews2",
        #"house_rules2",
    ]
    or_statement = f.lit(False)
    for col in cols_to_check:
        or_statement = or_statement | (f.lower(col).contains("wheelchair"))
    for col in ['top_reviews', 'house_rules']:
        or_statement |= f.lower(f.to_json(col)).contains('wheelchair')
    #for col in ["popular_facilities"]:
    # or_statement = or_statement | (f.lower(f.array_join(f.col(review), ", ")).contains("assessable"))
    query = f"""
        SELECT
          description,
          fine_print,
          property_highlights,
          top_reviews,
          house_rules,
          hotel_id
        FROM `dais-hackathon-2025`.bright_initiative.booking_hotel_listings
        WHERE
         location ILIKE '%{city}%'
         -- AND country ILIKE '%{country}%'
    """
    df = spark.sql(query)
    #df = df.withColumn('foobar', f.transform("top_reviews", lambda x: f.get_json_object(x, "$.review")))
    #df = df.withColumn('foobar', f.to_json("top_reviews"))
    print(query)
    return format_context(df.where(or_statement).limit(10).toPandas())

def format_context(df: pd.DataFrame) -> str:
    """
    Converts the DataFrame into a JSON string to ensure all data is passed
    to the model without truncation. JSON is also a great format for structured data
    like you have in 'description_by_sections'.
    """
    return df.to_json(orient='records', indent=2)

display(find_accessible_hotel_properties(('Chicago', 'US')))

# COMMAND ----------

prompt_template = PromptTemplate.from_template(
  """
  You are a helpful assistant for accessible travel. Your goal is to summarize potential hotel listings for a user.

  The following listing *mention* wheelchairs but may not actually be accessible. Closely review the descriptions and review. Then select the most
  assessible hotel and return the json just for that hotel.
  
  Here is the JSON data:
  {context}
  """
)

llm = ChatDatabricks(endpoint="databricks-llama-4-maverick")

# This is our simple "agentic" chain
chain = (
    find_accessible_hotel_properties
    | prompt_template
    | llm
    | StrOutputParser()
)

# Let's run the chain for Chicago!
result = chain.invoke(("Chicago", "US"))

print(result)

# COMMAND ----------

%pip install -U -qqqq databricks-langchain
%pip install -U -qqqq databricks-openai
%pip install -U -qqqq databricks-ai-bridge
%pip install -U -qqqq databricks-langchain langgraph langchain mlflow 
%restart_python

# COMMAND ----------

from langgraph.prebuilt import create_react_agent
from langchain_community.tools import DuckDuckGoSearchRun
import mlflow
from databricks_langchain import ChatDatabricks

llm = ChatDatabricks(endpoint="databricks-meta-llama-3-3-70b-instruct")
mlflow.langchain.autolog()
search = DuckDuckGoSearchRun()

agent = create_react_agent(
    model=llm,  
    tools=[search],  
    prompt="You are a helpful assistant that can search the web. Answer questions for the user based on web searches."  
)

# Run the agent
agent.invoke(
    {"messages": [{"role": "user", "content": "What are accessible attractions in Chicago?"}]}
)
     


# COMMAND ----------

