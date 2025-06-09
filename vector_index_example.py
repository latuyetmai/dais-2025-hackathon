# Databricks notebook source
from databricks_langchain.vectorstores import DatabricksVectorStore
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.tools.retriever import VectorStoreRetrieverTool
from langchain.agents import initialize_agent
from langchain.chat_models import ChatOpenAI

# 1. Initialize the embedding model
embedding_model = OpenAIEmbeddings()

# 2. Connect to the Databricks vector index
vector_store = DatabricksVectorStore(
    index_name="catalog.schema.index_name",  # Unity Catalog path
    embedding=embedding_model,
    text_column="content",                  # Main document content
    metadata_column="metadata_json",        # Column storing title, url, etc.
    embedding_column="embedding"            # Vector column
)

# 3. Convert to retriever
retriever = vector_store.as_retriever(search_kwargs={"k": 5})

# 4. Wrap retriever in a LangChain tool
retriever_tool = VectorStoreRetrieverTool(
    name="document_lookup",
    description="Search documents with full context and metadata.",
    retriever=retriever
)

# 5. Set up the LLM
llm = ChatOpenAI(temperature=0)

# 6. Create an agent with the retriever tool
agent = initialize_agent(
    tools=[retriever_tool],
    llm=llm,
    agent="chat-conversational-react-description",
    verbose=True
)

# 7. Run a query
query = "What are the key points in the cloud architecture document?"
response = agent.run(query)

# 8. Output the response
print(response)