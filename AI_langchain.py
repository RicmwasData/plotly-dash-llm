import getpass
import os
import pandas as pd
import ast

#big query
from google.cloud import bigquery
#client = bigquery.Client()

#langchain 
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI


# Provide the name of the project and dataset
dataset_name = 'bigquery-public-data.iowa_liquor_sales'
# Access the INFORMATION_SCHEMA for the dataset
#dataset = client.get_dataset(dataset_name)  
# Give a description of the data set
#print("Description: ", dataset.description)


# To get the schema details for a specific table, provide the complete path
full_table_path = 'bigquery-public-data.iowa_liquor_sales.sales'
# Access the table details
#table_detail = client.get_table(full_table_path)
# Print out the full schema information
#print("Table schema: ", table_detail.schema)


# Initialize the Ai model 
api_key='claude_api_key'
llm = ChatAnthropic(model="claude-3-5-sonnet-20240620",
                     api_key=api_key,
                     temperature=0)

# create the template 
template= """
based on the table schema below write a SQL query that would answer the user question:
{table_info}
Return the answer in a disctiony format with the first key being a 'query' and value is the query Incase where there is an executable sql query and None if the is no query. 
the second key should be any 'information' and value should another infomation other thean the query. 
Return only the dictionary no ther statement


Question:{input}
SQL QUERY
"""
prompt= ChatPromptTemplate.from_template(template)

#get the schema and description 
def get_schema(_):
    client = bigquery.Client()
    table_detail = client.get_table(full_table_path)
    return table_detail.schema

def get_description():
    dataset_name = 'bigquery-public-data.iowa_liquor_sales'
    # Access the INFORMATION_SCHEMA for the dataset
    client = bigquery.Client()
    dataset = client.get_dataset(dataset_name) 
    return dataset.description

# Create the sql_chain 
sql_chain= (
    RunnablePassthrough.assign(table_info= get_schema)
    | prompt
    | llm
    | StrOutputParser()
)

#Second template to get the responcse 

template = """Based on the table schema below, question, sql query, and sql response, write a natural language response:
{table_info}

Question: {input}
SQL Query: {query}
SQL Response: {response}"""
prompt_response = ChatPromptTemplate.from_template(template)


def run_query(query):
    client = bigquery.Client()
    # query_job = client.query(query)  # API request
    # df_ = query_job.to_dataframe() # Waits for query to finish
    df_ = client.query_and_wait(query).to_dataframe()
    return df_


full_chain = (
    RunnablePassthrough.assign(query=sql_chain).assign(
        table_info=get_schema,
        response=lambda vars: run_query(ast.literal_eval(vars['query'])['query']) if ast.literal_eval(vars['query'])['query'] else ast.literal_eval(vars['query'])['information'],
    )
    | prompt_response
    | llm
    |  StrOutputParser()
)


def check_memory(query, max_memory= 2147483648):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(dry_run=True)
    query_job = client.query(query, job_config=job_config)
    return query_job.total_bytes_processed <= max_memory


# import time

# start_time = time.time()
# print(sql_chain.invoke({'input': 'which are the best cities by sales in  county in 2022?'}))
# print(full_chain.invoke({'input': 'which are the best cities by sales in county in 2022?'}))
# end_time = time.time()

# execution_time = end_time - start_time
# print(f"Execution time: {execution_time} seconds")
