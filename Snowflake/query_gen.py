import os
from dotenv import load_dotenv
import snowflake.connector.cursor
import pandas as pd
import yaml
import streamlit as st
import re
#from langchain.chat_models import AzureChatOpenAI
from langchain_openai import AzureChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.chains.llm import LLMChain
load_dotenv()

SNOWFLAKE_USER = os.getenv("user")
SNOWFLAKE_PASSWORD = os.getenv("password")
SNOWFLAKE_ACCOUNT = os.getenv("account")
SNOWFLAKE_WAREHOUSE = os.getenv("warehouse")
SNOWFLAKE_DATABASE = os.getenv("database")
SNOWFLAKE_SCHEMA = os.getenv("schema")
SNOWFLAKE_HOST = os.getenv("host")
SNOWFLAKE_PORT = os.getenv("port")
SNOWFLAKE_ROLE = os.getenv("role")

Azure_EndPoint=os.getenv("Azure_EndPoint")
API_Key=os.getenv("API_Key")
API_Version=os.getenv("API_Version")

try:
    llm = AzureChatOpenAI(
        deployment_name="gpt-4o",
        model="gpt-4o",
        azure_endpoint=Azure_EndPoint,
        api_key=API_Key,
        api_version='2024-02-15-preview'
    )
    response = llm.predict("Hello, world!")  

    print("Connection successful!")
    print(f"Response from Azure OpenAI: {response}")

except Exception as e:
    print(f"Connection failed: {e}")

def load_yaml_schema(file_path):
    """Load and parse the YAML file."""
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

yaml_file_path = r"C:\Users\anju.ms\Learning\ssa-2\database_schema.yaml"
schema_data = load_yaml_schema(yaml_file_path)

def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect (
            user= os.getenv("user"),
            password= os.getenv("password"),
            account=os.getenv("account"),
            warehouse=os.getenv("warehouse"),
            database=os.getenv("database"),
            schema=os.getenv("schema"),
            host=os.getenv("host"),
            port=os.getenv("port"),
            role= os.getenv("role"),
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return None
snowflake_conn = connect_to_snowflake()

def execute_sql_query(conn, sql_query):
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        return column_names, result
    except Exception as e:
        raise Exception(f"SQL Execution Error: {e}")
    finally:
        cursor.close()

def visualize_results(column_names, rows):
    df = pd.DataFrame(rows, columns=column_names)
    return df

def extract_sql_query(response):
    """
    Extracts the SQL query from the LLM's response using regex.
    """

    sql_pattern = r"```sql\s*(.*?)\s*```"
    
    match = re.search(sql_pattern, response, re.DOTALL | re.IGNORECASE)
    
    if match:
        return match.group(1).strip()
    return None  

def generate_sql_query(schema_data, question):
    schema_str = yaml.dump(schema_data, default_flow_style=False, sort_keys=False)
    prompt_template = """
    You are an expert in generating SQL queries based on a given database schema.
    Here is the database schema described in YAML format:
    {schema}
    Generate an SQL query to answer the following question:
    {question}
    Ensure the SQL query adheres to the schema and is syntactically correct.
    Wrap the SQL query in a markdown block (```sql ... ```).
    Do not include any natural language explanations or comments outside the markdown block.
    """
    prompt = PromptTemplate(
        input_variables=["schema", "question"],
        template=prompt_template
    )
    chain = LLMChain(llm=llm, prompt=prompt)
    response = chain.run({"schema": schema_str, "question": question})
    return response.strip()

st.title("SQL Query Generator and Visualizer")


question = st.text_input("Enter your question:")

if st.button("Generate SQL Query"):
    if question:
        response = generate_sql_query(schema_data, question)
            
        sql_query = extract_sql_query(response)
        
        st.subheader("Generated SQL Query:")
        st.code(sql_query, language="sql")  
        
        try:
            if sql_query:
                column_names, rows = execute_sql_query(snowflake_conn, sql_query)
                result_df = visualize_results(column_names, rows)
                st.subheader("Query Result:")
                st.dataframe(result_df)
            else:
                st.error("Failed to extract a valid SQL query from the response.")
        except Exception as e:
            st.error(f"SQL Execution Error: {e}")
    else:
        st.warning("Please enter a question.")