from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import logging

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
user = os.getenv('s_user')
password = os.getenv('s_password')
account_identifier = 'bkmvtdf-rga00884'  # Change as per your Snowflake account
warehouse = os.getenv('s_warehouse')
# Replace with your warehouse
database = os.getenv('s_DATABASE')
schema = 'PUBLIC' # Replace with your schema

# Create the Snowflake connection string
connection_string = (
    f"snowflake://{user}:{password}@{account_identifier}"
    f"/{database}/{schema}?warehouse={warehouse}"
)

# Create an engine
engine = create_engine(connection_string)

try:
    with engine.connect() as connection:
        result = connection.execute(text('SELECT * from retail_analytics.public.dim_product limit 10')).fetchone()
        print(result)
except Exception as e:
    print(f"Error connecting to Snowflake: {e}")
finally:
    engine.dispose()
