import os
import yaml
from sqlalchemy import create_engine, inspect,String, Column
from sqlalchemy.schema import MetaData,Table
from sqlalchemy.orm import sessionmaker
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

user= os.getenv('user')
password = os.getenv('password')
account= os.getenv('account')
warehouse=os.getenv('warehouse')
host=os.getenv('host')
port=os.getenv('port')
role=os.getenv('role')
database=os.getenv('database')
schema=os.getenv('schema')

try:
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        database=database,
        warehouse=warehouse,
        host=os.getenv('host'),
        port=os.getenv('port'),
        role=os.getenv('role'),
        schema=os.getenv('schema')

    )
    print("snowflake.connector connection successful!")
except Exception as e:
    print(f"snowflake.connector error: {e}")

cursor = conn.cursor()


#engine = create_engine(f"snowflake:///?User={user}&Password={password}&Database={database}&Warehouse={warehouse}&Account={account}")
#{user}:{password}@{host}/{database}/{schema}?warehouse={warehouse}&role={role}
connection_url = f"snowflake://{conn}"
engine=create_engine(connection_url,creator=lambda:conn)
inspector = inspect(engine)
if engine:
    print("successful")

metadata=MetaData()

try:
    metadata.reflect(bind=engine)
    inspector = inspect(engine)
except Exception as e:
    print(f"Error reflecting database schema:{e}")
    engine.dispose()
    conn.close()
    exit()

table_data={}

for table_name, table in metadata.tables.items():
    table_info = {
        'columns': [],
        'primary_key': [],
        'foreign_keys': []
    }

    for column in table.columns:
        column_info = {
            'name': str(column.name),
            'type': str(column.type),
            'nullable': column.nullable,
            'default': column.default.arg if column.default else None
        }
        table_info['columns'].append(column_info)

    # Get primary key information
    primary_key = inspector.get_pk_constraint(table_name)
    if primary_key['constrained_columns']:
        table_info['primary_key'] = primary_key['constrained_columns']

    # Get foreign key information
    foreign_keys = inspector.get_foreign_keys(table_name)
    for fk in foreign_keys:
        fk_info = {
            'constrained_columns': fk['constrained_columns'],
            'referred_table': fk['referred_table'],
            'referred_columns': fk['referred_columns']
        }
        table_info['foreign_keys'].append(fk_info)

    # Add table information to the main dictionary
    table_data[table_name] = table_info

# Serialize to YAML
yaml_output = yaml.dump(table_data, default_flow_style=False)

# Write to a YAML file
with open('database_schema.yaml', 'w') as file:
    file.write(yaml_output)

print("YAML file 'database_schema.yaml' has been created successfully.")

# Close connections
engine.dispose()
conn.close()