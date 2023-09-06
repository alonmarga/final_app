from pyhive import hive
import csv
from hive_tables_conf import cols_data_type
cursor = hive.connect('localhost').cursor()


def create_hive_db():
    db_name= 'test'
    query = f"""CREATE DATABASE IF NOT EXISTS {db_name}"""
    cursor.execute(query)

def create_hive_table():
    db_name = 'test4'
    table_name = 'test'
    query = f"""CREATE TABLE IF NOT EXISTS {db_name}.{table_name}"""
    cursor.execute(query)

def  del_hive_table():
    db_name = 'test4'
    table_name = 'test4'
    query = f"""DROP TABLE IF EXISTS {db_name}.{table_name}"""
    cursor.execute(query)

def show_dbs():
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()
    for database in databases:
        print(database[0])

def drop_dbs():
    db_name = 'test2'
    cursor.execute(f"DROP DATABASE {db_name}")

def show_hive_tables():
    db_name = 'default'
    query = f"SHOW TABLES IN {db_name}"
    cursor.execute(query)
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        print(f"Table: {table_name}")

        # Fetch the columns and data types for the current table
        cursor.execute(f"DESCRIBE {db_name}.{table_name}")
        columns = cursor.fetchall()

        # Print the columns and their data types
        print("Columns:")
        for column in columns:
            col_name = column[0]
            col_data_type = column[1]
            print(f"{col_name}: {col_data_type}")

        print()  # Print an empty line for separation

def create_hive_table_with_data_types(cols_data_type):
    db_name='test'
    table_name='test2308'
    columns_str = ', '.join([f"{col_name} {col_type}" for col_name, col_type in cols_data_type.items()])
    create_query = f"CREATE TABLE {db_name}.{table_name} ({columns_str})"
    cursor.execute(create_query)
    print(f"Table '{table_name}' created successfully!")


def load_data_from_folder_to_hive_table():
    db_name='test'
    table_name='new_test'
    hdfs_folder_path = '/sales_data/testttttttt2203'
    load_query = f"LOAD DATA INPATH '{hdfs_folder_path}' INTO TABLE {db_name}.{table_name}"
    cursor.execute(load_query)
    print("Data from folder loaded successfully!")


def desc_table():
    # Define the database and table you want to describe
    database_name = 'test'
    table_name = 'new_testttttt2203'

    # Execute a describe command
    cursor.execute(f'DESCRIBE {database_name}.{table_name}')

    # Fetch the results
    results = cursor.fetchall()

    # Print the results
    for result in results:
        column_name, data_type, comment = result
        print(f"Column: {column_name}\tData Type: {data_type}")

show_hive_tables()