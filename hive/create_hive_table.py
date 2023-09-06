from pyhive import hive

# Connect to Hive
conn = hive.connect('localhost')
cursor = conn.cursor()

create_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS test_0609 (
    purchase_id STRING,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    country STRING,
    club BOOLEAN,
    card_type STRING,
    gender STRING,
    currency STRING,
    email STRING,
    product_id STRING,
    product_name STRING,
    product_price DOUBLE,
    quantity BIGINT,
    department_id STRING,
    department_name STRING,
    sub_department_id STRING,
    sub_department_name STRING,
    purchase_time STRING,
    online BOOLEAN,
    country_code STRING,
    total DOUBLE,
    city STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION '/test_0609'
"""
cursor.execute(create_table_query)
cursor.execute("MSCK REPAIR TABLE test_0609")

# select_all_query = "select purchase_time from alon2308"
#  
# sum_query = """
# SELECT *
# FROM  new_test1111
# """
# cursor.execute(sum_query)

# results = cursor.fetchall()
# for result in results:
#     print(result)



# all_data_query = "select * from new_test2"
# cursor.execute(all_data_query)
# results = cursor.fetchall()
# for result in results:
#     print(result)