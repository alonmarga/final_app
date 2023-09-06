from pyhive import hive

# Connect to Hive
conn = hive.connect('localhost')
cursor = conn.cursor()

create_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS new_testttttt2203 (
    purchase_id STRING,
    customer_id BIGINT,
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
LOCATION '/sales_data/testttttttt2203'
"""
cursor.execute(create_table_query)
cursor.execute("MSCK REPAIR TABLE new_testttttt2203")

select_all_query = "select first_name from new_testttttt2203"
cursor.execute(select_all_query)
# sum_query = """
# SELECT product_id, SUM(total) AS sum_total
# FROM new_testttttt2208
# GROUP BY product_id
# """
# cursor.execute(sum_query)

results = cursor.fetchall()
for result in results:
    print(result)



# all_data_query = "select * from new_test2"
# cursor.execute(all_data_query)
# results = cursor.fetchall()
# for result in results:
#     print(result)