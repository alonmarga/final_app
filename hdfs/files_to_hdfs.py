import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random


# Directory containing CSV files
directory_path = '/home/alonm/final_app/output_by_date'

# Define HDFS path
hdfs_path = '/test_0609'

# Connect to HDFS
fs = pa.fs.HadoopFileSystem(host='localhost', port=8020, user='root')

# Check if the directory exists in HDFS
try:
    fs.get_file_info(hdfs_path)
except pa.lib.ArrowIOError:
    # Create the directory if it doesn't exist
    fs.mkdir(hdfs_path)



# Specify partition columns
partition_cols = ['year', 'month', 'day']


# Iterate over CSV files in the directory
types = []
for file_name in os.listdir(directory_path):
    if file_name.endswith(".csv"):
        file_path = os.path.join(directory_path, file_name)

        # Read CSV file
        df = pd.read_csv(file_path)      

        unique_ids = df['customer_id'].unique()

        # Create a dictionary to map old customer_id to new customer_id
        id_map = {}
        for old_id in unique_ids:
            country = df[df['customer_id'] == old_id]['country'].iloc[0]
            new_id = f"{country}_{random.randint(1, 10001)}"
            id_map[old_id] = new_id

        # Map old customer_id to new customer_id
        df['customer_id'] = df['customer_id'].map(id_map)

        print(file_name)
        print(df)
        # Convert purchase_time to datetime
        purchase_time = pd.to_datetime(df['purchase_time'])
    
        # Extract year, month, and day
        df['year'] = purchase_time.dt.year
        df['month'] = purchase_time.dt.month
        df['day'] = purchase_time.dt.day

        df['product_price'] = df['product_price'].astype(float)
        df['total'] = df['total'].astype(float)

        df['en']
        # Convert DataFrame to Arrow Table
        table = pa.Table.from_pandas(df)
        #types.append(df['purchase_time'].dtype)
        # Write table to HDFS with partitioning
        # pq.write_to_dataset(table, root_path=hdfs_path, partition_cols=partition_cols, filesystem=fs)


# print(types)
# print(set(types))

# print(len(fs.get_file_info(selector)))
