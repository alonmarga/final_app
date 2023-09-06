import os
import pyarrow as pa
import pyarrow.parquet as pq

# Directory containing Parquet files
directory_path = '/home/alonm/final_app/testing_ground/files'

# Define HDFS path
hdfs_path = '/sales_data/testttttttt2203'

# Connect to HDFS
fs = pa.fs.HadoopFileSystem(host='localhost', port=8020, user='root')

# Check if the directory exists in HDFS
try:
    fs.get_file_info(hdfs_path)
except pa.lib.ArrowIOError:
    # Create the directory if it doesn't exist
    fs.mkdir(hdfs_path)

# Read the first Parquet file in the directory
for file_name in os.listdir(directory_path):
    if file_name.endswith(".parquet"):
        file_path = os.path.join(directory_path, file_name)

        # Read Parquet file
        table = pq.read_table(file_path)
        print(table)

        # Write table to HDFS without partitioning (remove partition_cols if not needed)
        pq.write_to_dataset(table, root_path=hdfs_path, filesystem=fs)

        # Break after reading the first Parquet file
        break

selector = pa.fs.FileSelector(hdfs_path, recursive=True)
print(len(fs.get_file_info(selector)))