import pyarrow as pa
import pyarrow.fs as pafs
import pandas as pd

def list_files_recursive(fs, path, records):
    try:
        info_list = fs.get_file_info(pafs.FileSelector(path, recursive=False))
        for info in info_list:
            sub_path = info.path
            type_info = 'dir' if info.type == pafs.FileType.Directory else 'file'
            records.append((sub_path, type_info))
            if info.type == pafs.FileType.Directory:
                list_files_recursive(fs, sub_path, records)
    except pa.lib.ArrowIOError:
        print(f"The path {path} does not exist.")

# Define HDFS connection parameters
HDFS_HOST = 'localhost'
HDFS_PORT = 8020
HDFS_USER = 'root'

# Connect to HDFS
fs = pafs.HadoopFileSystem(host=HDFS_HOST, port=HDFS_PORT, user=HDFS_USER)

# Define the HDFS path to the directory you want to explore
hdfs_path_to_explore = '/'

# List to store the path and type information
records = []

list_files_recursive(fs, hdfs_path_to_explore, records)

# Create a DataFrame
df = pd.DataFrame(records, columns=['path', 'type'])
print(df)
