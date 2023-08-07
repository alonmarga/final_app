import pyarrow as pa
import pyarrow.parquet as pq
import hdfs3

# Replace these values with your actual HDFS setup
hdfs_host = 'localhost'
hdfs_port = 9000
hdfs_path = '/user/naya/Test/'  # The destination path in HDFS
csv_file_path = '/home/naya/Alon/outputs/output_2023-08-01_14-18-13.csv'  # Replace with the actual path to your CSV file

def write_csv_to_parquet_in_hdfs():
    # Connect to HDFS
    hdfs = hdfs3.HDFileSystem(host=hdfs_host, port=hdfs_port)

    # Read the CSV file from Docker
    with open(csv_file_path, 'rb') as file:
        csv_data = file.read()

    # Infer the schema from the CSV data
    inferred_schema = pa.csv.read_csv(csv_data).schema

    # Convert the CSV data to Arrow Table
    table = pa.csv.read_csv(csv_data, schema=inferred_schema)

    # Write the Arrow Table as Parquet to a BytesIO object
    parquet_output = pa.BufferOutputStream()
    pq.write_table(table, parquet_output)

    # Get the Parquet data from the BytesIO object
    parquet_data = parquet_output.getvalue().to_pybytes()

    # Write the Parquet data to HDFS
    with hdfs.open(hdfs_path + 'your_parquet_file_name.parquet', 'wb') as hdfs_file:
        hdfs_file.write(parquet_data)

if __name__ == '__main__':
    write_csv_to_parquet_in_hdfs()
