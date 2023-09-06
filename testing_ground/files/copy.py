import shutil

# Source file path
source_file = "/home/alonm/final_app/testing_ground/files/test.parquet"

# Destination file path (in the same folder)
destination_file = "/home/alonm/final_app/testing_ground/files/test_copy.parquet"

# Copy the file
shutil.copy(source_file, destination_file)
