import uuid
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import random
from datetime import datetime, timedelta

# Function to generate random UUIDs
def generate_uuids(n):
    return [str(uuid.uuid4()) for _ in range(n)]

# Create random data
n_rows = 200
product_names = ['Apple', 'Banana', 'Carrot', 'Donut', 'Egg']
dates = [datetime(2023, 1, 1) + timedelta(days=random.randint(0, 270)) for _ in range(n_rows)]

data = {
    'transaction_id': range(1, n_rows + 1),
    'transaction_date': dates,
    'product_name': [random.choice(product_names) for _ in range(n_rows)],
    'quantity_sold': [random.randint(1, 10) for _ in range(n_rows)],
    'unit_price': [round(random.uniform(0.5, 5), 2) for _ in range(n_rows)],
    'customer_id': generate_uuids(n_rows)  # Add UUIDs as customer_id
}

# Create DataFrame
df = pd.DataFrame(data)

print(df)

# # Convert the Pandas DataFrame to a PyArrow Table
table = pa.table(df)

# # Save the PyArrow Table to a Parquet file, including the schema
pq.write_table(table, '/home/alonm/final_app/testing_ground/files/sales_data.parquet')
