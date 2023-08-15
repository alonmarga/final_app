from datetime import datetime
import os

from pii.mockaroo import create_mock_pii
from products.sales_generator import generate_mock_purchase_data
from get_population_data.add_cities import get_country_code, add_random_city, get_cities_file
from products.calcs import calc_total, create_invoice_data
from branches.online import split_purchases
from logs.logger_config import setup_logger
from export_final_data.export_data_by_month import export_dataframe_by_date

log_file_directory = 'logs'
# log_file_directory = r"C:\\Alon\\log_files\\"  # Directory where the log file will be stored
log_file_name = f"log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"  # Log file name with timestamp
log_file_path = os.path.join(log_file_directory, log_file_name)

logger = setup_logger(log_file_path)

output_path = 'outputs'
# output_path = r"C:\\Alon\\output_files\\"
output_file_name = f"output_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
output_file_path = os.path.join(os.path.dirname(output_path), output_file_name)

if not os.path.exists(log_file_directory):
    os.makedirs(log_file_directory)

# Check if output_path exists, if not, create it
if not os.path.exists(output_path):
    os.makedirs(output_path)

max_num_purchases = int(input("Enter the maximum number of purchases per customer: "))
logger.info(f"logs of file: {output_file_path}")
logger.info(f"Maximum number of purchases per customer: {max_num_purchases}")

personal_data = create_mock_pii()
mock_purchase_data = generate_mock_purchase_data(personal_data, max_num_purchases)

mock_purchase_data_with_total = calc_total(mock_purchase_data)
# invoice_level_data = create_invoice_data(mock_purchase_data_with_total)
cities_files_path = 'get_population_data/worldcities.csv'
cities_df = get_cities_file(cities_files_path)

mock_purchase_data_with_total_country_code = get_country_code(mock_purchase_data_with_total)
mock_purchase_data_with_total_country_code.to_csv('test.csv', index=False)
final_df = add_random_city(mock_purchase_data_with_total_country_code, cities_df)

# online_purchases_df, branches_purachse_df = split_purchases(final_df)
num_rows, num_cols = final_df.shape
logger.info(f"Final DataFrame contains {num_rows} rows and {num_cols} columns")

cols_to_count_unique = ['purchase_id', 'customer_id', 'country'] 
for col in cols_to_count_unique:
    unique_count = final_df[col].nunique()
    logger.info(f"Unique values in {col}: {unique_count}")

cols_to_sum = ['product_price', 'quantity', 'total']
for col in cols_to_sum:
    col_sum = final_df[col].sum()
    logger.info(f"Sum of {col}: {col_sum}")

final_df.to_csv(output_file_path, index=False)
export_dataframe_by_date(final_df)


print("Mock purchase data saved successfully to", output_file_path)
print("Logs data saved successfully to", log_file_path)
