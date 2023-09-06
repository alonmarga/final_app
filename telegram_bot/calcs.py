from pyhive import hive
from telegram import Update
from telegram.ext import CallbackContext
import tabulate
import pandas as pd

conn = hive.connect('localhost')


def fetch_data_from_hive():
    query = "SELECT * FROM test_3108"
    df = pd.read_sql(query, conn) # Pass the connection object
    print(df.columns)
    # Remove 'new_test.' prefix from column names
    df.columns = [col.replace('test_3108.', '') for col in df.columns]
    
    return df

df = fetch_data_from_hive()

group_product_df = df.groupby(by=['product_name'])
group_department_df = df.groupby(by=['department_name', 'sub_department_name'])

# print(df)

async def get_grouped_products(update: Update, context: CallbackContext):
    result_df = group_product_df.agg({'product_name': 'count', 'total': 'sum'})
    result_df = result_df.rename(columns={'product_name': 'count', 'total': 'sum'}).reset_index()

    # Format the DataFrame as a table
    table = tabulate.tabulate(result_df, headers='keys', tablefmt='pretty')

    # Send the formatted table as a reply to the user
    await update.message.reply_text(f"Grouped Data:\n{table}")

async def get_one_product(update: Update, context: CallbackContext):
    if not context.args:
        await update.message.reply_text("Please provide a product name as a parameter.")
        return

    parameter = context.args[0].lower()  # Convert the parameter to lowercase
    result_df = group_product_df.agg({'product_name': 'count', 'total': 'sum'})
    result_df = result_df.rename(columns={'product_name': 'count', 'total': 'sum'}).reset_index()

    # Convert all product names in the DataFrame to lowercase for comparison
    result_df['product_name'] = result_df['product_name'].str.lower()

    result_df = result_df[result_df['product_name'] == parameter]
    if len(result_df) > 0:
        # Format the DataFrame as a table
        table = tabulate.tabulate(result_df, headers='keys', tablefmt='pretty')
        # Send the formatted table as a reply to the user
        await update.message.reply_text(f"Grouped Data for Product '{parameter}':\n{table}")
    else:
        await update.message.reply_text("No such product")

async def get_departments(update: Update, context: CallbackContext):
    # Calculate required values
    result_df = group_department_df.agg({
        'sub_department_name': 'count',
        'total': 'sum'
    })

    # Rename columns for clarity
    result_df.columns = ['sub_department_count', 'sub_department_total']

    # Calculate department count and total
    department_summary = df.groupby('department_name').agg({
        'sub_department_name': 'count',
        'total': 'sum'
    })
    department_summary.columns = ['department_count', 'department_total']

    # Merge department summary with the result_df
    result_df = result_df.merge(department_summary, left_on='department_name', right_index=True)

    # Calculate percentages
    result_df['sub_department_percentage'] = round((result_df['sub_department_count'] / result_df['department_count']) * 100, 2)
    result_df['sub_department_total_percentage'] = round((result_df['sub_department_total'] / result_df['department_total']) * 100, 2)

    # Format numbers with commas for better readability
    result_df['sub_department_count'] = result_df['sub_department_count'].apply(lambda x: f'{x:,}')
    result_df['sub_department_total'] = result_df['sub_department_total'].apply(lambda x: f'{x:,}')
    result_df['department_count'] = result_df['department_count'].apply(lambda x: f'{x:,}')
    result_df['department_total'] = result_df['department_total'].apply(lambda x: f'{x:,}')

    # Reset index for a cleaner output
    result_df = result_df.reset_index()
    result_df.sort_values('sub_department_total', inplace=True, ascending=False)
    result_df = result_df[:10]

    # Print the table with formatted numbers
    table = tabulate.tabulate(result_df, headers='keys', tablefmt='pretty')
    await update.message.reply_text(f"Top 10 sub departments:\n{table}")