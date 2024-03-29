import os
import pandas as pd

output_path = 'output_by_date'
if not os.path.exists(output_path):
    os.makedirs(output_path)

def export_dataframe_by_date(input_df):
    input_df['purchase_time'] = pd.to_datetime(input_df['purchase_time'])

 
    grouped = input_df.groupby(input_df['purchase_time'].dt.date)

    for date, group in grouped:
        filename = os.path.join(output_path, f"export_{date}.csv")
        group.to_csv(filename, index=False)
        # print(f"Exported {len(group)} rows to {filename}")