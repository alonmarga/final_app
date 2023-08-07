import os
import pandas as pd

def export_dataframe_by_date(input_df):
    output_path = r'C:\Alon\outout_by_date'
    input_df['purchase_time'] = pd.to_datetime(input_df['purchase_time'])

    # Group by date and export
    grouped = input_df.groupby(input_df['purchase_time'].dt.date)

    for date, group in grouped:
        filename = os.path.join(output_path, f"export_{date}.csv")
        group.to_csv(filename, index=False)
        print(f"Exported {len(group)} rows to {filename}")