import pandas as pd
import logging


def calc_total(mock_purchase_data):
    logger = logging.getLogger("main_logger.calc_func")
    logger.info("Entered calc func")
    mock_df = pd.DataFrame.from_dict(mock_purchase_data)
    mock_df['total'] = mock_df['quantity'] * mock_df['product_price']
    logger.info("Calc func ended")
    return mock_df

#create dataframe based on the purchase_id
def create_invoice_data(df):
    invoice_df = df.groupby('purchase_id').agg({
        'customer_id': 'first',
        'first_name': 'first',
        'last_name': 'first',
        'email': 'first',
        'quantity': 'sum',
        'purchase_time': 'first',
        'gender': 'first',
        'country': 'first',
        'club': 'first',
        'card_type': 'first',
        'currency': 'first',
        'total': 'sum',
        'online': 'first',
    }).reset_index()

    return invoice_df