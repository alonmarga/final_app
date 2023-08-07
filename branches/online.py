def split_purchases(df):
    branches_purchases_df = df[df['online']==False]
    online_purchases_df = df[df['online']==True]
    return online_purchases_df, branches_purchases_df