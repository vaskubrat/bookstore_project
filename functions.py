import pandas as pd

def null_count(df):
    print(f"Columns with null counts: {df.isnull().sum()[df.isnull().sum() > 0]}")

def cleanup_dates(df, column):
    df[column] = pd.to_datetime(df[column])
    return df
