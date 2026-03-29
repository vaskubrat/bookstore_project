import pandas as pd
#connect to DB
import sqlite3

def null_count(df):
    print(f"Columns with null counts: {df.isnull().sum()[df.isnull().sum() > 0]}")

def cleanup_dates(df, column):
    df[column] = pd.to_datetime(df[column])
    return df

def connect():
    return sqlite3.connect('data/bookstore.db')

def run_query(query, conn):
    return pd.read_sql(query, conn)