import pandas as pd
from pathlib import Path


path = Path('data/')

def extract(path):
    print(f"Extracting a dictionary from : {path}")
    dictionary = {}     #creating general dictionary as a placeholder
    for name in ['customers', 'books', 'orders', 'reviews']:
        file = path / f'bookstore_{name}.csv'
        dictionary[name] = pd.read_csv(file)
        print(f"{name} has {len(dictionary[name])} rows")
    return dictionary

def transform(tables_dict):
    print('\nTransforming tables...')
    #creating df var for transformation
    books_df = tables_dict['books']
    reviews_df = tables_dict['reviews']
    customers_df = tables_dict['customers']
    orders_df = tables_dict['orders']

    #fix NA
    print(f"Book with missing price value:\n {books_df['title'][books_df['price'].isna()]}")
    books_df = books_df.dropna(subset=['price'])
    print("Removing from dict")

    #remove invalid ratings (outside 1-5 rating)
    removed_reviews = reviews_df[(reviews_df['rating'] < 1) | (reviews_df['rating'] > 5)]
    print(f"\nRemoved reviews from dataframe:\n {removed_reviews}")
    reviews_df = reviews_df[(reviews_df['rating'] >= 1) & (reviews_df['rating'] <= 5)].copy()

    # removing orphan orders
    valid_customers = customers_df['customer_id']
    orphan_orders = orders_df[orders_df['customer_id'].isin(valid_customers) == False]
    print(f"Orphan orders:\n {orphan_orders}")
    orders_df = orders_df[orders_df['customer_id'].isin(valid_customers)].copy()

    #parse date columns
    customers_df['signup_date'] = pd.to_datetime(customers_df['signup_date'])
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date'])
    reviews_df['review_date'] = pd.to_datetime(reviews_df['review_date'])

    #updating tables dict values
    tables_dict['books'] = books_df
    tables_dict['customers'] = customers_df
    tables_dict['orders'] = orders_df
    tables_dict['reviews'] = reviews_df
    print("\nCleaned date format")

    return tables_dict

tables_dict = extract(path)
tables_dict_clean = transform(tables_dict)


