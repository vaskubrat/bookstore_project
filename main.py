import pandas as pd

#load and clean data
books_filepath = 'data/bookstore_books.csv'
customers_filepath = 'data/bookstore_customers.csv'
orders_filepath = 'data/bookstore_orders.csv'
reviews_filepath = 'data/bookstore_reviews.csv'

books_df = pd.read_csv(books_filepath)
customers_df = pd.read_csv(customers_filepath)
orders_df = pd.read_csv(orders_filepath)
reviews_df = pd.read_csv(reviews_filepath)

#shape of each DF (rows, columns)
print(f"The books dataframe has {books_df.shape[0]} rows and {books_df.shape[1]} columns.")
print(f"The customers dataframe has {customers_df.shape[0]} rows and {customers_df.shape[1]} columns.")
print(f"The orders dataframe has {orders_df.shape[0]} rows and {orders_df.shape[1]} columns.")
print(f"The reviews dataframe has {reviews_df.shape[0]} rows and {reviews_df.shape[1]} columns.")
