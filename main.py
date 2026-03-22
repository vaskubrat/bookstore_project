from functions import *

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
print('=' * 50)

#null counts of each DF

print("Books dataframe null counts")
books_null_count = null_count(books_df)

print("Customers dataframe null counts")
customers_null_count = null_count(customers_df)

print("Orders dataframe null counts")
orders_null_count = null_count(orders_df)

print("Reviews dataframe null counts")
reviews_null_count = null_count(reviews_df)

print('=' * 50)

#clean up NA values
#books DF (already a float, no need to convert
print(books_df.dtypes)
print(f"Book with missing price value:\n {books_df['title'][books_df['price'].isna()]}")
books_df = books_df.dropna(subset=['price'])

#reviews DF: remove any rating outside 1-5 range
print("\nCleaning of Reviews dataframe")
removed_reviews = reviews_df[(reviews_df['rating'] < 1) | (reviews_df['rating'] > 5)]
print(f"Removed reviews from dataframe:\n {removed_reviews}")
reviews_df = reviews_df[(reviews_df['rating'] >= 1) & (reviews_df['rating'] <= 5)]
print('=' * 50)

#orders DF: find orphan customer_id
valid_customers = customers_df['customer_id']
orphan_orders = orders_df[orders_df['customer_id'].isin(valid_customers) == False]
print(f"Orphan orders:\n {orphan_orders}")
orders_df = orders_df[orders_df['customer_id'].isin(valid_customers)]

#clean up dates
customers_df = cleanup_dates(customers_df, 'signup_date')
orders_df = cleanup_dates(orders_df, 'order_date')
reviews_df = cleanup_dates(reviews_df, 'review_date')
print("\nCleaned date format")
print('=' * 50)

#final shapes
print("Final shapes of the dataframes after cleanup")
print(f"The books dataframe has {books_df.shape[0]} rows and {books_df.shape[1]} columns.")
print(f"The customers dataframe has {customers_df.shape[0]} rows and {customers_df.shape[1]} columns.")
print(f"The orders dataframe has {orders_df.shape[0]} rows and {orders_df.shape[1]} columns.")
print(f"The reviews dataframe has {reviews_df.shape[0]} rows and {reviews_df.shape[1]} columns.")
print('=' * 50)
