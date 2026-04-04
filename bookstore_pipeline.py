import pandas as pd
from pathlib import Path
import sqlite3

db_path = Path('data/bookstore.db')

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

    #add revenue column to orders
    merged_orders_books_df = pd.merge(orders_df, books_df, on='book_id', how='inner')
    merged_orders_books_df['revenue'] = merged_orders_books_df['quantity'] * merged_orders_books_df['price']
    print(f"\nMerged Orders and Books DF :\n {merged_orders_books_df.head(2)}")

    # print summary
    print("Summary of changes")
    print(f"Number of rows in Orders DF: {len(orders_df)}")
    print(f"Number of rows in Books DF: {len(books_df)}")
    print(f"Number of rows in Customers DF: {len(customers_df)}")
    print(f"Number of rows in Reviews DF: {len(reviews_df)}, removed rows: {len(removed_reviews)}, as invalid ratings")

    #updating tables dict values
    tables_dict['books'] = books_df
    tables_dict['customers'] = customers_df
    tables_dict['orders'] = merged_orders_books_df
    tables_dict['reviews'] = reviews_df
    print("\nCleaned date format")
    return tables_dict

def load(tables_dict, db_path):
    # creating df var for loading into DB
    books_df = tables_dict['books']
    reviews_df = tables_dict['reviews']
    customers_df = tables_dict['customers']
    orders_df = tables_dict['orders']

    print(f"Loading tables from {db_path}")
    conn = sqlite3.connect(db_path)
    customers_df.to_sql('customers', conn, if_exists='replace', index=False)
    orders_df.to_sql('orders', conn, if_exists='replace', index=False)
    reviews_df.to_sql('reviews', conn, if_exists='replace', index=False)
    books_df.to_sql('books', conn, if_exists='replace', index=False)
    print("Bookstore DB and Customers, Orders, Reviews and Books tables created")
    return conn

def report(conn):
#11 run query: top 5 customers by total spend (CTE joining orders - books - customers)
    top_five = pd.read_sql('''
    with customer_spend as (
        select customer_id, 
                sum(revenue) as total_spent,
                count(order_id) as num_orders
        from orders
        where status = 'Delivered'
        group by customer_id
    )
    select c.name, s.total_spent, s.num_orders
    from customer_spend s
    inner join customers c
        on s.customer_id = c.customer_id
    order by s.total_spent desc
    limit 5
    ''', conn)
    print(f"\nTop five customer spend:\n {top_five}")

#12 run query: genre popularity: nbr of orders and total revenue per genre (joining orders + books)
    Nbr_orders_revenue_per_genre = pd.read_sql('''
    SELECT b.genre, 
            count(o.order_id) AS nbr_orders,
            sum(b.price * o.quantity) AS total_revenue
    FROM orders o
    INNER JOIN books b ON o.book_id = b.book_id
    GROUP BY b.genre
    ORDER BY total_revenue DESC
    ''', conn)
    print(f"\nNumber of orders and total revenue per genre:\n {Nbr_orders_revenue_per_genre}")

#13 city breakdown: total revenue and number of unique customers per city
    revenue_unique_clt_per_city = pd.read_sql('''
    SELECT COUNT(distinct c.customer_id) AS unique_customers,
            sum(o.revenue) AS total_revenue
    FROM customers c
    INNER JOIN orders o ON o.customer_id = c.customer_id
    GROUP BY c.city
    ORDER BY total_revenue DESC
    ''', conn)
    print(f"\nTotal revenue and unique customers per city:\n {revenue_unique_clt_per_city}")

    highest_rated_books = pd.read_sql('''
    SELECT b.title,
            ROUND(AVG(r.rating), 1) AS avg_rating,
            COUNT(r.rating) AS total_reviews
    FROM books b
    INNER JOIN reviews r ON r.book_id = b.book_id
    GROUP BY b.book_id
    HAVING count(r.rating) > 1
    ORDER BY r.rating DESC
    ''', conn)
    print(f"\nHighest rated books with at least 2 reviews:\n {highest_rated_books}")

tables_dict = extract(path)
tables_dict_clean = transform(tables_dict)
connection = load(tables_dict_clean, db_path)
report(connection)

