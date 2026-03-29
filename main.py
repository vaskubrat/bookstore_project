from functions import *
from bookstore_pipeline import *

#load and clean data
books_filepath = 'data/bookstore_books.csv'
customers_filepath = 'data/bookstore_customers.csv'
orders_filepath = 'data/bookstore_orders.csv'
reviews_filepath = 'data/bookstore_reviews.csv'

books_df = extract(books_filepath)
customers_df = extract(customers_filepath)
orders_df = extract(orders_filepath)
reviews_df = extract(reviews_filepath)

#shape of each DF (rows, columns)
print("\nInitial rows and columns count before clean-up")
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
#books DF: already a float, no need to convert
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

#connect to database
conn = connect()

#create tables
customers_df.to_sql('customers', conn, if_exists='replace', index=False)
orders_df.to_sql('orders', conn, if_exists='replace', index=False)
reviews_df.to_sql('reviews', conn, if_exists='replace', index=False)
books_df.to_sql('books', conn, if_exists='replace', index=False)
print("Bookstore database and customers, orders, reviews and books tables created")

#count rows
for table in ['customers', 'orders', 'reviews', 'books']:
    count = run_query(f"select count(*) as rows from {table}", conn)
    print(f"{table}: {count['rows'][0]} rows")

#inner joins
three_table_inner_join = run_query('''SELECT o.order_id, c.name, b.title, o.quantity, o.order_date
                       FROM orders o
                       INNER JOIN customers c
                       ON o.customer_id = c.customer_id
                       INNER JOIN books b
                       ON o.book_id = b.book_id''', conn)
print("\nFirst inner join")
print(three_table_inner_join.head(10))

orders_books_inner_join = run_query('''
SELECT o.order_id, b.title, o.quantity, b.price, (o.quantity * b.price) as revenue
FROM orders o
INNER JOIN books b
ON o.book_id = b.book_id
where status = 'Delivered'
order by revenue desc''', conn)
print("\nSecond inner join")
print(orders_books_inner_join.head(10))
print('\n')

reviews_books_customers_inner_join = run_query('''
Select c.name, b.title, r.rating, r.review_date
from reviews r
INNER JOIN books b
ON r.book_id = b.book_id
INNER JOIN customers c
ON c.customer_id = r.customer_id
order by rating desc, r.review_date desc''', conn)

print("\nThird inner join")
print(reviews_books_customers_inner_join.head(10))
print('\n')

orders_customers_books_inner_join = run_query('''
Select c.city, count(o.order_id) as nbr_orders, (sum(o.quantity * b.price)) as total_revenue, (avg(o.quantity * b.price)) as avg_order_value
from orders o
inner join customers c
ON o.customer_id = c.customer_id
INNER JOIN books b
ON o.book_id = b.book_id
group by c.city
order by total_revenue desc''', conn)

print("\nFourth inner join")
print(orders_customers_books_inner_join.head(10))
print('\n')

#LEFT joins

left_cust_orders = run_query('''
select c.customer_id, c.name, o.order_id
from customers c
left join orders o
on o.customer_id = c.customer_id''', conn)
print("\nFirst left join")
print(left_cust_orders.head(15))
#No I have no nulls in order Id
print('\n')

left_cust_orders = run_query('''
select c.customer_id, c.name, o.order_id
from customers c
left join orders o
on o.customer_id = c.customer_id
where order_id IS NULL''', conn)
print("\nSecond left join")
print(left_cust_orders.head(15))
print('\n')

left_books_reviews = run_query('''
select b.book_id, b.title, 
count(r.review_id) as reviews_per_book, 
coalesce(avg(rating),0) as avg_rating
from books b
left join reviews r
on b.book_id = r.book_id
group by b.book_id, b.title
order by avg_rating desc''', conn)
print("\nThird left join")
print(left_books_reviews.head(15))
print('\n')

left_books_orders = run_query('''
select b.book_id, b.title, o.order_id
from books b
left join orders o
on o.book_id = b.book_id
where o.book_id IS NULL''', conn)
print("\nFourth left join")
print(left_books_orders.head(15))
print('\n')

#CTEs

customer_spend_query = run_query('''
    with customer_spend as (
        select o.customer_id, 
                sum(o.quantity * b.price) as total_spent, 
                count(o.order_id) as num_orders
        from orders o
            inner join books b
            on o.book_id = b.book_id
        where o.status = 'Delivered'
        group by o.customer_id
    )
    select c.name, c.city, cs.num_orders, cs.total_spent
    from customer_spend cs
    inner join customers c
        on cs.customer_id = c.customer_id
    order by cs.total_spent desc
''', conn)
print('First CTE')
print(customer_spend_query.head(15))
print('\n')
rank_customer_spend_query = run_query('''
    with customer_spend as (
            select o.customer_id, 
                    sum(o.quantity * b.price) as total_spent, 
                    count(o.order_id) as num_orders
            from orders o
                inner join books b
                on o.book_id = b.book_id
            where o.status = 'Delivered'
            group by o.customer_id
    )
    select c.name, c.city, cs.num_orders, cs.total_spent, rank() over (
    order by cs.total_spent desc) as spend_rank
    from customer_spend cs
    inner join customers c
    on cs.customer_id = c.customer_id
''', conn)
print('Second CTE')
print(rank_customer_spend_query.head(15))