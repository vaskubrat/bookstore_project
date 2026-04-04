# Bookstore Pipeline

ETL pipeline that cleans and analyzes online bookstore data using Python, pandas, and SQLite.

## What it does

- Reads 4 CSV files (customers, books, orders, reviews)
- Cleans data issues (missing prices, invalid ratings, orphan orders)
- Loads cleaned data into a SQLite database
- Runs summary reports (top customers, genre popularity, city breakdown, book ratings)

## How to run
pip install -r requirements.txt
python bookstore_pipeline.py

## Tech used

- Python
- pandas
- SQLite
- SQL (joins, CTEs, window functions)
