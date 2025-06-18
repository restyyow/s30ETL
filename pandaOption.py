import sqlite3
import pandas as pd

def fetch_data(db_path="S30 ETL Assignment.db"):
    with sqlite3.connect(db_path) as conn:
        customers_df = pd.read_sql_query("SELECT customer_id, age FROM customers WHERE age BETWEEN 18 AND 35", conn)
        sales_df = pd.read_sql_query("SELECT sales_id, customer_id FROM sales", conn)
        orders_df = pd.read_sql_query("SELECT sales_id, item_id, quantity FROM orders WHERE quantity > 0", conn)
        items_df = pd.read_sql_query("SELECT item_id, item_name FROM items", conn)
    return customers_df, sales_df, orders_df, items_df

def process_and_export_to_csv(customers_df, sales_df, orders_df, items_df, output_path="pandaOption.csv"):
    merged = (
        customers_df
        .merge(sales_df, on='customer_id', how='inner')
        .merge(orders_df, on='sales_id', how='inner')
        .merge(items_df, on='item_id', how='inner')
    )

    result = (
        merged
        .groupby(['customer_id', 'age', 'item_name'], as_index=False)['quantity']
        .sum()
        .rename(columns={
            'customer_id': 'Customer',
            'age': 'Age',
            'item_name': 'Item',
            'quantity': 'Quantity'
        })
    )

    result.to_csv(output_path, index=False, sep=';')
    print(f"Exported to {output_path}")
    print(result)

customers_df, sales_df, orders_df, items_df = fetch_data()
process_and_export_to_csv(customers_df, sales_df, orders_df, items_df)
