import sqlite3
import pandas as pd

with sqlite3.connect("S30 ETL Assignment.db") as conn:
    customers_df = pd.read_sql_query("select customer_id, age from customers where age BETWEEN 18 AND 35", conn)
    sales_df = pd.read_sql_query("select sales_id, customer_id from sales", conn)
    orders_df = pd.read_sql_query("select sales_id, item_id, quantity from orders where quantity > 0", conn)
    items_df = pd.read_sql_query("select item_id, item_name from items", conn)

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

result.to_csv("pandaOption.csv", index=False, sep=';')

print("Exported to pandaOption.csv")
print(result)
