import sqlite3
import pandas as pd

conn = sqlite3.connect("S30 ETL Assignment.db")

customers_df = pd.read_sql_query("SELECT * FROM customers", conn)
sales_df = pd.read_sql_query("SELECT * FROM sales", conn)
orders_df = pd.read_sql_query("SELECT * FROM orders", conn)
items_df = pd.read_sql_query("SELECT * FROM items", conn)

eligible_customers = customers_df[(customers_df['age'] >= 18) & (customers_df['age'] <= 35)]

merged = eligible_customers\
    .merge(sales_df, on='customer_id')\
    .merge(orders_df, on='sales_id')\
    .merge(items_df, on='item_id')

result = merged.groupby(['customer_id', 'age', 'item_name'])['quantity'].sum().reset_index()
result = result[result['quantity'] > 0]
result['quantity'] = result['quantity'].astype(int)
result.columns = ['Customer', 'Age', 'Item', 'Quantity']
result.to_csv("pandaOption.csv", index=False, sep=';')

print("Exported to pandaOption.csv")
print(result)

conn.close()
