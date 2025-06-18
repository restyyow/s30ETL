import sqlite3
import pandas as pd

def run_query(db_path="S30 ETL Assignment.db"):
    query = """
            ;WITH CTE AS
            (
                SELECT
                    customer_id,
                    age
                FROM customers 
                    WHERE age BETWEEN 18 AND 35
            ), CTE_Orders AS
            (
                select
                    o.sales_id,
                    o.quantity,
                    i.item_name
                from orders o
                    inner join items i	
                        on o.item_id = i.item_id
                    where o.quantity > 0
            ), CTE_GroupOrders as
            (
                select
                    s.customer_id,
                    o.item_name,
                    SUM(o.quantity) AS quantity
                from CTE_Orders o
                    inner join Sales s
                        on o.sales_id = s.sales_id
                group by
                    s.customer_id,
                    o.item_name
            )
            select
                c.customer_id as Customer,
                c.age as Age,
                co.item_name as Item,
                co.quantity as Quantity
            from CTE c
                inner join CTE_GroupOrders co
                    ON c.customer_id = co.customer_id
                ORDER BY co.customer_id, co.item_name;
    """
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(query, conn)
    return df

def export_result(df, output_path="sqlOption.csv"):
    df.to_csv(output_path, index=False, sep=';')
    print(f"Extracted data to {output_path}")
    print(df)

df = run_query()
export_result(df)
