import pandas as pd
import psycopg2
import psycopg2.extras

conn = psycopg2.connect(database = "Customer_Data", 
                        user = "postgres", 
                        host= 'localhost',
                        password = "root",
                        port = 5432)
df = pd.read_csv("online_retail_2.csv")  # change to your file path
df.dropna(subset=["Customer ID"], inplace=True)
df = df[df["Quantity"] > 0]
df = df[df["Price"] > 0]
df.drop_duplicates(inplace=True)
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
df["Customer ID"] = pd.to_numeric(df["Customer ID"]).astype(int)
df = df[~df['StockCode'].isin(['POST'])]
df['Total_Price'] = df['Price']*df['Quantity']
print(df)

# Remove rows where any column contains the string 'post'

insert_query = """
    INSERT INTO Customer_Data_2 (
        InvoiceNo, StockCode, Description, Quantity,
        InvoiceDate, Price, CustomerID, Country, Total_Price
    ) VALUES %s
"""
data = list(df[[
    'Invoice', 'StockCode', 'Description', 'Quantity',
    'InvoiceDate', 'Price', 'Customer ID', 'Country', 'Total_Price'
]].itertuples(index=False, name=None))

# Use psycopg2.extras.execute_values for bulk insert
with conn.cursor() as cursor:
    psycopg2.extras.execute_values(cursor, insert_query, data)
    conn.commit()


