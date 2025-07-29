import pandas as pd
import psycopg2
import psycopg2.extras

conn = psycopg2.connect(database = "Customer_Data", 
                        user = "postgres", 
                        host= 'localhost',
                        password = "root",
                        port = 5432)
df = pd.read_csv("Online_Retail.csv")  # change to your file path
df.dropna(subset=["CustomerID"], inplace=True)
df = df[df["Quantity"] > 0]
df = df[df["UnitPrice"] > 0]
df.drop_duplicates(inplace=True)
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
df["CustomerID"] = pd.to_numeric(df["CustomerID"]).astype(int)
df = df[~df['StockCode'].isin(['POST'])]
df['Total_Price'] = df['UnitPrice']*df['Quantity']
print(df)

# Remove rows where any column contains the string 'post'

insert_query = """
    INSERT INTO Customer_Data_2 (
        InvoiceNo, StockCode, Description, Quantity,
        InvoiceDate, Price, CustomerID, Country, Total_Price
    ) VALUES %s
"""
data = list(df[[
    'InvoiceNo', 'StockCode', 'Description', 'Quantity',
    'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country', 'Total_Price'
]].itertuples(index=False, name=None))

# Use psycopg2.extras.execute_values for bulk insert
with conn.cursor() as cursor:
    psycopg2.extras.execute_values(cursor, insert_query, data)
    conn.commit()


