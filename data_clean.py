import pandas as pd
from google.cloud import bigquery
from pandas_gbq import to_gbq

# Load CSV file
df = pd.read_csv('Online Retail.xlsx', encoding='ISO-8859-1')

# Drop rows with missing essential fields
df.dropna(subset=["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice"], inplace=True)

# Convert data types
df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype(int)
df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce").fillna(0.0)
df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce").astype("Int64")  # Allows for nulls
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")

# Remove negative or zero quantities or prices (optional, depending on use case)
df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)]

# Strip whitespace from text fields
df["Description"] = df["Description"].str.strip()
df["Country"] = df["Country"].str.strip()

# Drop duplicate rows
df.drop_duplicates(inplace=True)




# Set project ID and table
project_id = "jovial-totality-458202-q8"  # Replace with your GCP project ID
destination_table = "Stock_data.Customer_Data"  # Format: dataset.table

# Upload DataFrame
to_gbq(df, destination_table, project_id=project_id, if_exists='replace')