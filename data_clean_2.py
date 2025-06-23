import pandas as pd
from google.cloud import bigquery
from pandas_gbq import to_gbq

# Load your dataset
df = pd.read_csv("online_retail_2.csv")  # change to your file path

# Remove rows with missing CustomerID
df.dropna(subset=["Customer ID"], inplace=True)

# Remove negative or zero quantities (usually returns)
df = df[df["Quantity"] > 0]

# Remove rows with zero or negative unit prices
df = df[df["Price"] > 0]

# Optional: remove duplicates
df.drop_duplicates(inplace=True)

# Convert data types
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
df["Customer ID"] = df["Customer ID"].astype(str)



# Set project ID and table
project_id = "jovial-totality-458202-q8"  # Replace with your GCP project ID
destination_table = "Stock_data.Customer_Data_2"  # Format: dataset.table

# Upload DataFrame
to_gbq(df, destination_table, project_id=project_id, if_exists='replace')

