import pandas as pd
from sqlalchemy import create_engine

# Connect to PostgreSQL
try:
    engine = create_engine("postgresql://postgres:root@localhost:5432/Customer_Data")
    df = pd.read_sql("SELECT * FROM public.customer_data_2", engine)
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    exit()

# Convert dates if not already
df['invoicedate'] = pd.to_datetime(df['invoicedate'])

# Step 1: Compute total transaction value (already present as Total_Price, but we recalculate to be sure)
df['Total_Price'] = df['quantity'] * df['price']

# Step 2: Group by customer
clv_df = df.groupby('customerid').agg({
    'invoiceno': pd.Series.nunique,             # unique invoice count per customer
    'Total_Price': 'sum',                     # total spent
    'invoicedate': [min, max]                 # first and last purchase
}).reset_index()

# Step 3: Rename columns
clv_df.columns = ['customerid', 'purchase_frequency', 'total_spent', 'first_purchase', 'last_purchase']

# Step 4: Compute metrics
clv_df['avg_order_value'] = clv_df['total_spent'] / clv_df['purchase_frequency']
clv_df['customer_lifespan_months'] = ((clv_df['last_purchase'] - clv_df['first_purchase']) / pd.Timedelta(days=30)).round(1)
clv_df['customer_lifespan_months'] = clv_df['customer_lifespan_months'].replace(0, 1)

# Step 5: Calculate CLV
clv_df['predicted_clv'] = clv_df['avg_order_value'] * clv_df['purchase_frequency'] * clv_df['customer_lifespan_months']

# Step 6: Save to PostgreSQL
final_df = clv_df[['customerid', 'avg_order_value', 'purchase_frequency', 'customer_lifespan_months', 'predicted_clv']]

output_file = "clv.csv"
final_df.to_csv(output_file, index=False)
