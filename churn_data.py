import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
from sklearn.neighbors import NearestNeighbors
import numpy as np
import matplotlib.pyplot as plt

# Connect to PostgreSQL
try:
    engine = create_engine("postgresql://postgres:root@localhost:5432/Customer_Data")
    df = pd.read_sql("SELECT * FROM rfm_analysis.rfm_base", engine)
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    exit()
# Feature engineering data consistancy 
df['avg_order_value'] = df['monetary'] / df['frequency'].replace(0, 1)  # Avoid division by zero
df['monetary_per_day'] = df['monetary'] / (df['recency'] + 1)
df['is_high_value'] = (df['monetary'] > df['monetary'].quantile(0.75)).astype(int)

# Select features for clustering
features = ['frequency', 'monetary', 'avg_order_value', 'monetary_per_day', 'is_high_value']
X = df[features]

# Standardize features (DBSCAN is sensitive to scale)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Estimate eps using k-nearest neighbors
 # Rule of thumb: min_samples = 2 * number of features or adjust based on domain
eps = 0.6  # Reduced from 6 to create denser clusters
min_samples = 20 
neighbors = NearestNeighbors(n_neighbors=min_samples)
neighbors_fit = neighbors.fit(X_scaled)
distances, _ = neighbors_fit.kneighbors(X_scaled)
distances = np.sort(distances[:, min_samples - 1], axis=0)

# Plot k-distance graph to choose eps
plt.plot(distances)
plt.title("K-Distance Graph for DBSCAN eps Selection")
plt.xlabel("Points sorted by distance")
plt.ylabel(f"Distance to {min_samples}-th nearest neighbor")
plt.savefig("k_distance_plot.png")
plt.close()
print("K-distance plot saved as 'k_distance_plot.png'. Choose eps at the 'elbow' point.")

# DBSCAN implimentation 
 # Increased to require more points per cluster  # Placeholder: Adjust based on k-distance plot or domain knowledge
dbscan = DBSCAN(eps=eps, min_samples=min_samples)
df['cluster'] = dbscan.fit_predict(X_scaled)

# Analyze clustering results
print("Cluster distribution:\n", df['cluster'].value_counts())

# Prepare output DataFrame
output_df = df[['customerid', 'recency', 'frequency', 'monetary', 'avg_order_value', 'monetary_per_day', 'is_high_value', 'cluster']].copy()

# Save to CSV for Tableau
output_file = "rfm_dbscan_clusters.csv"
output_df.to_csv(output_file, index=False)
print(f"Clustering results saved to {output_file}")
