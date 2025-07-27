import os
from google.cloud import bigquery
from sklearn.cluster import KMeans

client = bigquery.Client(project="jovial-totality-458202-q8")

