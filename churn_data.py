import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

engine = create_engine("postgresql://postgres:root@localhost:5432/Customer_Data")
df = pd.read_sql("SELECT * FROM rfm_analysis.rfm_base", engine)

df['churn'] = df['recency'].apply(lambda x: 1 if x > 90 else 0)
df['avg_order_value'] = df['monetary'] / df['frequency']
df['monetary_per_day'] = df['monetary'] / (df['recency'] + 1)  # +1 to avoid division by zero
df['is_high_value'] = (df['monetary'] > df['monetary'].quantile(0.75)).astype(int)
X = df[['frequency', 'monetary', 'avg_order_value', 'monetary_per_day', 'is_high_value']]
y=df['churn']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

# Predict & Evaluate
y_pred = model.predict(X_test)
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay

cm = confusion_matrix(y_test, y_pred)
ConfusionMatrixDisplay(cm).plot()
print(classification_report(y_test, y_pred))