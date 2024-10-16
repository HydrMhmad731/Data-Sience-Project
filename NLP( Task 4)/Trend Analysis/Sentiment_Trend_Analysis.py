import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
from datetime import datetime


client = MongoClient("mongodb+srv://hydr-mhmd:NDeZ2Szte09vSy2y@ds-cluster.a0vcg.mongodb.net/?retryWrites=true&w=majority&appName=DS-Cluster")
db = client["DgPad_DS"]
collection = db["Almayadeen-articles"]

articles = collection.find({}, {"published_time": 1, "sentiment": 1})
data = list(articles)

# Create DataFrame
df = pd.DataFrame(data)

# Convert 'published_time' to datetime format
df['published_time'] = pd.to_datetime(df['published_time'])

# Group by date and sentiment to count occurrences
df_grouped = df.groupby([df['published_time'].dt.date, 'sentiment']).size().unstack(fill_value=0)

# Plotting
plt.figure(figsize=(12, 6))
for sentiment in df_grouped.columns:
    plt.plot(df_grouped.index, df_grouped[sentiment], label=sentiment)

plt.xlabel('Date')
plt.ylabel('Number of Articles')
plt.title('Sentiment Trends Over Time')
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

# Save the plot
plt.savefig('sentiment_trends.png')
plt.show()
