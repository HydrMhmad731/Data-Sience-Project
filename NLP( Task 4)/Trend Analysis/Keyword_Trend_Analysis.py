import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
from datetime import datetime
import collections
import matplotlib as mpl

# Enable right-to-left (RTL) support
mpl.rcParams['axes.unicode_minus'] = False

# MongoDB connection
client = MongoClient("mongodb+srv://hydr-mhmd:NDeZ2Szte09vSy2y@ds-cluster.a0vcg.mongodb.net/?retryWrites=true&w=majority&appName=DS-Cluster")
db = client["DgPad_DS"]
collection = db["Almayadeen-articles"]

# Fetching articles with keywords and publication time
articles = collection.find({}, {"published_time": 1, "keywords": 1})
data = list(articles)

# Create DataFrame
df = pd.DataFrame(data)

# Convert 'published_time' to datetime format
df['published_time'] = pd.to_datetime(df['published_time'])

# Explode keywords list to create one row per keyword
df = df.explode('keywords')

# Group by date and keyword to count occurrences
df_grouped = df.groupby([df['published_time'].dt.date, 'keywords']).size().unstack(fill_value=0)

# Plotting for top 10 keywords
top_keywords = df_grouped.sum().nlargest(10).index  # Get top 10 keywords
plt.figure(figsize=(12, 6))

for keyword in top_keywords:
    plt.plot(df_grouped.index, df_grouped[keyword], label=keyword)

plt.xlabel('Date')
plt.ylabel('Number of Mentions')
plt.title('Keyword Usage Trends Over Time')

# Set legend to display keywords right-to-left
plt.legend(prop={'size': 10}, title="Keywords", title_fontsize='13', bbox_to_anchor=(1.05, 1), borderaxespad=0.)
plt.gca().legend().set_title('الكلمات')  # Setting the legend title in Arabic (Right-to-left)

plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

# Save the plot
plt.savefig('keyword_trends.png')
plt.show()
