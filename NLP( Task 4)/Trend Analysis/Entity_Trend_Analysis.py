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

# Fetching articles with entities (PER, LOC, ORG) and publication time
articles = collection.find({}, {"published_time": 1, "entities": 1})
data = list(articles)

# Create DataFrame
df = pd.DataFrame(data)

# Convert 'published_time' to datetime format
df['published_time'] = pd.to_datetime(df['published_time'])

# Extract 'PER', 'LOC', 'ORG' entities into separate rows using explode
df['PER'] = df['entities'].apply(lambda x: x.get('PER', []) if x else [])
df['LOC'] = df['entities'].apply(lambda x: x.get('LOC', []) if x else [])
df['ORG'] = df['entities'].apply(lambda x: x.get('ORG', []) if x else [])

df_per = df.explode('PER')
df_loc = df.explode('LOC')
df_org = df.explode('ORG')

# Group by date and entity (PER, LOC, ORG) to count occurrences
df_per_grouped = df_per.groupby([df['published_time'].dt.date, 'PER']).size().unstack(fill_value=0)
df_loc_grouped = df_loc.groupby([df['published_time'].dt.date, 'LOC']).size().unstack(fill_value=0)
df_org_grouped = df_org.groupby([df['published_time'].dt.date, 'ORG']).size().unstack(fill_value=0)

# Plotting for top 10 people (PER)
top_per = df_per_grouped.sum().nlargest(10).index  # Get top 10 people
plt.figure(figsize=(12, 6))
for person in top_per:
    plt.plot(df_per_grouped.index, df_per_grouped[person], label=person)

plt.xlabel('Date')
plt.ylabel('Number of Mentions')
plt.title('Top 10 Person (PER) Mentions Over Time')
plt.legend(prop={'size': 10}, title="People", title_fontsize='13', bbox_to_anchor=(1.05, 1), borderaxespad=0.)
plt.gca().legend().set_title('أشخاص')  # Legend title in Arabic

plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

# Save the plot
plt.savefig('person_trends.png')
plt.show()

# Plotting for top 10 locations (LOC)
top_loc = df_loc_grouped.sum().nlargest(10).index  # Get top 10 locations
plt.figure(figsize=(12, 6))
for location in top_loc:
    plt.plot(df_loc_grouped.index, df_loc_grouped[location], label=location)

plt.xlabel('Date')
plt.ylabel('Number of Mentions')
plt.title('Top 10 Location (LOC) Mentions Over Time')
plt.legend(prop={'size': 10}, title="Locations", title_fontsize='13', bbox_to_anchor=(1.05, 1), borderaxespad=0.)
plt.gca().legend().set_title('المواقع')  # Legend title in Arabic

plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

# Save the plot
plt.savefig('location_trends.png')
plt.show()

# Plotting for top 10 organizations (ORG)
top_org = df_org_grouped.sum().nlargest(10).index  # Get top 10 organizations
plt.figure(figsize=(12, 6))
for org in top_org:
    plt.plot(df_org_grouped.index, df_org_grouped[org], label=org)

plt.xlabel('Date')
plt.ylabel('Number of Mentions')
plt.title('Top 10 Organization (ORG) Mentions Over Time')
plt.legend(prop={'size': 10}, title="Organizations", title_fontsize='13', bbox_to_anchor=(1.05, 1), borderaxespad=0.)
plt.gca().legend().set_title('المنظمات')  # Legend title in Arabic

plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()

# Save the plot
plt.savefig('organization_trends.png')
plt.show()
