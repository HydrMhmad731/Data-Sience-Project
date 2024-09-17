import pymongo
import json
import os
from pymongo.server_api import ServerApi

# The connection string that connects this code with monodb atlas
uri = "mongodb+srv://hydr-mhmd:NDeZ2Szte09vSy2y@ds-cluster.a0vcg.mongodb.net/?retryWrites=true&w=majority&appName=DS-Cluster"

# To reate a new client and connect to the server
client = pymongo.MongoClient(uri, server_api=ServerApi('1'))
db = client["DgPad_DS"]  # The name of the database at mongodb atlas
collection = db["Almayadeen-articles"]  # the name of the collection in the database

# The file containing the saved json articles
directory = 'C:/Users/USER/PYlearning/DataScience-DGPAD/scraped_articles'

# Iterate over all JSON files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.json'):
        file_path = os.path.join(directory, filename)
        try:
            with open(file_path, encoding='utf-8') as f:
                data = json.load(f)
                # Ensure data is a list of dictionaries
                if isinstance(data, list):
                    collection.insert_many(data)
                    print(f"{filename} inserted successfully!")
                else:
                    print(f"{filename} is not a valid JSON array. Skipping...")
        except Exception as e:
            print(f"Error processing {filename}: {e}")

print("All data inserted successfully!")
