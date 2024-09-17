from pymongo import MongoClient
from polyglot.text import Text
from concurrent.futures import ThreadPoolExecutor, as_completed

# MongoDB connection URI
uri = "mongodb+srv://hydr-mhmd:NDeZ2Szte09vSy2y@ds-cluster.a0vcg.mongodb.net/?retryWrites=true&w=majority&appName=DS-Cluster"
client = MongoClient(uri)

# Access the database and collection
db = client["DgPad_DS"]  # Database name
collection = db["Almayadeen-articles"]  # Collection name


# Function to extract entities and remove duplicates
def extract_entities(text):
    polyglot_text = Text(text, hint_language_code='ar')  # Arabic text
    entities = {"PER": set(), "LOC": set(), "ORG": set()}

    # Loop through each entity
    for entity in polyglot_text.entities:
        entity_str = " ".join(entity)
        if entity.tag == "I-PER":
            entities["PER"].add(entity_str)
        elif entity.tag == "I-LOC":
            entities["LOC"].add(entity_str)
        elif entity.tag == "I-ORG":
            entities["ORG"].add(entity_str)

    # Convert sets back to lists to store in MongoDB
    return {key: list(value) for key, value in entities.items()}


# Function to process a single article
def process_article(article):
    full_text = article.get("full_text", "")
    if full_text:
        extracted_entities = extract_entities(full_text)
        # Update the article with the extracted entities
        collection.update_one(
            {"_id": article["_id"]},  # Find the article by its ID
            {"$set": {"entities": extracted_entities}}  # Set the new 'entities' field
        )


# Define batch size
batch_size = 500  # Number of articles per batch

# Initialize counters
total_processed = 0

# Process articles in batches
while True:
    # Reinitialize the cursor for each batch
    cursor = collection.find().batch_size(batch_size).skip(total_processed)
    batch = list(cursor.limit(batch_size))

    if not batch:
        break

    with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust the number of workers as needed
        futures = [executor.submit(process_article, article) for article in batch]
        for future in as_completed(futures):
            future.result()  # Ensure any exceptions are raised

    total_processed += len(batch)
    print(f"Processed {total_processed} articles so far.")

print("Entity extraction and storage completed for all articles.")
