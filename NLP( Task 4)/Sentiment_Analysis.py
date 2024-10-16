from pymongo import MongoClient
from textblob import TextBlob
from concurrent.futures import ThreadPoolExecutor

# Connect to MongoDB using your URI
uri = "mongodb+srv://hydr-mhmd:NDeZ2Szte09vSy2y@ds-cluster.a0vcg.mongodb.net/?retryWrites=true&w=majority&appName=DS-Cluster"
client = MongoClient(uri)

# Access the database and collection
db = client["DgPad_DS"]  # Your database name
collection = db["Almayadeen-articles"]  # Your collection name


# Function to analyze sentiment
def analyze_sentiment(text):
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity
    if sentiment > 0:
        return 'positive'
    elif sentiment < 0:
        return 'negative'
    else:
        return 'neutral'


# Function to process each article
def process_article(article):
    # Get the full text of the article
    article_text = article.get('full_text', '')

    # Analyze sentiment
    sentiment_result = analyze_sentiment(article_text)

    # Update the article document with sentiment
    collection.update_one(
        {'_id': article['_id']},  # Filter by article ID
        {'$set': {'sentiment': sentiment_result}}  # Update with sentiment result
    )
    return article['_id']  # Return the article ID for progress tracking


# Get all articles from the collection
articles = list(collection.find())

# Use ThreadPoolExecutor to process articles concurrently
with ThreadPoolExecutor(max_workers=10) as executor:
    # Submit all articles for processing
    results = list(executor.map(process_article, articles))

print(f"Sentiment analysis completed for {len(results)} articles and updated in MongoDB!")
