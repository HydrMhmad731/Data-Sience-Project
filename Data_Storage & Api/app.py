from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson import ObjectId
from bson.json_util import dumps, loads
from datetime import datetime, timedelta

# Start the Flask app
app = Flask(__name__)

# The Connection string to connect to MongoDB
client = MongoClient("mongodb+srv://hydr-mhmd:NDeZ2Szte09vSy2y@ds-cluster.a0vcg.mongodb.net/?retryWrites=true&w=majority&appName=DS-Cluster")
db = client["DgPad_DS"]
collection = db["Almayadeen-articles"]

def serialize_document(document):
    """Convert MongoDB document ObjectId to string for JSON serialization."""
    if isinstance(document, list):
        return [serialize_document(doc) for doc in document]
    elif isinstance(document, dict):
        serialized = {}
        for key, value in document.items():
            if isinstance(value, ObjectId):
                serialized[key] = str(value)
            elif isinstance(value, dict) or isinstance(value, list):
                serialized[key] = serialize_document(value)
            else:
                serialized[key] = value
        return serialized
    else:
        return document

@app.route('/')
def index():
    return "Welcome to the Al Mayadeen Articles API!"

# Endpoint 1: Top Keywords
@app.route('/top_keywords', methods=['GET'])
def top_keywords():
    pipeline = [
        {"$unwind": "$keywords"},
        {"$group": {"_id": "$keywords", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 2: Top Authors
@app.route('/top_authors', methods=['GET'])
def top_authors():
    pipeline = [
        {"$group": {"_id": "$author", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 3: Articles by Date
@app.route('/articles_by_date', methods=['GET'])
def articles_by_date():
    pipeline = [
        {"$group": {"_id": "$publication_date", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 4: Articles by Word Count
@app.route('/articles_by_word_count', methods=['GET'])
def articles_by_word_count():
    pipeline = [
        {"$group": {"_id": "$word_count", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 5: Articles by Language
@app.route('/articles_by_language', methods=['GET'])
def articles_by_language():
    pipeline = [
        {"$group": {"_id": "$language", "count": {"$sum": 1}}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 6: Articles by Category
@app.route('/articles_by_classes', methods=['GET'])
def articles_by_classes():
    pipeline = [
        {"$unwind": "$classes"},
        {"$group": {"_id": "$classes", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 7: Recent Articles
@app.route('/recent_articles', methods=['GET'])
def recent_articles():
    pipeline = [
        {"$sort": {"publication_date": -1}},
        {"$limit": 10}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 8: Articles by Keyword
@app.route('/articles_by_keyword/<keyword>', methods=['GET'])
def articles_by_keyword(keyword):
    results = list(collection.find({"keywords": keyword}))
    return jsonify(serialize_document(results))

# Endpoint 9: Articles by Author
@app.route('/articles_by_author/<author_name>', methods=['GET'])
def articles_by_author(author_name):
    results = list(collection.find({"author": author_name}))
    return jsonify(serialize_document(results))

# Endpoint 10: Top Classes
@app.route('/top_classes', methods=['GET'])
def top_classes():
    pipeline = [
        {"$unwind": "$classes"},
        {"$group": {"_id": "$classes", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 11: Article Details
@app.route('/article_details/<postid>', methods=['GET'])
def article_details(postid):
    result = collection.find_one({"postid": int(postid)})
    return jsonify(serialize_document(result))

# Endpoint 12: Articles Containing Video
@app.route('/articles_with_video', methods=['GET'])
def articles_with_video():
    results = list(collection.find({"video_duration": {"$ne": None}}))
    return jsonify(serialize_document(results))

# Endpoint 13: Articles by Publication Year
@app.route('/articles_by_year/<year>', methods=['GET'])
def articles_by_year(year):
    results = list(collection.find({"publication_date": {"$regex": f"^{year}"}}))
    return jsonify(serialize_document(results))

# Endpoint 14: Longest Articles
@app.route('/longest_articles', methods=['GET'])
def longest_articles():
    pipeline = [
        {"$sort": {"word_count": -1}},
        {"$limit": 10}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 15: Shortest Articles
@app.route('/shortest_articles', methods=['GET'])
def shortest_articles():
    pipeline = [
        {"$sort": {"word_count": 1}},
        {"$limit": 10}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 16: Articles by Keyword Count
@app.route('/articles_by_keyword_count', methods=['GET'])
def articles_by_keyword_count():
    pipeline = [
        {"$project": {"keyword_count": {"$size": "$keywords"}}},
        {"$group": {"_id": "$keyword_count", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 17: Articles by Thumbnail Presence
@app.route('/articles_with_thumbnail', methods=['GET'])
def articles_with_thumbnail():
    results = list(collection.find({"thumbnail": {"$ne": None}}))
    return jsonify(serialize_document(results))

# Endpoint 18: Articles Updated After Publication
@app.route('/articles_updated_after_publication', methods=['GET'])
def articles_updated_after_publication():
    pipeline = [
        {"$match": {"$expr": {"$gt": ["$last_updated", "$publication_date"]}}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 19: Articles by Coverage
@app.route('/articles_by_coverage/<coverage>', methods=['GET'])
def articles_by_coverage(coverage):
    results = list(collection.find({"classes": coverage}))
    return jsonify(serialize_document(results))

# Endpoint 20: Most Popular Keywords in the Last X Days
@app.route('/popular_keywords_last_X_days/<days>', methods=['GET'])
def popular_keywords_last_X_days(days):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=int(days))
    pipeline = [
        {"$match": {"publication_date": {"$gte": start_date}}},
        {"$unwind": "$keywords"},
        {"$group": {"_id": "$keywords", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 21: Articles by Published Month
@app.route('/articles_by_month/<year>/<month>', methods=['GET'])
def articles_by_month(year, month):
    results = list(collection.find({"publication_date": {"$regex": f"^{year}-{month}"}}))
    return jsonify(serialize_document(results))

# Endpoint 22: Articles by Word Count Range
@app.route('/articles_by_word_count_range/<min>/<max>', methods=['GET'])
def articles_by_word_count_range(min, max):
    results = list(collection.find({"word_count": {"$gte": int(min), "$lte": int(max)}}))
    return jsonify(serialize_document(results))

# Endpoint 23: Articles with Specific Keyword Count
@app.route('/articles_with_specific_keyword_count/<count>', methods=['GET'])
def articles_with_specific_keyword_count(count):
    results = list(collection.find({"keyword_count": int(count)}))
    return jsonify(serialize_document(results))

# Endpoint 24: Articles by Specific Date
@app.route('/articles_by_specific_date/<date>', methods=['GET'])
def articles_by_specific_date(date):
    results = list(collection.find({"publication_date": date}))
    return jsonify(serialize_document(results))

# Endpoint 25: Articles Containing Specific Text
@app.route('/articles_containing_text/<text>', methods=['GET'])
def articles_containing_text(text):
    results = list(collection.find({"content": {"$regex": text}}))
    return jsonify(serialize_document(results))

# Endpoint 26: Articles with More than N Words
@app.route('/articles_with_more_than/<word_count>', methods=['GET'])
def articles_with_more_than(word_count):
    results = list(collection.find({"word_count": {"$gt": int(word_count)}}))
    return jsonify(serialize_document(results))

# Endpoint 27: Articles Grouped by Coverage
@app.route('/articles_grouped_by_coverage', methods=['GET'])
def articles_grouped_by_coverage():
    pipeline = [
        {"$unwind": "$classes"},
        {"$group": {"_id": "$classes", "count": {"$sum": 1}}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 28: Articles Published in Last X Hours
@app.route('/articles_last_X_hours/<hours>', methods=['GET'])
def articles_last_X_hours(hours):
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=int(hours))
    results = list(collection.find({"publication_date": {"$gte": start_date}}))
    return jsonify(serialize_document(results))

# Endpoint 29: Articles by Length of Title
@app.route('/articles_by_title_length', methods=['GET'])
def articles_by_title_length():
    pipeline = [
        {"$project": {"title_length": {"$strLenCP": "$title"}}},
        {"$group": {"_id": "$title_length", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 30: Most Updated Articles
@app.route('/most_updated_articles', methods=['GET'])
def most_updated_articles():
    pipeline = [
        {"$sort": {"update_count": -1}},
        {"$limit": 10}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Run the Flask application
if __name__ == '__main__':
    app.run(debug=True)
