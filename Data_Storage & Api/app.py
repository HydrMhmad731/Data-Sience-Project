from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson import ObjectId
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
            elif isinstance(value, datetime):
                serialized[key] = value.isoformat()
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
        {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$publication_date"}}, "count": {"$sum": 1}}},
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
        {"$group": {"_id": "$language", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
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
    regex = f"^{year}"
    results = list(collection.find({"publication_date": {"$regex": regex}}))
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
        {"$match": {"publication_date": {"$gte": start_date, "$lte": end_date}}},
        {"$unwind": "$keywords"},
        {"$group": {"_id": "$keywords", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 21: Article Statistics by Month and Year
@app.route('/articles_by_month/<year>/<month>', methods=['GET'])
def articles_by_month(year, month):
    regex = f"^{year}-{int(month):02d}"
    results = list(collection.find({"publication_date": {"$regex": regex}}))
    return jsonify(serialize_document(results))

# Endpoint 22: Articles by Word Count Range
@app.route('/articles_by_word_count_range/<int:min_count>/<int:max_count>', methods=['GET'])
def articles_by_word_count_range(min_count, max_count):
    results = list(collection.find({"word_count": {"$gte": min_count, "$lte": max_count}}))
    return jsonify(serialize_document(results))

# Endpoint 23: Articles by Keyword Count Range
@app.route('/articles_by_keyword_count_range/<int:min_count>/<int:max_count>', methods=['GET'])
def articles_by_keyword_count_range(min_count, max_count):
    pipeline = [
        {"$project": {"keyword_count": {"$size": "$keywords"}}},
        {"$match": {"keyword_count": {"$gte": min_count, "$lte": max_count}}},
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 24: Articles by Thumbnail and Keyword Count
@app.route('/articles_by_thumbnail_and_keyword_count/<int:min_keyword_count>', methods=['GET'])
def articles_by_thumbnail_and_keyword_count(min_keyword_count):
    pipeline = [
        {"$match": {"thumbnail": {"$ne": None}}},
        {"$project": {"keyword_count": {"$size": "$keywords"}, "thumbnail": 1, "title": 1}},
        {"$match": {"keyword_count": {"$gte": min_keyword_count}}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 25: Articles Containing Certain Keywords
@app.route('/articles_containing_keywords', methods=['POST'])
def articles_containing_keywords():
    keywords = request.json.get('keywords', [])
    results = list(collection.find({"keywords": {"$in": keywords}}))
    return jsonify(serialize_document(results))

# Endpoint 26: Articles by Author and Keyword Count
@app.route('/articles_by_author_and_keyword_count/<author>/<int:min_keyword_count>', methods=['GET'])
def articles_by_author_and_keyword_count(author, min_keyword_count):
    pipeline = [
        {"$match": {"author": author}},
        {"$project": {"keyword_count": {"$size": "$keywords"}, "author": 1, "title": 1}},
        {"$match": {"keyword_count": {"$gte": min_keyword_count}}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 27: Articles by Keyword and Word Count
@app.route('/articles_by_keyword_and_word_count/<keyword>/<int:min_word_count>', methods=['GET'])
def articles_by_keyword_and_word_count(keyword, min_word_count):
    pipeline = [
        {"$match": {"keywords": keyword}},
        {"$match": {"word_count": {"$gte": min_word_count}}},
        {"$sort": {"word_count": -1}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 28: Articles by Date and Keyword
@app.route('/articles_by_date_and_keyword/<date>/<keyword>', methods=['GET'])
def articles_by_date_and_keyword(date, keyword):
    try:
        date_obj = datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
    pipeline = [
        {"$match": {"publication_date": {"$eq": date_obj}, "keywords": keyword}}
    ]
    results = list(collection.aggregate(pipeline))
    return jsonify(serialize_document(results))

# Endpoint 29: Articles by Video Duration Range
@app.route('/articles_by_video_duration_range/<int:min_duration>/<int:max_duration>', methods=['GET'])
def articles_by_video_duration_range(min_duration, max_duration):
    results = list(collection.find({"video_duration": {"$gte": min_duration, "$lte": max_duration}}))
    return jsonify(serialize_document(results))

# Endpoint 30: Articles by Last Updated Range
@app.route('/articles_by_last_updated_range/<start_date>/<end_date>', methods=['GET'])
def articles_by_last_updated_range(start_date, end_date):
    try:
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

    results = list(collection.find({"last_updated": {"$gte": start_date_obj, "$lte": end_date_obj}}))
    return jsonify(serialize_document(results))

if __name__ == '__main__':
    app.run(debug=True)
