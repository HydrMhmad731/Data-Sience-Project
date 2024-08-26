from flask import Flask, jsonify
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from pymongo.errors import OperationFailure
from bson.json_util import dumps

app = Flask(__name__)

# Connect to MongoDB Atlas
client = MongoClient("mongodb+srv://hydr-mhmd:NDeZ2Szte09vSy2y@ds-cluster.a0vcg.mongodb.net/?retryWrites=true&w=majority&appName=DS-Cluster")
db = client["DgPad_DS"]
collection = db["Almayadeen-articles"]

# 1. Top Keywords
@app.route('/top_keywords', methods=['GET'])
def top_keywords():
    pipeline = [
        {"$unwind": "$keywords"},
        {"$group": {"_id": "$keywords", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 2. Top Authors
@app.route('/top_authors', methods=['GET'])
def top_authors():
    pipeline = [
        {"$group": {"_id": "$author", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 3. Articles by Date
@app.route('/articles_by_date', methods=['GET'])
def articles_by_date():
    try:
        pipeline = [
            {
                "$addFields": {
                    "published_time": {
                        "$toDate": "$published_time"
                    }
                }
            },
            {
                "$group": {
                    "_id": "$published_time",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"_id": 1}  # Sort by date ascending
            },
            {
                "$project": {
                    "_id": 0,
                    "date": "$_id",
                    "count": 1
                }
            }
        ]

        result = list(collection.aggregate(pipeline))
        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500

# 4. Articles by Word Count
@app.route('/articles_by_word_count', methods=['GET'])
def articles_by_word_count():
    pipeline = [
        {"$group": {"_id": "$word_count", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 5. Articles by Language
@app.route('/articles_by_language', methods=['GET'])
def articles_by_language():
    pipeline = [
        {"$group": {"_id": "$lang", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 6. Articles by Category (Classes)
@app.route('/articles_by_classes', methods=['GET'])
def articles_by_classes():
    pipeline = [
        {"$unwind": "$classes"},
        {"$group": {"_id": "$classes", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 7. Recent Articles
@app.route('/recent_articles', methods=['GET'])
def recent_articles():
    try:
        # Find the most recent 10 articles sorted by published_time in descending order
        result = list(collection.find().sort("published_time", -1).limit(10))

        # Convert ObjectId to string for JSON serialization and format the result
        for article in result:
            if '_id' in article:
                article['_id'] = str(article['_id'])

        # Check if the result is empty
        if not result:
            return jsonify({"message": "No recent articles found"}), 404

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 8. Articles by Keyword
@app.route('/articles_by_keyword/<keyword>', methods=['GET'])
def articles_by_keyword(keyword):
    pipeline = [
        {"$match": {"keywords": keyword}},
        {"$project": {"_id": 0, "postid": 1, "title": 1, "author": 1, "published_time": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 9. Articles by Author
@app.route('/articles_by_author/<author_name>', methods=['GET'])
def articles_by_author(author_name):
    pipeline = [
        {"$match": {"author": author_name}},
        {"$project": {"_id": 0, "postid": 1, "title": 1, "published_time": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 10. Top Classes
@app.route('/top_classes', methods=['GET'])
def top_classes():
    pipeline = [
        {"$unwind": "$classes"},
        {"$group": {"_id": "$classes", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 11. Article Details
@app.route('/article_details/<postid>', methods=['GET'])
def article_details(postid):
    result = collection.find_one({"postid": postid}, {"_id": 0})
    return jsonify(result)

# 12. Articles Containing Video
@app.route('/articles_with_video', methods=['GET'])
def articles_with_video():
    pipeline = [
        {"$match": {"video_duration": {"$ne": None}}},
        {"$project": {"_id": 0, "postid": 1, "title": 1, "video_duration": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 13. Articles by Publication Year
@app.route('/articles_by_year/<int:year>', methods=['GET'])
def articles_by_year(year):
    try:
        # Define the start and end dates for the year
        start_date = datetime(year, 1, 1)
        end_date = datetime(year + 1, 1, 1)

        # Define the aggregation pipeline
        pipeline = [
            {
                "$addFields": {
                    "published_time": {
                        "$toDate": "$published_time"  # Cast to date
                    }
                }
            },
            {
                "$match": {
                    "published_time": {"$gte": start_date, "$lt": end_date}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "postid": 1,
                    "title": 1,
                    "published_time": 1
                }
            }
        ]

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # If no results, return a message indicating no data found
        if not result:
            return jsonify({"message": "No articles found for the specified year."})

        # Return the results
        return jsonify(result)

    except OperationFailure as e:
        # Return an error message if the aggregation fails
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        # Return an error message if something goes wrong
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500

# 14. Longest Articles
@app.route('/longest_articles', methods=['GET'])
def longest_articles():
    try:
        # Retrieve and sort the articles by word_count in descending order
        cursor = collection.find().sort("word_count", -1).limit(10)

        # Convert each document to a dictionary and convert _id to string
        result = []
        for doc in cursor:
            # Convert ObjectId to string
            doc['_id'] = str(doc['_id'])
            result.append(doc)

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 15. Shortest Articles
@app.route('/shortest_articles', methods=['GET'])
def shortest_articles():
    try:
        # Retrieve and sort the articles by word_count in ascending order
        cursor = collection.find().sort("word_count", 1).limit(10)

        # Convert each document to a dictionary and convert _id to string
        result = []
        for doc in cursor:
            # Convert ObjectId to string
            doc['_id'] = str(doc['_id'])
            result.append(doc)

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 16. Articles by Keyword Count
@app.route('/articles_by_keyword_count', methods=['GET'])
def articles_by_keyword_count():
    pipeline = [
        {"$project": {"keyword_count": {"$size": "$keywords"}}},
        {"$group": {"_id": "$keyword_count", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 17. Articles with Thumbnail
@app.route('/articles_with_thumbnail', methods=['GET'])
def articles_with_thumbnail():
    pipeline = [
        {"$match": {"thumbnail": {"$ne": None}}},
        {"$project": {"_id": 0, "postid": 1, "title": 1, "thumbnail": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 18. Articles Updated After Publication
@app.route('/articles_updated_after_publication', methods=['GET'])
def articles_updated_after_publication():
    pipeline = [
        {"$match": {"$expr": {"$gt": ["$last_updated", "$published_time"]}}},
        {"$project": {"_id": 0, "postid": 1, "title": 1, "last_updated": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 19. Articles by Coverage
@app.route('/articles_by_coverage/<coverage>', methods=['GET'])
def articles_by_coverage(coverage):
    try:
        # Aggregate pipeline to match documents where 'coverage' exists in 'classes'
        pipeline = [
            {
                "$match": {
                    "classes": {
                        "$elemMatch": {
                            "mapping": "coverage",
                            "value": coverage
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "postid": 1,
                    "title": 1,
                    "published_time": 1
                }
            }
        ]

        result = list(collection.aggregate(pipeline))
        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 20. Most Popular Keywords in the Last X Days
@app.route('/popular_keywords_last_<int:x>_days', methods=['GET'])
def popular_keywords_last_X_days(x):
    try:
        # Calculate the date threshold
        date_threshold = datetime.utcnow() - timedelta(days=x)

        # Aggregation pipeline
        pipeline = [
            {
                "$addFields": {
                    "published_time": {
                        "$dateFromString": {
                            "dateString": "$published_time",
                            "onError": None  # Handle invalid date strings
                        }
                    }
                }
            },
            {
                "$match": {
                    "published_time": {
                        "$gte": date_threshold
                    }
                }
            },
            {
                "$unwind": "$keywords"  # Unwind the keywords array
            },
            {
                "$group": {
                    "_id": "$keywords",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"count": -1}
            },
            {
                "$limit": 10
            }
        ]

        # Execute aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if the result is empty
        if not result:
            return jsonify({"message": "No keywords found for the specified period"}), 404

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 21. Articles by Published Month
@app.route('/articles_by_month/<int:year>/<int:month>', methods=['GET'])
def articles_by_month(year, month):
    try:
        # Calculate the start and end dates for the specified month
        start_date = datetime(year, month, 1)
        end_date = datetime(year, month + 1, 1) if month < 12 else datetime(year + 1, 1, 1)

        # Aggregation pipeline
        pipeline = [
            {
                "$addFields": {
                    "published_time": {
                        "$dateFromString": {
                            "dateString": "$published_time",
                            "onError": None  # Handle invalid date strings
                        }
                    }
                }
            },
            {
                "$match": {
                    "published_time": {
                        "$gte": start_date,
                        "$lt": end_date
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "postid": 1,
                    "title": 1,
                    "published_time": 1
                }
            }
        ]

        # Execute aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if the result is empty
        if not result:
            return jsonify({"message": "No articles found for the specified month"}), 404

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 22. Articles by Word Count Range
@app.route('/articles_by_word_count_range/<int:min>/<int:max>', methods=['GET'])
def articles_by_word_count_range(min, max):
    try:
        # Aggregation pipeline
        pipeline = [
            {
                "$match": {
                    "word_count": {
                        "$gte": min,
                        "$lte": max
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "postid": 1,
                    "title": 1,
                    "word_count": 1
                }
            }
        ]

        # Execute aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if the result is empty
        if not result:
            return jsonify({"message": "No articles found for the specified word count range"}), 404

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 23. Articles with No Keywords
@app.route('/articles_with_no_keywords', methods=['GET'])
def articles_with_no_keywords():
    try:
        # Aggregation pipeline to find documents with no keywords
        pipeline = [
            {
                "$match": {
                    "$or": [
                        {"keywords": {"$exists": False}},
                        {"keywords": {"$eq": []}}
                    ]
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "postid": 1,
                    "title": 1
                }
            }
        ]

        # Execute aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if the result is empty
        if not result:
            return jsonify({"message": "No articles found with no keywords"}), 404

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 24. Most Frequent Video Duration
@app.route('/most_frequent_video_duration', methods=['GET'])
def most_frequent_video_duration():
    pipeline = [
        {"$group": {"_id": "$video_duration", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 25. Articles by Specific Day
@app.route('/articles_by_day/<int:year>/<int:month>/<int:day>', methods=['GET'])
def articles_by_day(year, month, day):
    try:
        # Create start and end dates
        start_date = datetime(year, month, day)
        end_date = start_date + timedelta(days=1)

        # Aggregation pipeline
        pipeline = [
            {
                "$addFields": {
                    "published_time": {"$toDate": "$published_time"}  # Ensure published_time is a date
                }
            },
            {
                "$match": {
                    "published_time": {"$gte": start_date, "$lt": end_date}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "postid": 1,
                    "title": 1,
                    "published_time": 1
                }
            }
        ]

        # Execute aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if the result is empty
        if not result:
            return jsonify({"message": "No articles found for the specified date"}), 404

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 26. Articles by Word Count Over X
@app.route('/articles_by_word_count_over/<int:x>', methods=['GET'])
def articles_by_word_count_over(x):
    pipeline = [
        {"$match": {"word_count": {"$gt": x}}},
        {"$project": {"_id": 0, "postid": 1, "title": 1, "word_count": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 27. Articles by Word Count Under X
@app.route('/articles_by_word_count_under/<int:x>', methods=['GET'])
def articles_by_word_count_under(x):
    try:
        # Aggregation pipeline
        pipeline = [
            {"$match": {"word_count": {"$lt": x}}},
            {"$project": {"_id": 0, "postid": 1, "title": 1, "word_count": 1}}
        ]

        # Execute aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if the result is empty
        if not result:
            return jsonify({"message": "No articles found with word count under the specified limit"}), 404

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 28. Articles by Keyword Length
@app.route('/articles_by_keyword_length', methods=['GET'])
def articles_by_keyword_length():
    pipeline = [
        {"$project": {"keyword_length": {"$size": "$keywords"}}},
        {"$group": {"_id": "$keyword_length", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 29. Article with Longest Title
@app.route('/article_with_longest_title', methods=['GET'])
def article_with_longest_title():
    try:
        # Aggregation pipeline
        pipeline = [
            {"$project": {"title_length": {"$strLenCP": "$title"}, "postid": 1, "title": 1}},
            {"$sort": {"title_length": -1}},
            {"$limit": 1}
        ]

        # Execute aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Convert ObjectId to string for JSON serialization
        for article in result:
            if '_id' in article:
                article['_id'] = str(article['_id'])

        # Check if the result is empty
        if not result:
            return jsonify({"message": "No articles found"}), 404

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 30. Article with Shortest Title
@app.route('/article_with_shortest_title', methods=['GET'])
def article_with_shortest_title():
    try:
        # Aggregation pipeline
        pipeline = [
            {"$project": {"title_length": {"$strLenCP": "$title"}, "postid": 1, "title": 1}},
            {"$sort": {"title_length": 1}},  # Sort by title length ascending
            {"$limit": 1}  # Limit to 1 result
        ]

        # Execute aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Convert ObjectId to string for JSON serialization
        for article in result:
            if '_id' in article:
                article['_id'] = str(article['_id'])

        # Check if the result is empty
        if not result:
            return jsonify({"message": "No articles found"}), 404

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
