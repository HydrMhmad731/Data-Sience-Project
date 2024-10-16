from flask import Flask, jsonify
from flask_cors import CORS
from flask import request
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime, timedelta
from pymongo.errors import OperationFailure
from bson.json_util import dumps
import pandas as pd


app = Flask(__name__)
CORS(app)

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
        {"$limit": 20}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 2. Top Authors
@app.route('/top_authors', methods=['GET'])
def top_authors():
    pipeline = [
        {"$group": {"_id": "$author", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20}
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
        {"$sort": {"count": -1}},
        {"$limit": 20}
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
        {"$project": {"_id": 0, "postid": 1, "title": 1, "author": 1, "published_time": 1, "url": 1}}
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

# 9.1 all authors
@app.route('/all_authors', methods=['GET'])
def all_authors():
    authors = collection.distinct("author")  # Get distinct authors from MongoDB
    return jsonify(authors)


# 10. Top Classes
@app.route('/top_classes', methods=['GET'])
def top_classes():
    limit = int(request.args.get('limit', 10))  # Get the limit from query parameters, default to 10
    pipeline = [
        {"$unwind": "$classes"},
        {"$group": {"_id": "$classes", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": limit}
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
                "$group": {
                    "_id": {
                        "month": {"$month": "$published_time"},
                        "year": {"$year": "$published_time"}
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {
                    "_id.year": 1,
                    "_id.month": 1
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "month": "$_id.month",
                    "year": "$_id.year",
                    "count": 1
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
# count updated articles
@app.route('/count_updated_articles', methods=['GET'])
def count_updated_articles():
    # Define the query to find articles updated after publication
    pipeline = [
        {
            '$match': {
                '$expr': {
                    '$gt': ['$last_updated', '$published_time']
                }
            }
        },
        {
            '$count': 'count'
        }
    ]

    # Execute the aggregation pipeline
    result = list(collection.aggregate(pipeline))

    # Get the count from the result
    count = result[0]['count'] if result else 0

    return jsonify({"count": count})


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


# 23. Articles with Specific Keyword Count
@app.route('/articles_with_specific_keyword_count/<int:count>', methods=['GET'])
def articles_with_specific_keyword_count(count):
    try:
        pipeline = [
            {"$project": {"keyword_count": {"$size": "$keywords"}, "postid": 1, "title": 1, "published_time": 1}},
            {"$match": {"keyword_count": count}},
            {"$limit": 10}  # Adjust this limit as needed
        ]
        result = list(collection.aggregate(pipeline))

        # Convert ObjectId to string directly in the route
        for doc in result:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        # Check if no articles were found
        if not result:
            return jsonify({"message": f"No articles found with exactly {count} keywords."}), 404

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500



# 24. Articles by Specific Date
@app.route('/articles_by_specific_date/<date>', methods=['GET'])
def articles_by_specific_date(date):
    try:
        # Parse the date string into a datetime object
        try:
            specific_date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD."}), 400

        # Create start and end dates for the day
        start_date = specific_date
        end_date = start_date + timedelta(days=1)

        # Aggregation pipeline to find articles by specific date
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

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if no articles were found for the date
        if not result:
            return jsonify({"message": f"No articles found for the date {date}."}), 404

        # Format the response with articles grouped by the specified date
        response = {
            "date": date,
            "articles": result
        }

        return jsonify(response)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500

# 25 articles with a specific text
@app.route('/articles_containing_text/<text>', methods=['GET'])
def articles_containing_text(text):
    try:
        # Aggregation pipeline to find articles containing specific text in full_text
        pipeline = [
            {
                "$match": {
                    "full_text": {"$regex": text, "$options": "i"}  # Case-insensitive search for the text
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "postid": 1,
                    "title": 1,
                    "full_text": 1,
                    "published_time": 1,
                    "url": 1  # Include the URL field
                }
            },
            {
                "$limit": 10  # Adjust limit as needed
            }
        ]

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if no articles were found
        if not result:
            return jsonify({"message": f"No articles found containing the text '{text}'."}), 404

        # Format the response with articles containing the specified text
        response = {
            "text": text,
            "articles": result
        }

        return jsonify(response)

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

# 28. articles grouped by coverage
@app.route('/articles_grouped_by_coverage', methods=['GET'])
def articles_grouped_by_coverage():
    try:
        # Aggregation pipeline to group articles by the 'coverage' field in 'classes'
        pipeline = [
            {
                "$unwind": "$classes"  # Unwind the 'classes' array
            },
            {
                "$match": {
                    "classes.mapping": "coverage"  # Ensure we are matching coverage
                }
            },
            {
                "$group": {
                    "_id": "$classes.value",  # Group by the coverage category
                    "article_count": {"$sum": 1}  # Count the number of articles
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "coverage": "$_id",
                    "article_count": 1
                }
            }
        ]

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if no articles were found
        if not result:
            return jsonify({"message": "No articles found for any coverage category."}), 404

        # Format the response with coverage and article counts
        response = {
            "coverage_summary": result
        }

        return jsonify(response)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 29 articles last x hours
@app.route('/articles_last_X_hours/<int:x>', methods=['GET'])
def articles_last_X_hours(x):
    try:
        # Calculate the time threshold (current time minus X hours)
        time_threshold = datetime.utcnow() - timedelta(hours=x)

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
                        "$gte": time_threshold  # Filter articles published in the last X hours
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "title": 1,
                    "published_time": 1
                }
            },
            {
                "$sort": {
                    "published_time": -1  # Sort articles by most recent first
                }
            }
        ]

        # Execute aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if no articles were found
        if not result:
            return jsonify({"message": f"No articles found in the last {x} hours."}), 404

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500

# 30 most articles by title length
@app.route('/articles_by_title_length', methods=['GET'])
def articles_by_title_length():
    try:
        # Aggregation pipeline
        pipeline = [
            {
                "$addFields": {
                    "title_length": {
                        "$size": {
                            "$split": ["$title", " "]  # Split the title into words and count them
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$title_length",  # Group by the length of the title
                    "count": {"$sum": 1}  # Count the number of articles for each title length
                }
            },
            {
                "$sort": {"_id": 1}  # Sort by title length in ascending order
            }
        ]

        # Execute aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if no articles were found
        if not result:
            return jsonify({"message": "No articles found."}), 404

        # Format the result to include title length in the message
        formatted_result = [
            {
                "title_length": item["_id"],
                "count": item["count"]
            } for item in result
        ]

        return jsonify(formatted_result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500

# 31 most updated artciles
@app.route('/most_updated_articles', methods=['GET'])
def most_updated_articles():
    try:
        # Aggregation pipeline
        pipeline = [
            {
                "$match": {
                    "$expr": {"$gt": ["$last_updated", "$published_time"]}  # Only articles updated after publication
                }
            },
            {
                "$addFields": {
                    "update_count": {
                        "$cond": {
                            "if": {"$isArray": "$last_updated"},  # Check if last_updated is an array
                            "then": {"$size": "$last_updated"},  # Count the number of updates (array size)
                            "else": 1  # If not an array, but updated, count as 1 update
                        }
                    }
                }
            },
            {
                "$match": {
                    "update_count": {"$gt": 0}  # Ensure articles have been updated at least once
                }
            },
            {
                "$sort": {"update_count": -1}  # Sort articles by number of updates (most to least)
            },
            {
                "$limit": 10  # Limit to the top 10 most updated articles
            },
            {
                "$project": {
                    "_id": 0,
                    "title": 1,
                    "update_count": 1,  # Include the number of updates
                    "last_updated": 1  # Optionally include the last updated time(s)
                }
            }
        ]

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Check if no articles were found
        if not result:
            return jsonify({"message": "No updated articles found."}), 404

        return jsonify(result)

    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500



# 32 articles last x hours by specific date
@app.route('/articles_last_X_hours/<int:x>/<string:date>', methods=['GET'])
def articles_last_X_hours_on_date(x, date):
    try:

        base_date = datetime.strptime(date, '%Y-%m-%d')


        start_time = base_date
        end_time = start_time + timedelta(hours=x)


        pipeline = [
            {
                "$addFields": {
                    "published_time": {
                        "$dateFromString": {
                            "dateString": "$published_time",
                            "onError": None
                        }
                    }
                }
            },
            {
                "$match": {
                    "published_time": {
                        "$gte": start_time,
                        "$lt": end_time
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "title": 1,
                    "published_time": 1
                }
            },
            {
                "$sort": {
                    "published_time": -1
                }
            }
        ]


        result = list(collection.aggregate(pipeline))


        if not result:
            return jsonify({"message": f"No articles found in the last {x} hours from {date}."}), 404

        return jsonify(result)

    except ValueError:
        return jsonify({"error": "Invalid date format. Please use yyyy-mm-dd format."}), 400
    except OperationFailure as e:
        return jsonify({"error": "MongoDB operation failed: " + str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred: " + str(e)}), 500


# 33. Articles by Keyword Length
@app.route('/articles_by_keyword_length', methods=['GET'])
def articles_by_keyword_length():
    pipeline = [
        {"$project": {"keyword_length": {"$size": "$keywords"}}},
        {"$group": {"_id": "$keyword_length", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = list(collection.aggregate(pipeline))
    return jsonify(result)

# 34. Article with Longest Title
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


# 35. Article with Shortest Title
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


# Route to get articles by sentiment (positive, negative, or neutral)
@app.route('/articles_by_sentiment/<sentiment>', methods=['GET'])
def get_articles_by_sentiment(sentiment):
    try:
        # Query articles based on sentiment
        articles = collection.find({'sentiment': sentiment})

        # Prepare response with article data
        result = []
        for article in articles:
            result.append({
                'title': article.get('title', ''),
                'url': article.get('url', ''),
                'published_time': article.get('published_time', ''),
                'sentiment': article.get('sentiment', '')
            })

        # Return the result as a JSON response
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Endpoint to query articles by person
@app.route('/articles_by_person/<person_name>', methods=['GET'])
def get_articles_by_person(person_name):
    # Query MongoDB for articles that mention the specified person
    query = {"entities.PER": {"$in": [person_name]}}

    # Fetch matching articles from the collection
    articles = collection.find(query, {"postid": 1, "title": 1, "published_time": 1, "entities.PER": 1})

    # Format the results to include only postid, title, publication time, and the person queried
    results = []
    for article in articles:
        results.append({
            "postid": article.get("postid"),
            "title": article.get("title"),
            "published_time": article.get("published_time"),
            "person_name": person_name
        })

    # Return the result as JSON
    return jsonify(results), 200

# endpoint entity loc
@app.route('/articles_by_location/<location_name>', methods=['GET'])
def get_articles_by_location(location_name):
    # Query MongoDB for articles that mention the specified location
    query = {"entities.LOC": {"$in": [location_name]}}

    # Fetch matching articles from the collection
    articles = collection.find(query, {"postid": 1, "title": 1, "published_time": 1, "entities.LOC": 1})

    # Format the results to include only postid, title, publication time, and the location queried
    results = []
    for article in articles:
        results.append({
            "postid": article.get("postid"),
            "title": article.get("title"),
            "published_time": article.get("published_time"),
            "location_name": location_name
        })

    # Return the result as JSON
    return jsonify(results), 200


# endpoint entity org
@app.route('/articles_by_organization/<organization_name>', methods=['GET'])
def get_articles_by_organization(organization_name):
    # Query MongoDB for articles that mention the specified organization
    query = {"entities.ORG": {"$in": [organization_name]}}

    # Fetch matching articles from the collection
    articles = collection.find(query, {"postid": 1, "title": 1, "published_time": 1, "entities.ORG": 1})

    # Format the results to include only postid, title, publication time, and the organization queried
    results = []
    for article in articles:
        results.append({
            "postid": article.get("postid"),
            "title": article.get("title"),
            "published_time": article.get("published_time"),
            "organization_name": organization_name
        })

    # Return the result as JSON
    return jsonify(results), 200

#top entities endpoint
@app.route('/top_entities', methods=['GET'])
def get_top_entities():
    # Aggregation pipeline to find top 10 persons (PER)
    top_persons_pipeline = [
        {"$unwind": "$entities.PER"},
        {"$group": {"_id": "$entities.PER", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]

    # Aggregation pipeline to find top 10 locations (LOC)
    top_locations_pipeline = [
        {"$unwind": "$entities.LOC"},
        {"$group": {"_id": "$entities.LOC", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]

    # Aggregation pipeline to find top 10 organizations (ORG)
    top_organizations_pipeline = [
        {"$unwind": "$entities.ORG"},
        {"$group": {"_id": "$entities.ORG", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]

    # Execute the pipelines
    top_persons = list(collection.aggregate(top_persons_pipeline))
    top_locations = list(collection.aggregate(top_locations_pipeline))
    top_organizations = list(collection.aggregate(top_organizations_pipeline))

    # Format the results
    results = {
        "top_persons": [{"name": person["_id"], "count": person["count"]} for person in top_persons],
        "top_locations": [{"name": location["_id"], "count": location["count"]} for location in top_locations],
        "top_organizations": [{"name": organization["_id"], "count": organization["count"]} for organization in
                              top_organizations]
    }

    # Return the result as JSON
    return jsonify(results), 200


def get_sentiment_trends():
    # Fetching articles with sentiment and publication time
    articles = collection.find({}, {"published_time": 1, "sentiment": 1})
    data = list(articles)

    # Create DataFrame
    df = pd.DataFrame(data)

    # Convert 'published_time' to datetime format
    df['published_time'] = pd.to_datetime(df['published_time'])

    # Group by month and sentiment to count occurrences
    df['month'] = df['published_time'].dt.to_period('M')
    df_grouped = df.groupby(['month', 'sentiment']).size().unstack(fill_value=0)

    # Convert 'month' period to string format
    df_grouped.index = df_grouped.index.astype(str)

    # Convert DataFrame to JSON-friendly format
    sentiment_trends = df_grouped.reset_index().to_dict(orient='records')

    return sentiment_trends


@app.route('/sentiment_trend', methods=['GET'])
def get_sentiment_trend():
    try:
        sentiment_trends = get_sentiment_trends()
        return jsonify(sentiment_trends)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def get_keyword_trends():
    # Fetching articles with keywords and publication time
    articles = collection.find({}, {"published_time": 1, "keywords": 1})
    data = list(articles)

    # Create DataFrame
    df = pd.DataFrame(data)

    # Convert 'published_time' to datetime format
    df['published_time'] = pd.to_datetime(df['published_time'])

    # Explode keywords list to create one row per keyword
    df = df.explode('keywords')

    # Group by month and keyword to count occurrences
    df['month'] = df['published_time'].dt.to_period('M')
    df_grouped = df.groupby(['month', 'keywords']).size().unstack(fill_value=0)

    # Convert 'month' period to string format
    df_grouped.index = df_grouped.index.astype(str)

    # Get top 10 keywords
    top_keywords = df_grouped.sum().nlargest(10).index

    # Filter DataFrame to include only top keywords
    df_grouped_top = df_grouped[top_keywords]

    # Convert DataFrame to JSON-friendly format
    keyword_trends = df_grouped_top.reset_index().to_dict(orient='records')

    return keyword_trends
#Keyword trend Endpoint
@app.route('/keyword_trend', methods=['GET'])
def get_keyword_trend():
    try:
        keyword_trends = get_keyword_trends()
        return jsonify(keyword_trends)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def get_entity_trends(entity_type):
    # Fetching articles with entities (PER, LOC, ORG) and publication time
    articles = collection.find({}, {"published_time": 1, "entities": 1})
    data = list(articles)

    if not data:
        return []

    # Create DataFrame
    df = pd.DataFrame(data)

    # Convert 'published_time' to datetime format
    df['published_time'] = pd.to_datetime(df['published_time'])

    # Check if 'entities' field exists and extract the entity type
    if 'entities' not in df.columns or not df['entities'].apply(lambda x: isinstance(x, dict)).all():
        raise ValueError("Entities field is missing or not in expected format.")

    # Extract entities into separate rows
    df[entity_type] = df['entities'].apply(lambda x: x.get(entity_type, []) if isinstance(x, dict) else [])
    df_entity = df.explode(entity_type)

    # Check if there are no entities of the specified type
    if df_entity.empty:
        return []

    # Add 'month' column for grouping
    df_entity['month'] = df_entity['published_time'].dt.to_period('M').astype(str)

    # Group by month and entity to count occurrences
    df_grouped = df_entity.groupby(['month', entity_type]).size().unstack(fill_value=0)

    # Get top 10 entities
    top_entities = df_grouped.sum().nlargest(10).index

    # Filter DataFrame to include only top entities
    df_grouped_top = df_grouped[top_entities]

    # Convert DataFrame to JSON-friendly format
    entity_trends = df_grouped_top.reset_index().to_dict(orient='records')

    return entity_trends

@app.route('/entity_trend/person', methods=['GET'])
def get_person_trend():
    try:
        person_trends = get_entity_trends('PER')
        return jsonify(person_trends)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/entity_trend/location', methods=['GET'])
def get_location_trend():
    try:
        location_trends = get_entity_trends('LOC')
        return jsonify(location_trends)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/entity_trend/organization', methods=['GET'])
def get_organization_trend():
    try:
        organization_trends = get_entity_trends('ORG')
        return jsonify(organization_trends)
    except Exception as e:
        return jsonify({'error': str(e)}), 500



def get_top_articles_by_sentiment(sentiment, limit=10):
    try:
        # Query articles based on sentiment, sort by score
        articles = collection.find({'sentiment': sentiment}).sort('sentiment_score', -1).limit(limit)

        # Prepare response with article data
        result = []
        for article in articles:
            result.append({
                'title': article.get('title', ''),
                'url': article.get('url', ''),
                'published_time': article.get('published_time', ''),
                'sentiment': article.get('sentiment', ''),
                'sentiment_score': article.get('sentiment_score', '')  # Assuming sentiment_score exists
            })

        return result

    except Exception as e:
        return {"error": str(e)}

@app.route('/most_positive_articles', methods=['GET'])
def get_most_positive_articles():
    try:
        positive_articles = get_top_articles_by_sentiment('positive')
        return jsonify(positive_articles), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/most_negative_articles', methods=['GET'])
def get_most_negative_articles():
    try:
        negative_articles = get_top_articles_by_sentiment('negative')
        return jsonify(negative_articles), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/most_neutral_articles', methods=['GET'])
def get_most_neutral_articles():
    try:
        neutral_articles = get_top_articles_by_sentiment('neutral')
        return jsonify(neutral_articles), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500





if __name__ == '__main__':
    app.run(debug=True)