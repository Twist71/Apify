from pymongo import MongoClient
from db_config import MONGO_URI, DB_NAME, COLLECTION_NAME, COMMENTS_COLLECTION

print("[DB] Initializing MongoDB connection...")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
comments_collection = db[COMMENTS_COLLECTION]

print(f"[DB] Connected to DB: {DB_NAME}, Collection: {COLLECTION_NAME}")

def insert_post(post_data):
    return collection.insert_one(post_data)

def find_posts(query={}, limit=50):
    return list(collection.find(query).limit(limit))

def insert_article(article_data):
    return collection.insert_one(article_data)

def find_articles(query={}, limit=50):
    return list(collection.find(query).limit(limit))

def insert_comment(comment_data):
    return comments_collection.insert_one(comment_data)

def create_indices():
    collection.create_index("url", unique=True)
    collection.create_index("post_type")
    collection.create_index("page")
    collection.create_index("matched_keywords")
    collection.create_index("scraped_at")
    collection.create_index("source_type")
    collection.create_index("source_name")
    collection.create_index("published_date")

def create_comment_indices():
    comments_collection.create_index("comment_id", unique=True)
    comments_collection.create_index("post_id")
    comments_collection.create_index("from_id")
    comments_collection.create_index("created_time")
    comments_collection.create_index("matched_keywords")

def get_article_stats():
    stats = {
        "total_count": collection.count_documents({"source_type": "article"}),
    }

    oldest = list(collection.find({"source_type": "article", "published_date": {"$exists": True}})
                            .sort("published_date", 1).limit(1))
    newest = list(collection.find({"source_type": "article", "published_date": {"$exists": True}})
                            .sort("published_date", -1).limit(1))

    if oldest and newest:
        stats["oldest_date"] = oldest[0].get("published_date")
        stats["newest_date"] = newest[0].get("published_date")

    pipeline = [
        {"$match": {"source_type": "article"}},
        {"$group": {"_id": "$source_name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    stats["sources"] = list(collection.aggregate(pipeline))

    return stats
