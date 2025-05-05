import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get MongoDB connection details from environment
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME', 'web_listener')
COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'Posts')
COMMENTS_COLLECTION = os.getenv('COMMENTS_COLLECTION', 'facebook_comments')