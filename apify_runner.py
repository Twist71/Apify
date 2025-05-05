import json
import os
import time
import requests
import sys
from contextlib import redirect_stdout, contextmanager
from datetime import datetime, timezone
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId
import traceback


# Custom JSON Encoder for MongoDB ObjectId
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)


@contextmanager
def suppress_stdout():
    """Context manager to temporarily suppress stdout output"""
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


# Load environment variables from .env file
load_dotenv()

# Get database configuration from environment
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "web_listener")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "articles")

# Get APIFY token from environment
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
if not APIFY_TOKEN:
    raise ValueError("API token not found. Please ensure APIFY_TOKEN is set in your .env file.")

# Test database connection during initialization
try:
    print("[INFO] Starting Apify runner script...")
    print("[INFO] Testing database connection...")
    # MongoDB setup
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Force a connection test by requesting server info
    client.server_info()
    print("[INFO] Database connection successful!")
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
except Exception as e:
    print(f"[ERROR] Database connection failed: {str(e)}")
    print(traceback.format_exc())
    raise


def load_config(path="config.json"):
    """Load configuration from a JSON file."""
    with open(path) as f:
        return json.load(f)


def load_state(path):
    """Load the last timestamp from a state file."""
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"lastTimestamp": ""}


def save_state(path, timestamp):
    """Save the current timestamp to the state file."""
    with open(path, "w") as f:
        json.dump({"lastTimestamp": timestamp}, f, indent=2)


def run_actor_and_get_items(actor_id, token, actor_input):
    """Run the Apify actor and retrieve items."""
    url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items?token={token}&clean=true&format=json"

    print(f"[INFO] Sending request to URL: {url}")

    start_time = time.time()

    # Use context manager to suppress stdout during API call
    with suppress_stdout():
        res = requests.post(url, json=actor_input, timeout=600)

    elapsed_time = time.time() - start_time
    print(f"[INFO] Response time: {elapsed_time:.2f} seconds")

    if res.status_code != 200:
        print("[ERROR] Response Status Code:", res.status_code)
        with suppress_stdout():
            error_text = res.text
        print("[ERROR] Response Error: Check logs for details")

        # Write error to log file instead of printing
        with open("error_log.txt", "a") as f:
            f.write(f"[{datetime.now()}] API Error: {error_text}\n")

    res.raise_for_status()  # Ensure the request was successful

    # Process the JSON in a suppressed context
    with suppress_stdout():
        response_data = res.json()

        # Extract items from the response
        if isinstance(response_data, list):
            items = response_data
        elif isinstance(response_data, dict):
            items = response_data.get("data", [])
        else:
            items = []

    if not items:
        print("[ERROR] Unexpected response structure!")

    return items


def extract_source_metadata(config):
    """Extract source metadata from configuration."""
    # Get default metadata
    default_metadata = config.get("source_metadata", {
        "source_type": "facebook",
        "post_type": "post",
        "source_name": "Facebook Page",
        "category": "Social Media"
    })

    # Get the start URLs from config
    start_urls = [url_obj["url"] if isinstance(url_obj, dict) else url_obj
                  for url_obj in config["inputTemplate"].get("startUrls", [])]

    # Use the first URL to determine the source name if not specified
    if start_urls and not default_metadata.get("source_name"):
        url = start_urls[0]
        if "facebook.com" in url:
            page_id = url.split('facebook.com/')[-1].strip('/')
            default_metadata["source_name"] = page_id.replace(".", " ").title()

    return default_metadata, start_urls


def save_items_to_db(items, metadata):
    """Save items to the database with source metadata."""
    success_count = 0
    for item in items:
        try:
            # Convert ObjectId to string if present
            for key, value in item.items():
                if isinstance(value, dict) and "_id" in value:
                    value["_id"] = str(value["_id"])

            # Add source metadata to item
            item["source_type"] = metadata.get("source_type", "facebook")
            item["post_type"] = metadata.get("post_type", "post")
            item["source_name"] = metadata.get("source_name", item.get("pageNameSource", "Facebook Page"))
            item["category"] = metadata.get("category", "Social Media")

            # Insert directly into MongoDB collection
            with suppress_stdout():
                collection.insert_one(item)
            success_count += 1
        except Exception as e:
            print(f"[WARN] Failed to insert item: {str(e)}")

    print(f"[INFO] Successfully inserted {success_count} of {len(items)} items into database")
    return success_count


def log_run(data, path="log.jsonl"):
    """Log the status of each run."""
    with open(path, "a") as f:
        f.write(json.dumps(data, cls=MongoJSONEncoder) + "\n")


def display_metadata_summary(url, item_count, metadata, success_count):
    """Display a formatted summary of the metadata and results."""
    print("\n" + "═" * 45)
    print("FACEBOOK SCRAPER RESPONSE")
    print("═" * 45)
    print(f"Status: {'success' if success_count > 0 else 'no new items'}")
    print(f"Source: {metadata.get('source_name', 'Unknown')}")
    print(f"URL: {url}")
    print("\nSource Metadata:")
    print("═" * 45)
    print(f"source_type: {metadata.get('source_type', 'facebook')}")
    print(f"post_type: {metadata.get('post_type', 'post')}")
    print(f"source_name: {metadata.get('source_name', 'Unknown')}")
    print(f"category: {metadata.get('category', 'Social Media')}")
    print("\nRetrieved Items:")
    print("═" * 45)
    print(f"Retrieved {item_count} items")
    if item_count > 0:
        print("\nAll items tagged with:")
        print(f"- source_type: {metadata.get('source_type', 'facebook')}")
        print(f"- post_type: {metadata.get('post_type', 'post')}")
        print(f"- source_name: {metadata.get('source_name', 'Unknown')}")
        print(f"- category: {metadata.get('category', 'Social Media')}")
        print(f"\nSuccessfully inserted {success_count} of {item_count} items into database")
    print("═" * 45 + "\n")


# -------------------------------
# Script starts here
# -------------------------------

# Create data directory if it doesn't exist
os.makedirs("data", exist_ok=True)

# Load configuration from config.json
config = load_config()
actor_id = config["actorId"]
input_template = config["inputTemplate"]
state_file = config.get("stateFile", "state.json")
frequency = config.get("frequency", 900)  # in seconds

# Extract source metadata and URLs
source_metadata, start_urls = extract_source_metadata(config)

print(f"[INFO] Loaded 1 source configuration")

while True:
    state = load_state(state_file)
    last_ts = state.get("lastTimestamp", "")

    # Build input template from config
    actor_input = input_template.copy()

    # Handle timestamp replacement if needed
    actor_input_str = json.dumps(actor_input)
    if "__LAST_TIMESTAMP__" in actor_input_str:
        actor_input = json.loads(actor_input_str.replace("__LAST_TIMESTAMP__", last_ts or ""))

    print(f"\n[RUN] Triggering actor with URL(s): {', '.join(start_urls)} and since: {last_ts or 'N/A'}")
    try:
        items = run_actor_and_get_items(actor_id, APIFY_TOKEN, actor_input)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

        print(f"[INFO] Retrieved {len(items)} items")
        print(f"[INFO] Matched item to source: {source_metadata.get('source_name')}")

        latest_ts = None
        success_count = 0

        if items:
            # Use the first URL for display
            primary_url = start_urls[0] if start_urls else ""

            # Save items to database and get success count
            success_count = save_items_to_db(items, source_metadata)

            # Display formatted metadata summary
            display_metadata_summary(primary_url, len(items), source_metadata, success_count)

            with suppress_stdout():
                latest_ts = items[0].get("timestamp")
            if latest_ts:
                save_state(state_file, latest_ts)
                print(f"[INFO] Updated lastTimestamp to {latest_ts}")
        else:
            print("[INFO] No new items found")
            # Display empty summary for no items
            if start_urls:
                primary_url = start_urls[0]
                display_metadata_summary(primary_url, 0, source_metadata, 0)

        # Save the Apify response to a file with runtime in the filename
        runtime_str = datetime.now().strftime("%I-%M%p").lower()
        response_filename = f"data/apify_response_{runtime_str}.json"

        # Save response data with custom JSON encoder - suppress any console output
        with suppress_stdout(), open(response_filename, "w") as f:
            json.dump(
                {"runAt": datetime.now(timezone.utc).isoformat() + "Z", "data": items},
                f,
                indent=2,
                cls=MongoJSONEncoder
            )
        print(f"[INFO] Apify response saved to {response_filename}")

        log_entry = {
            "runAt": datetime.now(timezone.utc).isoformat() + "Z",
            "runId": run_id,
            "status": "SUCCEEDED",
            "itemCount": len(items),
            "latestTimestamp": latest_ts,
            "error": None
        }
        log_run(log_entry)

    except Exception as e:
        print(f"[ERROR] {e}")
        print(traceback.format_exc())
        log_run({
            "runAt": datetime.now(timezone.utc).isoformat() + "Z",
            "runId": None,
            "status": "ERROR",
            "itemCount": 0,
            "latestTimestamp": None,
            "error": str(e)
        })

    print(f"[SLEEP] Waiting {frequency} seconds until next run...\n")
    time.sleep(frequency)