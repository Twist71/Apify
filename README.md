A robust solution for automated collection, enrichment, and storage of Facebook page content.

Overview
This project provides a complete system for monitoring Facebook pages and collecting new posts in real-time. It uses Apify's Facebook scraping actor in combination with MongoDB for data storage, creating a reliable pipeline for social media data collection.

Features:
Automated Facebook Content Collection: Continuously monitors Facebook pages for new posts
Incremental Data Gathering: Avoids duplicates by tracking timestamps of collected content
Metadata Enrichment: Automatically tags content with source information
MongoDB Integration: Stores structured data directly to MongoDB collections
Scheduled Execution: Configurable polling frequency for different monitoring needs
Detailed Logging: Comprehensive logging of activities and errors
Connection Validation: Verifies database connectivity before operation
Formatted Output: Clear, readable summaries of scraping results

Prerequisites:
Python 3.7+
MongoDB database
Apify API token
