# Connect to Redis
# Pull articles from Redis List
# Fetch Website
# scrape data and store
# connect to mongodb
# save to mongodb

import redis
import json
import time
import requests


from pymongo import MongoClient
from bs4 import BeautifulSoup
from textblob import TextBlob

# Connection
# Redis connection
r=redis.Redis(host='redis',port=6379,decode_responses=True)

# MongoDB connection
mongo_client=MongoClient("mongodb://mongo:27017")

db=mongo_client["news_db"]
collection=db["articles"]

# Logic
# 1. Scrape Articles
# 2. Perform Sentimental Analysis
def process_articles(article_data):
    url=article_data['url']
    print(f"Processing: {url}...")

    try:
        # Scraping
        response=requests.get(url,timeout=10)
        response.raise_for_status() #raises error if bad status (404,500)

        soup=BeautifulSoup(response.content,'html.parser')
        title=soup.title.string if soup.title else "No Title Found"

        # Sentiment Analysis
        # Analyze the title to see if it sounds positive or negative
        blob = TextBlob(title)
        sentiment_score = blob.sentiment.polarity
        sentiment_label = "Neutral"
        if sentiment_score > 0.1: sentiment_label = "Positive"
        elif sentiment_score < -0.1: sentiment_label = "Negative"       

        # Prepare document for MongoDB
        document = {
            "metadata": article_data,
            "scraped_data": {
                "title": title.strip(),
                "sentiment": sentiment_label,
                "sentiment_score": sentiment_score
            }
        }

        # Save to database
        # We use the URL as the unique key. If it exists, update it. If not, insert it.
        collection.update_one(
            {"metadata.url": url}, 
            {"$set": document}, 
            upsert=True
        )
        print(f"Saved: {title} ({sentiment_label})")
        
    except Exception as e:
        print(f"Error processing {url}: {e}")

def run_consumer():
    print("Consumer Started. Waiting for tasks...")
    while True:
        # Pop from Redis
        # 'brpop' waits until there is something in the list.
        # It returns a tuple: (queue_name, data)
        task = r.brpop('article_queue', timeout=0)

        # Process the articles pop from redis
        if task:
            # task[1] contains the actual JSON string
            article_data = json.loads(task[1])
            process_article(article_data)

if __name__=="main":
    # Wait briefly for Redis/Mongo to spin up in Docker
    time.sleep(5)
    run_consumer()


