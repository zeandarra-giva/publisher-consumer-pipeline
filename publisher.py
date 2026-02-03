# connect to Redis and pushes the work into a list
import json
import redis
import os
import time

# Connect to Redis
# Read JSON file
# Push article to Redis List

# We use 'redis' as the hostname because in Docker, services find each other by name.
r=redis.Redis(host='redis',port=6379, decode_responses=True)

def publish_tasks():
    # Read the JSON file
    with open('articles.json','r') as f:
        data=json.load(f)
    
    articles=data['articles']

    print(f"Found {len(articles)} articles to publish.")

    for article in articles:
        # Push articles to Redis List
        #  We use lpush to add to the queue named articles_queue
        r.lpush('article_queue',json.dumps(article))
        print(f"Published: {article['url']}")
    

if __name__=="__main__":
    time.sleep(5)
    publish_tasks()