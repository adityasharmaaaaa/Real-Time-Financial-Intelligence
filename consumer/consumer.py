from kafka import KafkaConsumer
import json
from pathlib import Path

existing_ids=set()
DATA_FILE = Path("data/raw/news_validated.jsonl")
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

Consumer= KafkaConsumer(
    topic='news_events',
    bootstrap_servers='localhost:9092',
    group_id='news_consumer_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    exclude_auto_commit=False
)

for message in Consumer:
    news_event = message.value
    if news_event['id'] in existing_ids:
        print(f"Duplicate event with id {news_event['id']} skipped.")
        continue
    
    existing_ids.add(news_event['id'])
    with DATA_FILE.open("a") as f:
        f.write(json.dumps(news_event) + "\n")
    
    Consumer.commit()
    
    print(f"Event with id {news_event['id']} processed and stored.")