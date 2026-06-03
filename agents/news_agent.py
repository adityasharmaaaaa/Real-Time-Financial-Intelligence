import asyncio
import aiohttp
from uuid import uuid4
from datetime import datetime,timezone
import random

API_URL = "http://localhost:8001/ingest"

SOURCES=["Bloomberg","Reuters","CNBC","Financial Times","Wall Street Journal"]
TICKERS=["AAPL","GOOGL","AMZN","MSFT","TSLA","JPM","NVDA","GS"]
EVENTS = [
    "soars after surprise earnings beat.", 
    "plummets amid new regulatory concerns.", 
    "announces massive breakthrough in AI infrastructure.", 
    "downgraded by major analysts ahead of Q3.", 
    "faces unprecedented supply chain bottlenecks.",
    "reports record-breaking quarterly revenue.",
    "struggles with global chip shortage impacts.",
    "secures landmark deal with major automaker.",
    "hits all-time high on strong market demand."
]

async def fetch_news_event():
    """
    Simulates fetching a real-time news event from an external API.
    In the future, replace this logic with an actual GET request to Finnhub/Alpaca.
    """
    await asyncio.sleep(random.uniform(0.1,1.5))

    ticker = random.choice(TICKERS)
    event = random.choice(EVENTS)

    return {
        "id": str(uuid4()),
        "headline": f"{ticker} {event}",
        "source": random.choice(SOURCES),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

async def send_to_pipeline(session:aiohttp.ClientSession,event:dict):
    """Fires the JSON payload to our FASTApi gateway."""
    try:
        async with session.post(API_URL,json=event) as response:
            if response.status == 200:
                print(f"✅ Successfully sent event: {event['headline']}")
            else:
                print(f"❌ Failed to send event: {event['headline']} - Status Code: {response.status}")
    except Exception as e:
        print(f"💥 Error sending event: {event['headline']} - Error: {str(e)}")

async def main():
    print(f"🚀 Starting High-Frequency Data Agent targeting {API_URL}...")
    print("Press Ctrl+C to stop.\n" + "-"*50)

    async with aiohttp.ClientSession() as session:
        while True:
            news_event = await fetch_news_event()
            asyncio.create_task(send_to_pipeline(session, news_event))
            await asyncio.sleep(0.5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Agent stopped gracefully. Data stream halted.")

