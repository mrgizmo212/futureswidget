from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import databento as db
from datetime import datetime, timezone
import threading
import logging

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8881",
        "http://localhost",
        "https://futuresfeed.net",  # Add your WordPress domain
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LiveQuoteManager:
    def __init__(self):
        self.latest_quotes = {"ES.c.0": {"error": "Waiting for data..."}}
        self.client = None
        self.running = False

    def start_live_feed(self):
        try:
            logger.info("Connecting to Databento...")
            self.client = db.Live(key="db-eAhRWMKCiJLEpDAk8cvbFSeUWSXCK")
            
            logger.info("Subscribing to ES futures...")
            self.client.subscribe(
                dataset="GLBX.MDP3",
                schema="trades",
                stype_in="continuous",
                symbols=["ES.c.0"]
            )
            
            self.running = True
            logger.info("Feed started")
            
            for record in self.client:
                if not self.running:
                    break
                    
                if isinstance(record, db.TradeMsg):
                    try:
                        ts_event = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
                        
                        quote_data = {
                            "symbol": "ES",
                            "price": float(record.price) / 1e9,
                            "size": int(record.size),
                            "timestamp": ts_event.strftime('%H:%M:%S'),
                            "side": record.side,
                            "date": ts_event.strftime('%Y-%m-%d'),
                            "status": "live"
                        }
                        
                        self.latest_quotes["ES.c.0"] = quote_data
                        logger.info(f"Updated quote: {quote_data}")
                        
                    except Exception as e:
                        logger.error(f"Error processing record: {str(e)}")
                        
        except Exception as e:
            logger.error(f"Feed error: {str(e)}")
            self.latest_quotes["ES.c.0"] = {"error": str(e)}

    def stop_live_feed(self):
        self.running = False
        if self.client:
            self.client.stop()

quote_manager = LiveQuoteManager()

@app.get("/")
async def get_quote():
    return {
        "quotes": quote_manager.latest_quotes,
        "server_time": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    }

def start_feed():
    feed_thread = threading.Thread(target=quote_manager.start_live_feed)
    feed_thread.daemon = True
    feed_thread.start()
    logger.info("Feed thread started")

# Start the feed when app starts
@app.on_event("startup")
async def startup_event():
    start_feed()

# Requirements (save as requirements.txt):
# fastapi
# uvicorn
# databento
