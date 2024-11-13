from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import databento as db
from datetime import datetime, timezone
import threading
import logging
import os
from typing import Dict, Any

app = FastAPI(title="ES Futures Quote API")

# Enhanced CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://truetradinggroup.com",
        "https://www.truetradinggroup.com",
        "http://localhost:8881",
        "http://localhost",
        "http://127.0.0.1:8000",
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
    max_age=3600,  # Cache preflight requests for 1 hour
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LiveQuoteManager:
    def __init__(self):
        self.latest_quotes = {"ES.c.0": {"error": "Initializing feed..."}}
        self.client = None
        self.running = False
        self.last_update = datetime.now(timezone.utc)

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
            logger.info("Feed started successfully")
            
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
                        self.last_update = datetime.now(timezone.utc)
                        logger.info(f"Updated quote: {quote_data}")
                        
                    except Exception as e:
                        logger.error(f"Error processing record: {str(e)}")
                        
        except Exception as e:
            error_msg = f"Feed error: {str(e)}"
            logger.error(error_msg)
            self.latest_quotes["ES.c.0"] = {"error": error_msg}

    def stop_live_feed(self):
        self.running = False
        if self.client:
            self.client.stop()
        logger.info("Feed stopped")

    def is_feed_healthy(self) -> bool:
        """Check if feed is healthy and updating"""
        if not self.running:
            return False
        time_since_update = (datetime.now(timezone.utc) - self.last_update).seconds
        return time_since_update < 30  # Consider feed healthy if updated within 30 seconds

quote_manager = LiveQuoteManager()

@app.get("/")
async def get_quote() -> Dict[str, Any]:
    """Get the latest ES futures quote"""
    if not quote_manager.is_feed_healthy():
        logger.warning("Feed appears to be stale or not running")
        return {
            "quotes": {"ES.c.0": {"error": "Feed unavailable - reconnecting..."}},
            "server_time": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
            "status": "unhealthy"
        }
    
    return {
        "quotes": quote_manager.latest_quotes,
        "server_time": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
        "status": "healthy"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy" if quote_manager.is_feed_healthy() else "unhealthy",
        "last_update": quote_manager.last_update.strftime('%Y-%m-%d %H:%M:%S UTC'),
        "running": quote_manager.running
    }

def start_feed():
    """Start the feed in a separate thread"""
    feed_thread = threading.Thread(target=quote_manager.start_live_feed)
    feed_thread.daemon = True
    feed_thread.start()
    logger.info("Feed thread started")

# Start the feed when app starts
@app.on_event("startup")
async def startup_event():
    """Initialize the feed on startup"""
    start_feed()
    logger.info("Application started")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown"""
    quote_manager.stop_feed()
    logger.info("Application shutting down")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="info")
