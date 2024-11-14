from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import databento as db
from datetime import datetime, timezone
import threading
import logging
import os
from typing import Dict, Any
from threading import Lock
from pytz import timezone as pytz_timezone

app = FastAPI(title="Futures Quotes API")

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
        # Define supported tickers
        self.SUPPORTED_TICKERS = ["ES.c.0", "NQ.c.0", "RTY.c.0", "BTC.c.0"]
        
        # Initialize quotes dictionary for all tickers
        self.latest_quotes = {
            ticker: {"error": "Initializing feed..."} 
            for ticker in self.SUPPORTED_TICKERS
        }
        
        self.client = None
        self.running = False
        self.last_updates = {ticker: datetime.now(timezone.utc) for ticker in self.SUPPORTED_TICKERS}
        self.lock = Lock()
        self.instrument_map = {}
        
        # Initialize timezone
        self.eastern_tz = pytz_timezone('America/New_York')

    def start_live_feed(self):
        try:
            logger.info("Connecting to Databento...")
            self.client = db.Live(key="db-eAhRWMKCiJLEpDAk8cvbFSeUWSXCK")
            
            logger.info("Subscribing to futures...")
            self.client.subscribe(
                dataset="GLBX.MDP3",
                schema="trades",
                stype_in="continuous",
                symbols=self.SUPPORTED_TICKERS
            )
            
            # Initialize symbol mapping
            def handle_mapping(msg):
                if isinstance(msg, db.SymbolMappingMsg):
                    self.instrument_map[msg.instrument_id] = msg.stype_in_symbol
                    logger.info(f"Mapped instrument {msg.instrument_id} to {msg.stype_in_symbol}")

            self.client.add_callback(handle_mapping)
            
            self.running = True
            logger.info("Feed started successfully")
            
            for record in self.client:
                if not self.running:
                    break
                    
                if isinstance(record, db.TradeMsg):
                    try:
                        # Get the ticker symbol from the instrument map
                        ticker = self.instrument_map.get(record.instrument_id)
                        
                        if not ticker or ticker not in self.SUPPORTED_TICKERS:
                            continue
                        
                        # Convert UTC timestamp to Eastern Time
                        ts_event = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
                        ts_event_eastern = ts_event.astimezone(self.eastern_tz)
                        
                        quote_data = {
                            "symbol": ticker.split('.')[0],  # Remove the .c.0 suffix
                            "price": float(record.price) / 1e9,
                            "size": int(record.size),
                            "timestamp": ts_event_eastern.strftime('%I:%M:%S %p'),  # 12-hour format with AM/PM
                            "side": record.side,
                            "date": ts_event_eastern.strftime('%Y-%m-%d'),
                            "status": "live"
                        }
                        
                        with self.lock:
                            self.latest_quotes[ticker] = quote_data
                            self.last_updates[ticker] = datetime.now(timezone.utc)
                        
                        logger.info(f"Updated quote for {ticker}: {quote_data}")
                        
                    except Exception as e:
                        logger.error(f"Error processing record: {str(e)}")
                        
        except Exception as e:
            error_msg = f"Feed error: {str(e)}"
            logger.error(error_msg)
            with self.lock:
                for ticker in self.SUPPORTED_TICKERS:
                    self.latest_quotes[ticker] = {"error": error_msg}

    def stop_live_feed(self):
        self.running = False
        if self.client:
            self.client.stop()
        logger.info("Feed stopped")

    def is_feed_healthy(self, ticker: str = None) -> bool:
        """Check if feed is healthy and updating"""
        if not self.running:
            return False
            
        current_time = datetime.now(timezone.utc)
        
        if ticker:
            # Check specific ticker
            time_since_update = (current_time - self.last_updates.get(ticker, current_time)).seconds
            return time_since_update < 30
        else:
            # Check all tickers
            return all(
                (current_time - update_time).seconds < 30
                for update_time in self.last_updates.values()
            )

quote_manager = LiveQuoteManager()

@app.get("/")
async def get_quotes(symbol: str = None) -> Dict[str, Any]:
    """Get the latest futures quotes"""
    current_time = datetime.now(timezone.utc)
    eastern_time = current_time.astimezone(quote_manager.eastern_tz)
    
    if symbol:
        # Convert to continuous contract format if needed
        ticker = f"{symbol.upper()}.c.0"
        if ticker not in quote_manager.SUPPORTED_TICKERS:
            raise HTTPException(status_code=400, detail="Invalid symbol")
            
        if not quote_manager.is_feed_healthy(ticker):
            return {
                "quotes": {ticker: {"error": "Feed unavailable - reconnecting..."}},
                "server_time": eastern_time.strftime('%Y-%m-%d %I:%M:%S %p %Z'),
                "status": "unhealthy"
            }
            
        return {
            "quotes": {ticker: quote_manager.latest_quotes[ticker]},
            "server_time": eastern_time.strftime('%Y-%m-%d %I:%M:%S %p %Z'),
            "status": "healthy"
        }
    
    # Return all quotes if no symbol specified
    if not quote_manager.is_feed_healthy():
        logger.warning("Feed appears to be stale or not running")
        return {
            "quotes": {
                ticker: {"error": "Feed unavailable - reconnecting..."}
                for ticker in quote_manager.SUPPORTED_TICKERS
            },
            "server_time": eastern_time.strftime('%Y-%m-%d %I:%M:%S %p %Z'),
            "status": "unhealthy"
        }
    
    return {
        "quotes": quote_manager.latest_quotes,
        "server_time": eastern_time.strftime('%Y-%m-%d %I:%M:%S %p %Z'),
        "status": "healthy"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    current_time = datetime.now(timezone.utc)
    eastern_time = current_time.astimezone(quote_manager.eastern_tz)
    
    health_status = {
        ticker: quote_manager.is_feed_healthy(ticker)
        for ticker in quote_manager.SUPPORTED_TICKERS
    }
    
    return {
        "status": "healthy" if all(health_status.values()) else "unhealthy",
        "ticker_status": health_status,
        "last_updates": {
            ticker: update_time.astimezone(quote_manager.eastern_tz).strftime('%Y-%m-%d %I:%M:%S %p %Z')
            for ticker, update_time in quote_manager.last_updates.items()
        },
        "current_time": eastern_time.strftime('%Y-%m-%d %I:%M:%S %p %Z'),
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
    quote_manager.stop_live_feed()
    logger.info("Application shutting down")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="info")
