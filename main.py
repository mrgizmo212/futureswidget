from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import databento as db
from datetime import datetime, timezone
import threading
import logging
import logging.handlers
import os
from typing import Dict, Any, Optional, List
from threading import Lock
from pytz import timezone as pytz_timezone
import time
import traceback
from enum import Enum
from dataclasses import dataclass
from contextlib import contextmanager
from contextlib import asynccontextmanager

# Configure logging with rotation
LOG_FILENAME = "futures_quotes.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_FILENAME, maxBytes=5*1024*1024, backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FeedState(Enum):
    """Enum representing possible states of the data feed"""
    INITIALIZING = "initializing"
    CONNECTING = "connecting"
    RUNNING = "running"
    RECONNECTING = "reconnecting"
    ERROR = "error"
    STOPPED = "stopped"

@dataclass
class QuoteData:
    """Data class for storing quote information"""
    symbol: str
    price: float
    size: int
    timestamp: str
    side: str
    date: str
    status: str

class FeedHealthMonitor:
    """Monitors the health of the data feed including heartbeats and updates"""
    
    def __init__(self):
        self.last_heartbeat = datetime.now(timezone.utc)
        self.last_updates: Dict[str, datetime] = {}
        self.lock = Lock()
        
    def update_heartbeat(self):
        """Update the last heartbeat timestamp"""
        with self.lock:
            self.last_heartbeat = datetime.now(timezone.utc)
            
    def update_ticker(self, ticker: str):
        """Update the last update timestamp for a specific ticker"""
        with self.lock:
            self.last_updates[ticker] = datetime.now(timezone.utc)
            
    def is_healthy(self, ticker: Optional[str] = None) -> bool:
        """Check if the feed is healthy for a specific ticker or overall"""
        current_time = datetime.now(timezone.utc)
        
        with self.lock:
            # Check heartbeat health - 60 second timeout
            heartbeat_age = (current_time - self.last_heartbeat).total_seconds()
            if heartbeat_age > 60:
                return False
                
            if ticker:
                if ticker not in self.last_updates:
                    return False
                time_since_update = (current_time - self.last_updates[ticker]).total_seconds()
                timeout = 60  # 60 second timeout for all tickers
                return time_since_update < timeout
            
            # Check all tickers with 60 second timeout
            return all(
                (current_time - update_time).total_seconds() < 60
                for update_time in self.last_updates.values()
            )

class LiveQuoteManager:
    """Manages real-time quotes from Databento, including feed health and reconnection"""
    
    def __init__(self):
        self.SUPPORTED_TICKERS: List[str] = ["ES.c.0", "NQ.c.0", "RTY.c.0", "BTC.c.0"]
        self.latest_quotes = {
            ticker: {"error": "Initializing feed..."} 
            for ticker in self.SUPPORTED_TICKERS
        }
        self.client: Optional[db.Live] = None
        self.running = False
        self.lock = Lock()
        self.state_lock = Lock()
        self.feed_state = FeedState.INITIALIZING
        self.health_monitor = FeedHealthMonitor()
        self.instrument_map: Dict[int, str] = {}
        self.eastern_tz = pytz_timezone('America/New_York')
        self.connection_attempts = 0
        self.MAX_RECONNECT_ATTEMPTS = 5
        self.RECONNECT_DELAY = 5
        self.DATABENTO_KEY = "db-eAhRWMKCiJLEpDAk8cvbFSeUWSXCK"  # Databento key directly in code

    @contextmanager
    def state_transition(self, new_state: FeedState):
        """Context manager for handling state transitions"""
        try:
            with self.state_lock:
                old_state = self.feed_state
                self.feed_state = new_state
                logger.info(f"Feed state transition: {old_state.value} -> {new_state.value}")
            yield
        except Exception as e:
            logger.error(f"Error in state {new_state.value}: {str(e)}")
            with self.state_lock:
                self.feed_state = FeedState.ERROR
            raise

    def get_state(self) -> FeedState:
        """Get the current feed state"""
        with self.state_lock:
            return self.feed_state

    def process_message(self, record: db.DBNRecord):
        """Process incoming messages based on their type with proper type checking"""
        try:
            if isinstance(record, db.SystemMsg):
                if hasattr(record, 'is_heartbeat') and record.is_heartbeat():
                    self.health_monitor.update_heartbeat()
                    logger.debug("Heartbeat received")
            elif isinstance(record, db.SymbolMappingMsg):
                if hasattr(record, 'stype_in_symbol'):
                    with self.lock:
                        self.instrument_map[record.instrument_id] = record.stype_in_symbol
                        logger.info(f"Mapped instrument {record.instrument_id} to {record.stype_in_symbol}")
            elif isinstance(record, db.TradeMsg):
                self.process_trade(record)
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}\n{traceback.format_exc()}")

    def process_trade(self, record: db.TradeMsg):
        """Process trade messages and update quotes"""
        try:
            with self.lock:
                ticker = self.instrument_map.get(record.instrument_id)
                
            if not ticker or ticker not in self.SUPPORTED_TICKERS:
                return
                
            ts_event = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
            ts_event_eastern = ts_event.astimezone(self.eastern_tz)
            
            quote_data = QuoteData(
                symbol=ticker.split('.')[0],
                price=float(record.price) / 1e9,
                size=int(record.size),
                timestamp=ts_event_eastern.strftime('%I:%M:%S %p'),
                side=record.side,
                date=ts_event_eastern.strftime('%Y-%m-%d'),
                status="live"
            )
            
            with self.lock:
                self.latest_quotes[ticker] = quote_data.__dict__
            
            self.health_monitor.update_ticker(ticker)
            logger.info(f"Updated quote for {ticker}: {quote_data.__dict__}")
            
        except Exception as e:
            logger.error(f"Error processing trade: {str(e)}\n{traceback.format_exc()}")

    def start_live_feed(self):
        """Start the live data feed with automatic reconnection"""
        self.running = True
        
        while self.running:
            try:
                with self.state_transition(FeedState.CONNECTING):
                    logger.info("Connecting to Databento...")
                    self.client = db.Live(
                        key=self.DATABENTO_KEY,  # Use direct key
                        reconnect_policy="reconnect",
                        heartbeat_interval_s=5,
                        ts_out=True
                    )
                    
                    # Set up message handler
                    self.client.add_callback(self.process_message)
                    
                    logger.info("Subscribing to futures...")
                    self.client.subscribe(
                        dataset="GLBX.MDP3",
                        schema="trades",
                        stype_in="continuous",
                        symbols=self.SUPPORTED_TICKERS
                    )
                    
                with self.state_transition(FeedState.RUNNING):
                    for record in self.client:
                        if not self.running:
                            break
                            
                    logger.info("Feed stream ended")
                    
            except Exception as e:
                logger.error(f"Feed error: {str(e)}\n{traceback.format_exc()}")
                
                with self.state_transition(FeedState.RECONNECTING):
                    self.connection_attempts += 1
                    
                    if self.connection_attempts >= self.MAX_RECONNECT_ATTEMPTS:
                        logger.error("Max reconnection attempts reached")
                        self.running = False
                        break
                        
                    logger.info(f"Attempting to reconnect... ({self.connection_attempts}/{self.MAX_RECONNECT_ATTEMPTS})")
                    time.sleep(self.RECONNECT_DELAY)
                    
        with self.state_transition(FeedState.STOPPED):
            logger.info("Feed stopped")

    def stop_live_feed(self):
        """Stop the live data feed"""
        self.running = False
        if self.client:
            self.client.stop()
        logger.info("Feed stopped")

    def is_feed_healthy(self, ticker: str = None) -> bool:
        """Check if the feed is healthy"""
        return (
            self.get_state() == FeedState.RUNNING 
            and self.health_monitor.is_healthy(ticker)
        )

# Initialize FastAPI application
app = FastAPI(title="Futures Quotes API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://truetradinggroup.com",
        "https://www.truetradinggroup.com",
        "https://futureswidget.onrender.com",
        "http://localhost:8881",
        "http://localhost",
        "http://127.0.0.1:8000",
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
    max_age=3600,
)

# Initialize quote manager
quote_manager = LiveQuoteManager()

@app.get("/")
async def get_quotes(
    request: Request,
    symbol: Optional[str] = None
) -> Dict[str, Any]:
    """Get the latest futures quotes"""
    current_time = datetime.now(timezone.utc)
    eastern_time = current_time.astimezone(quote_manager.eastern_tz)
    
    logger.info(f"Quote request from {request.client.host} for symbol: {symbol}")
    
    if symbol:
        ticker = f"{symbol.upper()}.c.0"
        if ticker not in quote_manager.SUPPORTED_TICKERS:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid symbol. Supported symbols: {[t.split('.')[0] for t in quote_manager.SUPPORTED_TICKERS]}"
            )
            
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
async def health_check() -> Dict[str, Any]:
    """Health check endpoint"""
    current_time = datetime.now(timezone.utc)
    eastern_time = current_time.astimezone(quote_manager.eastern_tz)
    
    health_status = {
        ticker: quote_manager.is_feed_healthy(ticker)
        for ticker in quote_manager.SUPPORTED_TICKERS
    }
    
    return {
        "status": "healthy" if all(health_status.values()) else "unhealthy",
        "feed_state": quote_manager.get_state().value,
        "ticker_status": health_status,
        "current_time": eastern_time.strftime('%Y-%m-%d %I:%M:%S %p %Z'),
        "running": quote_manager.running
    }

def start_feed():
    """Start the feed in a separate thread"""
    feed_thread = threading.Thread(
        target=quote_manager.start_live_feed,
        name="FeedThread",
        daemon=True
    )
    feed_thread.start()
    logger.info("Feed thread started")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for the FastAPI application"""
    # Startup
    logger.info("Starting application...")
    start_feed()
    logger.info("Application started")
    
    yield  # Run the application
    
    # Shutdown
    logger.info("Application shutting down...")
    quote_manager.stop_live_feed()
    logger.info("Application shutdown complete")

# Update FastAPI initialization
app = FastAPI(
    title="Futures Quotes API",
    lifespan=lifespan
)
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
