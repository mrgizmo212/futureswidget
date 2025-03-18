import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Any

import databento as db
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("futures-data-service")

# Initialize FastAPI app
app = FastAPI(title="TTG Futures Data Service")

# Set up security - use environment variable for the token
# IMPORTANT: Set API_TOKEN environment variable to a secure value in .env file or deployment environment
security = HTTPBearer()
API_TOKEN = os.getenv("API_TOKEN")
if not API_TOKEN:
    logger.warning("API_TOKEN environment variable not set. Authentication will be disabled.")
    API_TOKEN = None

# Add CORS middleware to allow requests from WordPress
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Databento API credentials
DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
if not DATABENTO_API_KEY:
    logger.warning("DATABENTO_API_KEY environment variable not set. Will display N/A values.")

# Define contract symbols for futures data - Databento specific format
# Format: Symbol.c.N for continuous contracts (c.0 is front month)
INDEX_FUTURES = {
    "ES": "ES.c.0",  # E-mini S&P 500 - CME
    "NQ": "NQ.c.0",  # E-mini Nasdaq-100 - CME
    "YM": "YM.c.0",  # E-mini Dow ($5) - CBOT
    "RTY": "RTY.c.0",  # E-mini Russell 2000 - CME
}

MICRO_FUTURES = {
    "MES": "MES.c.0",  # Micro E-mini S&P 500 - CME
    "MNQ": "MNQ.c.0",  # Micro E-mini Nasdaq-100 - CME
    "MYM": "MYM.c.0",  # Micro E-mini Dow - CBOT
    "M2K": "M2K.c.0",  # Micro E-mini Russell 2000 - CME
}

# Databento dataset configuration
DATABENTO_DATASET = db.Dataset.GLBX_MDP3  # For CME Group Futures

# Class to manage WebSocket connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {
            "index": [],
            "micro": []
        }
        self.last_data: Dict[str, Dict] = {
            "index": {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data": {
                    # Default empty data for each index future
                    k: {"last": 0.0, "change": 0.0, "percent_change": 0.0, "volume": 0} 
                    for k in INDEX_FUTURES.keys()
                }
            },
            "micro": {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data": {
                    # Default empty data for each micro future
                    k: {"last": 0.0, "change": 0.0, "percent_change": 0.0, "volume": 0} 
                    for k in MICRO_FUTURES.keys()
                }
            }
        }
        self.running = False
        self._databento_client = None
        self.lock = asyncio.Lock()
        # Store last heartbeat time for each connection to manage cleanup
        self.last_heartbeat: Dict[WebSocket, datetime] = {}
        # Store reference price for calculating change values
        self.reference_prices: Dict[str, float] = {}
        # Flag to indicate if we're using real or simulated data
        self.using_real_data = False
        # Initialize symbol mappings dict to track instrument_id to symbol mapping
        self.symbol_mappings: Dict[int, str] = {}

    async def connect(self, websocket: WebSocket, widget_type: str):
        await websocket.accept()
        self.active_connections[widget_type].append(websocket)
        self.last_heartbeat[websocket] = datetime.now()
        
        # Send the last known data immediately after connection
        if self.last_data[widget_type]:
            await websocket.send_json(self.last_data[widget_type])

    def disconnect(self, websocket: WebSocket, widget_type: str):
        if websocket in self.active_connections[widget_type]:
            self.active_connections[widget_type].remove(websocket)
        if websocket in self.last_heartbeat:
            del self.last_heartbeat[websocket]

    async def broadcast(self, message: dict, widget_type: str):
        """
        Send data to all connections of a specific type.
        Maintains a single data source that is distributed to all clients.
        """
        self.last_data[widget_type] = message
        disconnected = []
        
        for connection in self.active_connections[widget_type]:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error sending message: {e}")
                disconnected.append((connection, widget_type))
        
        # Remove any disconnected clients
        for connection, wtype in disconnected:
            self.disconnect(connection, wtype)

    async def _broadcast_error_state(self):
        """Broadcast error state to all clients when Databento is unavailable"""
        # Create error data for index futures
        index_error_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error": "Data provider connection failed",
            "data": {}
        }
        
        # Add N/A values for each index future
        for symbol in INDEX_FUTURES.keys():
            index_error_data["data"][symbol] = {
                "last": "N/A",
                "change": "N/A",
                "percent_change": "N/A",
                "volume": "N/A"
            }
        
        # Create error data for micro futures
        micro_error_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error": "Data provider connection failed",
            "data": {}
        }
        
        # Add N/A values for each micro future
        for symbol in MICRO_FUTURES.keys():
            micro_error_data["data"][symbol] = {
                "last": "N/A",
                "change": "N/A",
                "percent_change": "N/A",
                "volume": "N/A"
            }
        
        # Broadcast error data to all clients
        await self.broadcast(index_error_data, "index")
        await self.broadcast(micro_error_data, "micro")
        
        logger.info("Broadcast error state to all clients due to data provider failure")

    async def start_databento_client(self):
        """Initialize and start the Databento client if not already running"""
        async with self.lock:
            if self.running:
                return
            
            try:
                logger.info("Starting Databento client")
                # Check if API key is provided
                if DATABENTO_API_KEY:
                    # Initialize Databento Live client for real-time data
                    try:
                        # Use Live API for real-time data
                        logger.info(f"Connecting to Databento Live API with dataset {DATABENTO_DATASET}")
                        
                        # Initialize the symbols to get data for
                        index_symbols = list(INDEX_FUTURES.values())
                        micro_symbols = list(MICRO_FUTURES.values())
                        all_symbols = index_symbols + micro_symbols
                        logger.info(f"Using index symbols: {index_symbols}")
                        logger.info(f"Using micro symbols: {micro_symbols}")
                        
                        # Initialize the Databento client for real-time data
                        # Matching the documentation example
                        self._databento_client = db.Live(
                            key=DATABENTO_API_KEY,
                        )
                        logger.info("Databento Live client created successfully")
                        
                        # First subscribe to statistics schema to get settlement prices for reference
                        # This helps us calculate accurate change values
                        logger.info("Subscribing to statistics data for settlement prices")
                        self._databento_client.subscribe(
                            dataset=DATABENTO_DATASET,
                            schema=db.Schema.STATISTICS,
                            symbols=all_symbols,
                            stype_in=db.SType.CONTINUOUS,
                        )
                        
                        # Then subscribe to market data with mbp-1 (BBO) schema
                        logger.info("Subscribing to market data (BBO)")
                        self._databento_client.subscribe(
                            dataset=DATABENTO_DATASET,
                            schema=db.Schema.MBP_1,  # Market by Price level 1 (BBO)
                            symbols=all_symbols,
                            stype_in=db.SType.CONTINUOUS,
                        )
                        
                        # Also subscribe to OHLCV data as a backup
                        logger.info("Subscribing to OHLCV data as backup")
                        self._databento_client.subscribe(
                            dataset=DATABENTO_DATASET,
                            schema=db.Schema.OHLCV_1S,  # 1-second OHLCV data
                            symbols=all_symbols,
                            stype_in=db.SType.CONTINUOUS,
                        )
                        
                        # Initialize symbol mappings dict to track instrument_id to symbol mapping
                        self.symbol_mappings = {}
                        
                        logger.info(f"Connected to Databento with {len(all_symbols)} symbols")
                        self.using_real_data = True
                    except Exception as e:
                        logger.error(f"Failed to initialize Databento client: {e}")
                        logger.warning("Data provider connection failed - showing N/A values")
                        await self._broadcast_error_state()
                        return  # Exit without starting data fetch task
                else:
                    logger.warning("No Databento API key provided - showing N/A values")
                    self.using_real_data = False
                    await self._broadcast_error_state()
                
                self.running = True
                asyncio.create_task(self._fetch_futures_data())
                # Start the connection cleanup task
                asyncio.create_task(self._cleanup_stale_connections())
            except Exception as e:
                logger.error(f"Error starting Databento client: {e}")
                self.running = False
                await self._broadcast_error_state()
                raise

    async def _cleanup_stale_connections(self):
        """Periodically check and remove stale connections"""
        while self.running:
            try:
                now = datetime.now()
                stale_threshold = 60  # seconds
                
                disconnected = []
                for websocket, last_time in self.last_heartbeat.items():
                    if (now - last_time).total_seconds() > stale_threshold:
                        # Find which widget type this connection belongs to
                        for widget_type, connections in self.active_connections.items():
                            if websocket in connections:
                                disconnected.append((websocket, widget_type))
                                break
                
                for websocket, widget_type in disconnected:
                    logger.info(f"Removing stale connection from {widget_type}")
                    self.disconnect(websocket, widget_type)
                
                await asyncio.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.error(f"Error in connection cleanup: {e}")
                await asyncio.sleep(60)  # On error, wait longer before retry

    async def update_heartbeat(self, websocket: WebSocket):
        """Update the last heartbeat time for a connection"""
        self.last_heartbeat[websocket] = datetime.now()

    def _transform_databento_data(self, databento_data: dict, symbol_type: str) -> dict:
        """
        Transform Databento data format to our expected dashboard format
        
        This processes the actual Databento data format to match our
        expected output structure for the dashboard
        """
        # Convert between our key (ES) and Databento symbol (ES.FUT.CME)
        symbol_map = INDEX_FUTURES if symbol_type == "index" else MICRO_FUTURES
        reverse_map = {v: k for k, v in symbol_map.items()}
        
        # Build our output format
        transformed_data = {}
        for full_symbol, value in databento_data.items():
            if full_symbol in reverse_map:
                short_symbol = reverse_map[full_symbol]
                
                # Calculate change and percent change if we have reference price
                if full_symbol in self.reference_prices:
                    reference_price = self.reference_prices.get(full_symbol)
                    change = value["last"] - reference_price
                    percent_change = (change / reference_price) * 100 if reference_price else 0
                else:
                    # If we don't have a reference price yet, store the current price as reference
                    # and show 0 change
                    self.reference_prices[full_symbol] = value["last"]
                    change = 0
                    percent_change = 0
                    logger.info(f"Setting initial reference price for {full_symbol} to {value['last']}")
                
                transformed_data[short_symbol] = {
                    "last": value["last"],
                    "change": change,
                    "percent_change": percent_change,
                    "volume": value["volume"]
                }
        
        return transformed_data

    async def _fetch_futures_data(self):
        """Fetch or simulate futures data"""
        try:
            if self.using_real_data:
                logger.info("Using real Databento data")
                # Process Databento real-time data
                record_count = 0
                async for record in self._databento_client:
                    try:
                        record_count += 1
                        if record_count % 10 == 0:  # Log every 10 records
                            logger.info(f"Processed {record_count} records from Databento")
                        
                        # Process different message types from Databento
                        # The record is likely a dictionary or a Record object with attributes
                        record_type = getattr(record, "record_type", None)
                        
                        # Log the record type to debug
                        logger.debug(f"Received record: {type(record).__name__}, record_type: {record_type}")
                        
                        # Check if it's a symbol mapping message
                        if hasattr(record, "instrument_id") and hasattr(record, "stype_in_symbol") and hasattr(record, "stype_out_symbol"):
                            # Store symbol mappings for later reference
                            instrument_id = record.instrument_id
                            continuous_symbol = record.stype_in_symbol
                            raw_symbol = record.stype_out_symbol
                            
                            # Store mapping from instrument_id to continuous symbol
                            self.symbol_mappings[instrument_id] = continuous_symbol
                            logger.info(f"Symbol mapping: {continuous_symbol} ({raw_symbol}) -> ID {instrument_id}")
                            continue
                        
                        # Check if it's an error message    
                        elif hasattr(record, "err"):
                            # Handle error messages
                            logger.error(f"Databento error: {record.err}")
                            continue
                        
                        # Check if it's a statistics message
                        elif hasattr(record, "stat_type") and hasattr(record, "instrument_id"):
                            # Process statistics messages (settlement prices, etc.)
                            # These are useful for reference prices
                            instrument_id = record.instrument_id
                            
                            # StatisticsMsg with stat_type 3 is settlement price
                            if record.stat_type == 3 and instrument_id in self.symbol_mappings:
                                symbol = self.symbol_mappings[instrument_id]
                                # The price is in nanosecond precision (1e-9)
                                price = record.price / 1_000_000_000.0
                                
                                # Store as reference price
                                self.reference_prices[symbol] = price
                                logger.info(f"Got settlement price for {symbol}: {price}")
                            continue
                        
                        # Check if it's a market by price message
                        elif hasattr(record, "price") and hasattr(record, "size") and hasattr(record, "action"):
                            # Process Market by Price (BBO) messages
                            instrument_id = record.instrument_id
                            
                            # Skip if we don't have a mapping for this instrument
                            if instrument_id not in self.symbol_mappings:
                                continue
                                
                            # Get the mapped symbol
                            symbol = self.symbol_mappings[instrument_id]
                            
                            # Determine if this is an index or micro future
                            reverse_index_map = {v: k for k, v in INDEX_FUTURES.items()}
                            reverse_micro_map = {v: k for k, v in MICRO_FUTURES.items()}
                            
                            if symbol in reverse_index_map:
                                widget_type = "index"
                                short_symbol = reverse_index_map[symbol]
                            elif symbol in reverse_micro_map:
                                widget_type = "micro"
                                short_symbol = reverse_micro_map[symbol]
                            else:
                                # Skip unknown symbols
                                continue
                            
                            # Extract data from the MBP-1 message
                            # Prices are in nanosecond precision (1e-9)
                            price = record.price / 1_000_000_000.0
                            size = record.size
                            
                            # Check for trade event
                            if record.action == 'T':  # Trade
                                # For trades, we want to update our "last" price
                                
                                # Create data structure for this symbol
                                if "data" not in self.last_data.get(widget_type, {}):
                                    self.last_data[widget_type] = {
                                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                        "data": {}
                                    }
                                
                                # Initialize if doesn't exist
                                if short_symbol not in self.last_data[widget_type]["data"]:
                                    self.last_data[widget_type]["data"][short_symbol] = {
                                        "last": price,
                                        "volume": size,
                                        "change": 0,
                                        "percent_change": 0
                                    }
                                else:
                                    # Update volume (accumulate)
                                    current_volume = self.last_data[widget_type]["data"][short_symbol]["volume"]
                                    # Update with new data
                                    self.last_data[widget_type]["data"][short_symbol]["last"] = price
                                    self.last_data[widget_type]["data"][short_symbol]["volume"] = current_volume + size
                                
                                # Calculate change and percent change if we have reference price
                                if symbol in self.reference_prices:
                                    reference_price = self.reference_prices[symbol]
                                    change = price - reference_price
                                    percent_change = (change / reference_price) * 100 if reference_price else 0
                                    
                                    self.last_data[widget_type]["data"][short_symbol]["change"] = change
                                    self.last_data[widget_type]["data"][short_symbol]["percent_change"] = percent_change
                                elif price > 0:
                                    # If we don't have a reference price yet, store the current price as reference
                                    self.reference_prices[symbol] = price
                                    logger.info(f"Setting initial reference price for {symbol} to {price}")
                                
                                self.last_data[widget_type]["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                
                                # Broadcast updated data
                                await self.broadcast(self.last_data[widget_type], widget_type)
                        
                        # Check if it's an OHLCV message
                        elif hasattr(record, "open") and hasattr(record, "close") and hasattr(record, "volume"):
                            # Process OHLCV data as backup
                            instrument_id = record.instrument_id
                            
                            # Skip if we don't have a mapping for this instrument
                            if instrument_id not in self.symbol_mappings:
                                continue
                                
                            # Get the mapped symbol
                            symbol = self.symbol_mappings[instrument_id]
                            
                            # Determine if this is an index or micro future
                            reverse_index_map = {v: k for k, v in INDEX_FUTURES.items()}
                            reverse_micro_map = {v: k for k, v in MICRO_FUTURES.items()}
                            
                            if symbol in reverse_index_map:
                                widget_type = "index"
                                short_symbol = reverse_index_map[symbol]
                            elif symbol in reverse_micro_map:
                                widget_type = "micro"
                                short_symbol = reverse_micro_map[symbol]
                            else:
                                # Skip unknown symbols
                                continue
                            
                            # Extract data from OHLCV
                            # Prices are in nanosecond precision (1e-9)
                            price = record.close / 1_000_000_000.0
                            volume = record.volume
                            
                            # For OHLCV, we'll update our data directly
                            if "data" not in self.last_data.get(widget_type, {}):
                                self.last_data[widget_type] = {
                                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "data": {}
                                }
                            
                            # Initialize if doesn't exist
                            if short_symbol not in self.last_data[widget_type]["data"]:
                                self.last_data[widget_type]["data"][short_symbol] = {
                                    "last": price,
                                    "volume": volume,
                                    "change": 0,
                                    "percent_change": 0
                                }
                            else:
                                # Update with new data
                                self.last_data[widget_type]["data"][short_symbol]["last"] = price
                                self.last_data[widget_type]["data"][short_symbol]["volume"] = volume
                            
                            # Calculate change and percent change if we have reference price
                            if symbol in self.reference_prices:
                                reference_price = self.reference_prices[symbol]
                                change = price - reference_price
                                percent_change = (change / reference_price) * 100 if reference_price else 0
                                
                                self.last_data[widget_type]["data"][short_symbol]["change"] = change
                                self.last_data[widget_type]["data"][short_symbol]["percent_change"] = percent_change
                            elif price > 0:
                                # If we don't have a reference price yet, store the current price as reference
                                self.reference_prices[symbol] = price
                                logger.info(f"Setting initial reference price for {symbol} to {price}")
                            
                            self.last_data[widget_type]["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            # Broadcast updated data
                            await self.broadcast(self.last_data[widget_type], widget_type)
                        
                        # Add support for other message types as needed
                        else:
                            # Log unexpected message types
                            logger.debug(f"Received unhandled message type: {type(record).__name__}")
                            
                    except Exception as e:
                        logger.error(f"Error processing Databento message: {e}")
            else:
                # Show N/A values when data provider is unavailable
                logger.warning("Data provider unavailable - showing N/A values")
                await self._broadcast_error_state()
                
                # Keep the service alive but don't send simulated data
                while self.running:
                    # Periodically re-broadcast N/A values to ensure clients see the error state
                    # This helps with reconnecting clients
                    await self._broadcast_error_state()
                    await asyncio.sleep(30)  # Check every 30 seconds
                
        except Exception as e:
            logger.error(f"Error in futures data fetching: {e}")
            self.running = False
            await self._broadcast_error_state()  # Show error state if data fetching fails
        finally:
            logger.info("Futures data fetching stopped")

    async def stop_databento_client(self):
        """Stop the Databento client"""
        async with self.lock:
            logger.info("Stopping Databento client")
            self.running = False
            # Close Databento client if it exists
            if self._databento_client:
                try:
                    # Close the client connection if method exists
                    if hasattr(self._databento_client, 'close') and callable(self._databento_client.close):
                        await self._databento_client.close()
                    logger.info("Closed Databento client connection")
                except Exception as e:
                    logger.error(f"Error closing Databento client: {e}")

# Initialize connection manager
manager = ConnectionManager()

# Helper function for token verification
def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verify the bearer token against the expected value.
    In production, set API_TOKEN environment variable to a secure value.
    """
    if API_TOKEN is None:
        # If no token is set, authentication is disabled
        logger.warning("Request allowed without authentication as no API_TOKEN is set")
        return "no_token_required"
    
    if credentials.credentials != API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials

# API endpoints
@app.get("/")
async def root():
    """Root endpoint for service health check"""
    return {"status": "online", "service": "TTG Futures Data Service"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "connections": {
            "index": len(manager.active_connections["index"]),
            "micro": len(manager.active_connections["micro"]),
        },
        "data_source": "real" if manager.using_real_data else "N/A"
    }

@app.get("/futures/index")
async def get_index_futures(token: str = Depends(verify_token)):
    """REST API endpoint for index futures data"""
    # Always return data, even if it's empty/default values
    return manager.last_data["index"]

@app.get("/futures/micro")
async def get_micro_futures(token: str = Depends(verify_token)):
    """REST API endpoint for micro futures data"""
    # Always return data, even if it's empty/default values
    return manager.last_data["micro"]

@app.websocket("/ws/futures/index")
async def websocket_index_futures(websocket: WebSocket):
    """WebSocket endpoint for index futures data"""
    await manager.connect(websocket, "index")
    
    # Start the databento client if not already running
    await manager.start_databento_client()
    
    try:
        while True:
            # Process incoming messages and keep connection alive
            data = await websocket.receive_text()
            # Update heartbeat
            await manager.update_heartbeat(websocket)
            
            # Process any commands if needed
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(websocket, "index")
    except Exception as e:
        logger.error(f"Error in index futures websocket: {e}")
        manager.disconnect(websocket, "index")

@app.websocket("/ws/futures/micro")
async def websocket_micro_futures(websocket: WebSocket):
    """WebSocket endpoint for micro futures data"""
    await manager.connect(websocket, "micro")
    
    # Start the databento client if not already running
    await manager.start_databento_client()
    
    try:
        while True:
            # Process incoming messages and keep connection alive
            data = await websocket.receive_text()
            # Update heartbeat
            await manager.update_heartbeat(websocket)
            
            # Process any commands if needed
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(websocket, "micro")
    except Exception as e:
        logger.error(f"Error in micro futures websocket: {e}")
        manager.disconnect(websocket, "micro")

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("Starting TTG Futures Data Service")
    
    # Verify essential environment variables
    missing_vars = []
    if not API_TOKEN:
        missing_vars.append("API_TOKEN")
        logger.warning("API_TOKEN not set - Bearer token authentication will be disabled!")
        logger.warning("Authentication token should be set via environment variable")
    
    if not DATABENTO_API_KEY:
        missing_vars.append("DATABENTO_API_KEY")
        logger.warning("DATABENTO_API_KEY not set - Will display N/A values!")
    
    if missing_vars:
        logger.warning(f"Missing essential environment variables: {', '.join(missing_vars)}")
    
    # Log environment configuration (without sensitive values)
    logger.info(f"CORS origins: {os.getenv('ALLOWED_ORIGINS', '*')}")
    logger.info(f"Authentication enabled: {bool(API_TOKEN)}")
    logger.info(f"Databento API key configured: {bool(DATABENTO_API_KEY)}")
    logger.info(f"Using dataset: {DATABENTO_DATASET}")
    
    # Start data stream on server startup
    asyncio.create_task(manager.start_databento_client())

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown"""
    logger.info("Shutting down TTG Futures Data Service")
    await manager.stop_databento_client()

# Run with: uvicorn main:app --host 0.0.0.0 --port 8000
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 
