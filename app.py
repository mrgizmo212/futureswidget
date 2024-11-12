from flask import Flask, jsonify
from flask_cors import CORS
import databento as db
from datetime import datetime, timezone
import threading
import logging
import os

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LiveQuoteManager:
    def __init__(self):
        self.latest_quotes = {"ES.c.0": {"error": "Waiting for data..."}}
        self.client = None
        self.running = False

    def start_live_feed(self):
        try:
            # Get API key from environment variable or use default
            api_key = os.environ.get('DATABENTO_KEY', 'db-eAhRWMKCiJLEpDAk8cvbFSeUWSXCK')
            self.client = db.Live(key=api_key)
            
            self.client.subscribe(
                dataset="GLBX.MDP3",
                schema="trades",
                stype_in="continuous",
                symbols=["ES.c.0"]
            )
            
            self.running = True
            logger.info("Live feed started successfully")
            
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
                        logger.info(f"Quote updated: {quote_data}")
                        
                    except Exception as e:
                        logger.error(f"Record processing error: {str(e)}")
                        
        except Exception as e:
            error_msg = f"Feed error: {str(e)}"
            logger.error(error_msg)
            self.latest_quotes["ES.c.0"] = {"error": error_msg}

    def stop_live_feed(self):
        self.running = False
        if self.client:
            self.client.stop()
        logger.info("Live feed stopped")

quote_manager = LiveQuoteManager()

@app.route('/')
def get_quote():
    return jsonify({
        "quotes": quote_manager.latest_quotes,
        "server_time": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    })

def start_feed():
    feed_thread = threading.Thread(target=quote_manager.start_live_feed)
    feed_thread.daemon = True
    feed_thread.start()
    logger.info("Feed thread started")

# Initialize the feed
with app.app_context():
    start_feed()

if __name__ == '__main__':
    # Get port from environment variable (for Render) or use 8000 (for local)
    port = int(os.environ.get('PORT', 8000))
    logger.info(f"Starting server on port {port}")
    app.run(host='0.0.0.0', port=port)
