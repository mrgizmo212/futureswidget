from flask import Flask, jsonify
from flask_cors import CORS
import databento as db
from datetime import datetime, timezone
import threading
import logging
import os

app = Flask(__name__)
CORS(app)

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

@app.route('/')
def get_quote():
    logger.info(f"Current quotes: {quote_manager.latest_quotes}")  # Add logging
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
    port = int(os.environ.get('PORT', 8000))
    logger.info(f"Starting server on port {port}")
    app.run(host='0.0.0.0', port=port)
