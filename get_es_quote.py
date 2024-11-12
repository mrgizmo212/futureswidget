from http.server import HTTPServer, BaseHTTPRequestHandler
import databento as db
import json
from datetime import datetime, timezone
import threading

class LiveQuoteManager:
    def __init__(self):
        self.latest_quote = {"error": "Waiting for data..."}
        self.client = None
        self.running = False

    def start_live_feed(self):
        self.client = db.Live(key="db-eAhRWMKCiJLEpDAk8cvbFSeUWSXCK")
        
        # Subscribe to ES continuous front month
        self.client.subscribe(
            dataset="GLBX.MDP3",
            schema="trades",
            stype_in="continuous",
            symbols=["ES.c.0"]
        )
        
        self.running = True
        
        # Start processing live data
        for record in self.client:
            if not self.running:
                break
                
            if isinstance(record, db.TradeMsg):
                # Convert nanoseconds to seconds and create timestamp
                ts_event = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
                
                self.latest_quote = {
                    "symbol": "ES",
                    "price": float(record.price) / 1e9,
                    "size": int(record.size),
                    "timestamp": ts_event.strftime('%H:%M:%S'),
                    "side": record.side,
                    "date": ts_event.strftime('%Y-%m-%d')
                }
                print(f"Updated quote: {self.latest_quote}")

    def stop_live_feed(self):
        self.running = False
        if self.client:
            self.client.stop()

# Global quote manager
quote_manager = LiveQuoteManager()

class QuoteHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Send the latest quote
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(quote_manager.latest_quote).encode())

        except Exception as e:
            error_response = {"error": str(e)}
            print(f"Error occurred: {str(e)}")
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(error_response).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

def run_server(port=8000):
    server_address = ('', port)
    httpd = HTTPServer(server_address, QuoteHandler)
    print(f"Server running on port {port}")
    
    # Start the live feed in a separate thread
    feed_thread = threading.Thread(target=quote_manager.start_live_feed)
    feed_thread.daemon = True
    feed_thread.start()
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        quote_manager.stop_live_feed()
        httpd.server_close()

if __name__ == '__main__':
    run_server()