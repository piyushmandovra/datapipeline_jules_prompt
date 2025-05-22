import logging
import os
from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Handles incoming POST requests to the /webhook endpoint.
    Expects JSON data in the request body.
    """
    try:
        data = request.get_json()
        if data is None:
            logging.error("Received empty or non-JSON data.")
            return jsonify({"status": "error", "message": "Invalid JSON data"}), 400

        logging.info(f"Received data: {data}")
        # In a real scenario, this data would be passed to the decryption module.
        # For now, we just log it.
        return jsonify({"status": "success", "message": "Data received"}), 200
    except Exception as e:
        logging.exception("Error processing request:")
        return jsonify({"status": "error", "message": "An internal error occurred"}), 500

if __name__ == '__main__':
    # Fetch host and port from environment variables or use defaults
    host = os.environ.get('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.environ.get('FLASK_RUN_PORT', 5000))
    debug_mode = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

    logging.info(f"Starting Flask app on {host}:{port} (Debug: {debug_mode})")
    app.run(host=host, port=port, debug=debug_mode)
