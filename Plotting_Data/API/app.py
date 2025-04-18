# filepath: app.py
from flask import Flask
from controllers.stock_controller import stock_bp
from services import mongodb_service # Import to ensure connection is attempted at startup

app = Flask(__name__)

# Ensure DB connection is attempted when app starts
mongodb_service.connect_db()

# Register blueprints
app.register_blueprint(stock_bp, url_prefix='/api') # Add '/api' prefix to routes

@app.route('/')
def index():
    return "Stock Data API is running!, how to use: append /api/stock/<emiten>?period=<period> to the URL"

if __name__ == '__main__':
    # Use host='0.0.0.0' to make it accessible on your network
    app.run(debug=True, host='0.0.0.0', port=5000)