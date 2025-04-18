# filepath: config/settings.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env') # Adjust path if .env is elsewhere
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    # Try loading from the project root if not found adjacent to config
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
    else:
        print("Warning: .env file not found.")


MONGODB_CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING')
MONGODB_DATABASE_NAME = os.getenv('MONGODB_DATABASE_NAME')
COLLECTION_YFINANCE_DATA = os.getenv('COLLECTION_YFINANCE_DATA')

if not all([MONGODB_CONNECTION_STRING, MONGODB_DATABASE_NAME, COLLECTION_YFINANCE_DATA]):
    print("Warning: One or more MongoDB environment variables are not set.")