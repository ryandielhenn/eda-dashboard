from dotenv import load_dotenv
import os

load_dotenv()
API_BASE = os.getenv("API_BASE_URL", "http://api:8000")
