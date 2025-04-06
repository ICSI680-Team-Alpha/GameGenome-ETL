from connect_db import connect_to_mongodb
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
db_username = os.getenv("DATABASE_USERNAME")
db_password = os.getenv("DATABASE_PASSWORD")
db_connection_string = f"mongodb+srv://{db_username}:{db_password}@cluster0.figpbbf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
db_name = os.getenv("DATABASE_NAME")

mongoClient, db = connect_to_mongodb(db_connection_string, db_name)
