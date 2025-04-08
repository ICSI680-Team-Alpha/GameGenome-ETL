from dotenv import load_dotenv
import os
from load_steam import load_steam_data_to_mongodb
from load_game_metadata import load_steam_metadata_to_mongodb
from load_game_genre import load_steam_genre_to_mongodb
from load_steam_media import load_steam_media_to_mongodb

# Load environment variables from .env file
load_dotenv()
db_username = os.getenv("DATABASE_USERNAME")
db_password = os.getenv("DATABASE_PASSWORD")
db_connection_string = f"mongodb+srv://{db_username}:{db_password}@cluster0.figpbbf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
db_name = os.getenv("DATABASE_NAME")

load_steam_data_to_mongodb(db_connection_string=db_connection_string, db_name=db_name)
load_steam_metadata_to_mongodb(db_connection_string=db_connection_string, db_name=db_name)
load_steam_genre_to_mongodb(db_connection_string=db_connection_string, db_name=db_name)
load_steam_media_to_mongodb(db_connection_string=db_connection_string, db_name=db_name)