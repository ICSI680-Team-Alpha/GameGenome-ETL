import pandas as pd
import json
import pymongo
from connect_db import connect_to_mongodb


def load_steam_media_to_mongodb(db_connection_string, db_name):
    """
    Load Steam data from a CSV file into MongoDB collection steam

    :param file_path: Path to the CSV file containing Steam data
    :param db_connection_string: MongoDB connection string
    :param db_name: Name of the database to connect to
    """
    
    file_path = './datasets/steam_media_data.csv'
    collection = "steam_media"

    print("Loading csv files...")
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    # df_tag = pd.read_csv(file_path_tag)
    print("CSV file loaded successfully.")
    print("Transforming data...")
    df = transform_steam_data(df=df)
    print("Data transformed successfully.")

    # Connect to MongoDB
    client, db = connect_to_mongodb(db_connection_string=db_connection_string, db_name=db_name)
    print("Inserting data into MongoDB...")

    # Convert DataFrame to JSON format
    records = json.loads(df.to_json(orient='records'))

    # Insert records into MongoDB collection
    collection = db[collection]
    # Make AppID the primary key
    collection.create_index([('AppID', pymongo.ASCENDING)], unique=True)
    collection.insert_many(records)

    print("Data loaded successfully into MongoDB.")

def transform_steam_data(df):
    """
    Transform the Steam data by removing unnecessary columns and renaming others.
    steam_media_data.csv -> steam_appid,header_image,screenshots,background,movies

    database design:
    AppID, HeaderImage,Screenshots,Background,Movies

    :param file_path: Path to the CSV file containing Steam data
    :return: Transformed DataFrame
    """

    # Rename columns
    df.rename(columns={
        "steam_appid": "AppID",
        "header_image": "HeaderImage",
        "screenshots": "Screenshots",
        "background": "Background",
        "movies": "Movies"
    }, inplace=True)

    return df