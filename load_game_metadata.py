import pandas as pd
import json
import pymongo
from connect_db import connect_to_mongodb


def load_steam_metadata_to_mongodb(db_connection_string, db_name):
    """
    Load Steam data from a CSV file into MongoDB collection steam

    :param file_path: Path to the CSV file containing Steam data
    :param db_connection_string: MongoDB connection string
    :param db_name: Name of the database to connect to
    """
    
    file_path = './datasets/steam.csv'
    # file_path_tag = './datasets/steam_tag_data.csv'
    collection = "steam_metadata"

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
    collection.create_index([('GameID', pymongo.ASCENDING)], unique=True)
    collection.insert_many(records)

    print("Data loaded successfully into MongoDB.")

def transform_steam_data(df):
    """
    Transform the Steam data by removing unnecessary columns and renaming others.
    steam.csv -> appid,name,release_date,english,developer,publisher,platforms,required_age,categories,
    genres,steamspy_tags,achievements,positive_ratings,negative_ratings,average_playtime,median_playtime,owners,price

    database design:
    GameID, Genres[], Tags[], Platforms[], Features[]

    :param file_path: Path to the CSV file containing Steam data
    :return: Transformed DataFrame
    """
    
    df = df.drop(columns=[
        "name", "release_date", "english", "developer", "publisher", "required_age", "categories", "achievements",
        "positive_ratings", "negative_ratings", "average_playtime", "median_playtime", "owners", "price"
    ])

    # Rename columns
    df.rename(columns={
        "appid": "GameID",
        "platforms": "Platforms", 
        "genres": "Genres",
        "steamspy_tags": "Tags",
    }, inplace=True)

    return df


#     steam_tag_data.csv -> appid, tags()
# # Create the new column D as a dictionary containing values from A, B, and C
#     columns_to_merge = [col for col in df.columns if col != "appid"]
#     new_column_name = 'D'

#     # Create the new nested column
#     df["tags"] = df.apply(
#         lambda row: {col: row[col] for col in columns_to_merge}, 
#         axis=1
#     )