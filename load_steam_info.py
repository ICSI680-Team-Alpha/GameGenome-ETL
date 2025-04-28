import pandas as pd
import json
import pymongo
from connect_db import connect_to_mongodb


def load_steam_info_mongodb(db_connection_string, db_name):
    """
    Load Steam data from a CSV file into MongoDB collection steam

    :param file_path: Path to the CSV file containing Steam data
    :param db_connection_string: MongoDB connection string
    :param db_name: Name of the database to connect to
    """
    
    file_path = './datasets/steam.csv'
    file_path2 = './datasets/steam_media_data.csv'
    collection = "steam_info"

    print("Loading csv files...")
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)
    df2 = pd.read_csv(file_path2)
    print("CSV file loaded successfully.")
    print("Transforming data...")
    df = transform_steam_data(df=df, df2=df2)
    print("Data transformed successfully.")

    # Connect to MongoDB
    client, db = connect_to_mongodb(db_connection_string=db_connection_string, db_name=db_name)
    # Convert DataFrame to JSON format
    records = json.loads(df.to_json(orient='records'))

    # Insert records into MongoDB collection
    collection = db[collection]
    # Make AppID the primary key
    print("Creating index on AppID...")
    collection.create_index([('AppID', pymongo.ASCENDING)], unique=True)
    print("Index created successfully.")
    print("Inserting records into MongoDB...")
    collection.insert_many(records)

    print("Data loaded successfully into MongoDB.")

def transform_steam_data(df, df2):
    """
    Transform the Steam data by removing unnecessary columns and renaming others.
    steam.csv -> appid,name,release_date,english,developer,publisher,platforms,required_age,categories,
    genres,steamspy_tags,achievements,positive_ratings,negative_ratings,average_playtime,median_playtime,owners,price
    database design:
    AppID, Name, ReleaseDate, Developer, Publisher, RequiredAge, Achievements, PositiveRatings, NegativeRatings,
    AveragePlaytime, MedianPlaytime, OwnersMin, OwnersMax, Price

    :param file_path: Path to the CSV file containing Steam data
    :return: Transformed DataFrame
    """
    df = df.drop(columns=[
        'publisher',
        'average_playtime',
        'negative_ratings',
        'median_playtime',
        'price',
        'required_age',
        'positive_ratings',
        'achievements',
        'developer',
        'english', 
        'platforms', 
        'release_date',
    ])

    # Rename columns
    df.rename(columns={
        'appid': 'AppID',
        'name': 'Name',
        'categories': 'Categories', 
        'genres': 'Genres', 
        'steamspy_tags': 'Tags'
    }, inplace=True)

    df2 = df2.drop(columns=[
        'movies'
    ])

    df2.rename(columns={
        'steam_appid': 'AppID',
        'header_image': 'HeaderImage',
        'background': 'Background',
        'screenshots': 'Screenshots',
    }, inplace=True)

    # Merge the two DataFrames on 'AppID'
    df3 = pd.merge(df, df2, on='AppID', how='left')

    return df3