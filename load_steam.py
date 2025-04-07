import pandas as pd
import numpy as np
import os
import json
import pymongo
from pymongo import MongoClient
from connect_db import connect_to_mongodb


def load_steam_data_to_mongodb(db_connection_string, db_name):
    """
    Load Steam data from a CSV file into MongoDB collection steam

    :param file_path: Path to the CSV file containing Steam data
    :param db_connection_string: MongoDB connection string
    :param db_name: Name of the database to connect to
    """
    
    file_path = './datasets/steam.csv'
    collection = "steam"

    print("Loading csv files...")
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)
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
    steam.csv -> appid,name,release_date,english,developer,publisher,platforms,required_age,categories,
    genres,steamspy_tags,achievements,positive_ratings,negative_ratings,average_playtime,median_playtime,owners,price
    database design:
    AppID, Name, ReleaseDate, Developer, Publisher, RequiredAge, Achievements, PositiveRatings, NegativeRatings,
    AveragePlaytime, MedianPlaytime, OwnersMin, OwnersMax, Price

    :param file_path: Path to the CSV file containing Steam data
    :return: Transformed DataFrame
    """
    df = df.drop(columns=[
        'english', 'platforms', 'categories', 'genres', 'steamspy_tags',
    ])

    # add OwersMin and OwnersMax columns
    df['OwnersMin'] = df['owners'].apply(lambda x: int(x.split('-')[0].replace(',', '')))
    df['OwnersMax'] = df['owners'].apply(lambda x: int(x.split('-')[1].replace(',', '')) if '-' in x else int(x.replace(',', '')))

    # Rename columns
    df.rename(columns={
        'appid': 'AppID',
        'name': 'Name',
        'release_date': 'ReleaseDate',
        'developer': 'Developer',
        'publisher': 'Publisher',
        'required_age': 'RequiredAge',
        'achievements': 'Achievements',
        'positive_ratings': 'PositiveRatings',
        'negative_ratings': 'NegativeRatings',
        'average_playtime': 'AveragePlaytime',
        'median_playtime': 'MedianPlaytime',
        'price': 'Price'
    }, inplace=True)

    return df