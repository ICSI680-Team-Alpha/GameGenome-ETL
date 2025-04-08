import pandas as pd
import json
import pymongo
from connect_db import connect_to_mongodb


def load_steam_genre_to_mongodb(db_connection_string, db_name):
    """
    Load Steam data from a CSV file into MongoDB collection steam

    :param file_path: Path to the CSV file containing Steam data
    :param db_connection_string: MongoDB connection string
    :param db_name: Name of the database to connect to
    """
    
    file_path = './datasets/steamspy_tag_data.csv'
    collection = "steam_genre"

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
    steam_tag_data.csv -> appid, tags(781 columns)


    database design:
    steam_tag_data.csv -> appid, tags[]

    :param file_path: Path to the CSV file containing Steam data
    :return: Transformed DataFrame
    """

    # Create the new column D as a dictionary containing values from A, B, and C
    columns_to_merge = [col for col in df.columns if col != "appid"]

    # Create the new nested column
    df["genre"] = df.apply(
        lambda row: {col: row[col] for col in columns_to_merge}, 
        axis=1
    )

    df = df.drop(columns=columns_to_merge)

    # Rename columns
    df.rename(columns={
        "appid": "AppID"
    }, inplace=True)

    return df
