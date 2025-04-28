from pymongo import MongoClient

def connect_to_mongodb(db_connection_string, db_name):
    """
    Connect to MongoDB using the provided connection string and database name.
    
    :param connection_string: MongoDB connection string
    :param db_name: Name of the database to connect to
    :return: MongoClient instance and database object
    """
    print("Connecting to MongoDB...")
    client = MongoClient(db_connection_string, 
                        connectTimeoutMS=30000,  # Increase from 20000 to 30000
                        socketTimeoutMS=45000,   # Add socket timeout
                        serverSelectionTimeoutMS=30000)  # Add server selection timeout
    db = client[db_name]
    print("Connected to MongoDB successfully.")
    return client, db
