#Extracting data from OpenSea
'''
import requests

#API endpoint URL
url = "https://api.opensea.io/api/v2/collections?chain=ethereum"


#Query parameters for filtering the results
params = {
    "chain": "ethereum"
}

#Adding API key to the headers
headers = {
    "Accept": "application/json"
    "X-API-KEY": "2TYNN3CIIPRVHTJW43PPCCPFXRND2KKE"
}

response = requests.get(url, params=params, headers=headers)

if response.status_code == 200:
    data = response.json()
    collections = data.get("collections", [])
        for collection in collections:
        print("Collection:", collection.get("collection"))
        print("Name:", collection.get("name"))
        print("Description:", collection.get("description"))
        print("Image URL:", collection.get("image_url"))
        print("Owner:", collection.get("owner"))
        print("Twitter Username:", collection.get("twitter_username"))
        print("Contracts:", collection.get("contracts"))

else:
   print("Error code:", response.status_code)
'''

#Importing mock data
import csv
from warnings import filters

#Path to the CSV file
csv_file_path = '/home/nata/Downloads/MOCK_DATA.csv'

#Opening the CSV file containing mock data
with open(csv_file_path, 'r', newline='') as csvfile:
    #Creating a CSV reader object
    csvreader = csv.reader(csvfile)

    #Skipping the header row if present
    next(csvreader)

    #Iterating over each row in the CSV file
    for row in csvreader:
        #Processing each row of mock data as needed
        collection = {
            "collection": row[0],
            "name": row[1],
            "description": row[2],
            "image_url": row[3],
            "owner": row[4],
            "twitter_username": row[5],
            "contracts": row[6]
        }

        #Printing or processing the collection data as needed
        print("Collection:", collection.get("collection"))
        print("Name:", collection.get("name"))
        print("Description:", collection.get("description"))
        print("Image URL:", collection.get("image_url"))
        print("Owner:", collection.get("owner"))
        print("Twitter Username:", collection.get("twitter_username"))
        print("Contracts:", collection.get("contracts"))

#Data Transformation

import pandas as pd

df = pd.read_csv('/home/nata/Downloads/MOCK_DATA.csv')
print("Original Data:")
#Displaying the first few rows, last few rows and general information about the dataframe
print(df.head())
print(df.tail())
print(df.info)
print(df.describe())
#Finding out if there are any duplicate rows and removing them if present
duplicates = df.duplicated()
num_duplicates = duplicates.sum()
#Finding out if there are any NaN values and deleting them/replacing them with placeholders if present
NaN_values = df.isna().sum()
print(NaN_values)
#Finding out if there are any datetime values and formatting them if present
date_columns = df.select_dtypes(include=['datetime', 'datetime64'])
if not date_columns.empty:
    print("Date columns found:")
    print(date_columns.columns)
else:
    print("No date columns found.")
#Displaying the cleaned data
print("\nCleaned Data:")
print(df.head())

#Data Mapping

#Finding all unique values in "collection" column
unique_collections = df['collection'].unique()
print("Unique Collections:", unique_collections)

#Mapping values in "collection" column
mapping = {
    'Honorable': 'Honorable',
    'Dr': 'Doctor',
    'Mrs': 'Mrs',
    'Ms': 'Miss',
    'Mr': 'Mister',
    'Rev': 'Reverend'
}
#Applying this mapping to the 'collection' column
df['collection'] = df['collection'].map(mapping)

print(df)

#Data Aggregation

#Grouping the dataframe by 'collection' column
collection_groups = df.groupby('collection')

#Counting how many items are in each collection
item_counts = collection_groups.size()
print(item_counts)

#Listing all unique descriptions for each collection
unique_descriptions = collection_groups['description'].unique()
print("Unique Descriptions:", unique_descriptions)

#Finding out which owner has the most items in each collection
top_owners = collection_groups['owner'].agg(lambda x: x.mode()[0])
print("Top Owners:", top_owners)

#Extracting a sample item from each collection for further analysis if needed
sample_items = collection_groups.apply(lambda x: x.sample(n=1))
print("Sample Items:", sample_items)

#Data Loading
'''
I created a database using terminal, after that I created a table using following commands:
postgres=# CREATE TABLE OpenSeaCollections (
postgres(#     collection VARCHAR(255),
postgres(#     name VARCHAR(255),
postgres(#     description TEXT,
postgres(#     image_url TEXT,
postgres(#     owner VARCHAR(255),
postgres(#     twitter_username VARCHAR(255),
postgres(#     contracts TEXT
postgres(# );
CREATE TABLE

Then I loaded data into the database:
postgres=# COPY OpenSeaCollections(collection, name, description, image_url, owner, twitter_username, contracts) 
postgres-# FROM '/home/nata/Downloads/MOCK_DATA.csv'
postgres-# DELIMITER ',' 
postgres-# CSV HEADER;
COPY 1000
'''

#Bonus Assignment: Custom ORM Development

import psycopg2_binary

class customORM:
    def __init__(self, dbname, user, password):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password
        )
        self.cursor = self.conn.cursor()

    def create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS OpenSeaCollections (
        id SERIAL PRIMARY KEY,
        collection VARCHAR(255),
        name VARCHAR(255),
        description TEXT,
        image_url TEXT,
        owner VARCHAR(255),
        twitter_username VARCHAR(255),
        contracts TEXT
        )
      """)
        self.conn.commit()
'''
    def delete_table(self, table_name):
        self.cursor.execute(f"DROP TABLE IF EXISTS {OpenSeaCollections}")
        self.conn.commit()
'''


def insert_record(self, collection, name, description, image_url, owner, twitter_username, contracts):
    self.cursor.execute("""
            INSERT INTO OpenSeaCollections (collection, name, description, image_url, owner, twitter_username, contracts)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (collection, name, description, image_url, owner, twitter_username, contracts))
    self.conn.commit()


def read_record(self, filters=None, limit=None, order_by=None):
    query = "SELECT * FROM OpenSeaCollections"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    if order_by:
        query += " ORDER BY " + order_by
    if limit:
        query += " LIMIT " + str(limit

        filters = ["description LIKE '%amet%' OR description ILIKE 'n%'"]
        records = orm.read_record(filters=filters)
        print("Filtered Records:", records)

            self.cursor.execute(query)
        return self.cursor.fetchall()

    def update_record(self, record_id, new_collection, new_name, new_description, new_image_url, new_owner, new_twitter_username, new_contracts):
        self.cursor.execute("""
            UPDATE OpenSeaCollections
            SET collection = %s, name = %s, description = %s, image_url = %s, owner = %s, twitter_username = %s, contracts = %s
            WHERE ID = %s
        """, (new_collection, new_name, new_description, new_image_url, new_owner, new_twitter_username, new_contracts, records_id))
        self.conn.commit()

    def delete_record(self, record_id):
        self.cursor.execute("DELETE FROM OpenSeaCollections WHERE id = %s", (record_id,))
        self.conn.commit()


    def remove_column(self, twitter_username):
        self.cursor.execute(f"ALTER TABLE OpenSeaCollections DROP COLUMN {twitter_username}")
        self.conn.commit()

    def add_column(self, twitter_username):
        self.cursor.execute(f"ALTER TABLE OpenSeaCollections ADD COLUMN {twitter_username}")
        self.conn.commit()

    def change_column_data_type(self, column_name, new_data_type):
        self.cursor.execute(f"ALTER TABLE OpenSeaCollections ALTER COLUMN {collections} TYPE {VARCHAR(255)}")
        self.conn.commit()

    def insert_records(self, records):
        for record in records:
            self.insert_record(*record)






