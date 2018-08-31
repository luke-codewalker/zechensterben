import os
import psycopg2
import requests
from dotenv import load_dotenv
load_dotenv("../.env")

# connect to db and open a cursor to start read/write
print("Connecting to PostgreSQL")

connection = psycopg2.connect(
    "dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}".format(**os.environ))
cursor = connection.cursor()


# get all rows that have valid years/active status
cursor.execute(
    "SELECT * FROM wikidata WHERE open_year IS NOT NULL AND (close_year IS NOT NULL OR is_active = True);")
results = cursor.fetchall()


# commit changes and close connections
print("Done")

connection.commit()
cursor.close()
connection.close()
