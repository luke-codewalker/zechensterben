import psycopg2
import requests
from bs4 import BeautifulSoup
import re
from dotenv import load_dotenv
load_dotenv("../.env")
import os
from hashlib import md5

# connect to db and open a cursor to start read/write
print("Connecting to PostgreSQL")

connection = psycopg2.connect(
    "dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}".format(**os.environ))
cursor = connection.cursor()


# check if wikidata table already exists if not create it
tablename = "wikidata"

cursor.execute(
    "SELECT * FROM information_schema.tables WHERE table_name='{}';".format(tablename))

result = cursor.fetchone()

if(result == None):
    print("--> Creating new table", tablename)
    cursor.execute(
        "CREATE TABLE {} (id varchar PRIMARY KEY, name varchar, region varchar, city varchar, open_year integer, close_year integer, is_active boolean DEFAULT false, notes varchar);".format(tablename))
else:
    print("--> Using existing table", result[2])


# get the wikipedia page and parse the content
print("Fetching data from wikipedia page")

response = requests.get(
    "https://de.m.wikipedia.org/wiki/Liste_von_Bergwerken_in_Nordrhein-Westfalen")

soup = BeautifulSoup(response.text, 'html.parser')


# find all collapsible divs and extract the tables in there
print("Now scraping", soup.title.get_text())

divs = soup.find_all("div", {"class": "collapsible-block"})

tables = []
for div in divs:
    table = div.find("table")
    if(table):
        tables.append(table)


# pull data out of tables and write it to db
print("Formatting data and inserting into", tablename)

inserted_ids = []

for i, table in enumerate(tables):
    rows = table.find_all("tr")
    for row in rows[1:len(rows)-1]:
        # make list of all elements in row
        raw_data = list(BeautifulSoup.get_text(x).replace("\n", "").replace("\'", "")
                        for x in row.find_all("td"))
        # turn list into dictionary and format values correctly
        open_year_match = re.search(r"\d{4}", raw_data[3])
        close_year_match = re.search(r"\d{4}", raw_data[4])
        data = {
            "name": raw_data[0],
            "region": raw_data[1],
            "city": raw_data[2],
            "open_year": open_year_match.group(0) if open_year_match else None,
            "close_year": close_year_match.group(0) if close_year_match else None,
            "notes": raw_data[5]
        }

        ident_string = data["name"] + data["region"] + data["city"]
        data["id"] = md5(ident_string.encode("utf-8")).hexdigest()[0:7]
        data["is_active"] = True if raw_data[4] == "heute" or data["notes"] == "aktiv" else False

        # insert into db (on succes id is returned and added to list)
        cursor.execute(
            "INSERT INTO wikidata (id, name, region, city, open_year, close_year, is_active, notes) VALUES (%(id)s, %(name)s, %(region)s, %(city)s, %(open_year)s::integer, %(close_year)s::integer, %(is_active)s::boolean, %(notes)s) ON CONFLICT (id) DO NOTHING RETURNING id;", data)
        if(cursor.fetchone()):
            inserted_ids.append(cursor.fetchone())

print("Inserted {0} rows {1}".format(len(inserted_ids), "" if len(inserted_ids) > 0 else "(db might already be populated)"))

# commit changes and close connections
print("Done")

connection.commit()
cursor.close()
connection.close()
