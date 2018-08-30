import psycopg2
import requests
from bs4 import BeautifulSoup
import re

# connect to db and open a cursor to start read/write
connection = psycopg2.connect(
    "dbname=zechensterben user=python password=topsecret")
cursor = connection.cursor()

# check if wikidata table already exists if not create it
tablename = "wikidata"

cursor.execute(
    "SELECT * FROM information_schema.tables WHERE table_name='{0}';".format(tablename))

result = cursor.fetchone()

if(result == None):
    print("creating new table", tablename)
    cursor.execute(
        "CREATE TABLE {0} (id serial PRIMARY KEY, name varchar, region varchar, city varchar, open_year integer, close_year integer, is_active boolean DEFAULT false, notes varchar);".format(tablename))
else:
    print("Using existing table", result[2])


# get the wikipedia page and parse the content
response = requests.get(
    "https://de.m.wikipedia.org/wiki/Liste_von_Bergwerken_in_Nordrhein-Westfalen")
soup = BeautifulSoup(response.text, 'html.parser')

print('Now sraping', soup.title.get_text())

# find all collapsible divs and extract the tables in there
divs = soup.find_all("div", {"class": "collapsible-block"})

tables = []
for div in divs:
    table = div.find("table")
    if(table):
        tables.append(table)

# pull data out of tables and write it to db
print("Formatting data and inserting into", tablename)

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
            "open_year": open_year_match.group(0) if open_year_match else "NULL",
            "close_year": close_year_match.group(0) if close_year_match else "NULL",
            "notes": raw_data[5]
        }

        data["is_active"] = True if raw_data[4] == "heute" or data["notes"] == "aktiv" else False

        # insert into db
        cursor.execute(
            "INSERT INTO wikidata (name, region, city, open_year, close_year, is_active, notes) VALUES ('{name}', '{region}', '{city}', {open_year}, {close_year}, '{is_active}', '{notes}');".format(**data))

# commit changes and close connections
print("Done")

connection.commit()
cursor.close()
connection.close()
