import os
import psycopg2
import requests
from dotenv import load_dotenv
load_dotenv("../.env")


def encodePlace(place_tupel):
    return(place_tupel[0].replace(" ", "+").replace("?", "") + "," + place_tupel[1].replace(" ", "+").replace("?", ""))


def main():
    # connect to db and open a cursor to start read/write
    print("Connecting to PostgreSQL")

    connection = psycopg2.connect(
        "dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD}".format(**os.environ))
    cursor = connection.cursor()

    # get all rows that have valid years/active status and have no geo location yet
    cursor.execute(
        "SELECT * FROM wikidata WHERE open_year IS NOT NULL AND (close_year IS NOT NULL OR is_active = True) AND (lat IS NULL OR lon IS NULL);")
    results = cursor.fetchall()
    connection.commit()

    # collect ids for all location names
    places_dict = {}

    for result in results:
        place_tupel = (result[2], result[3])
        if(place_tupel in places_dict):
            places_dict[place_tupel]["ids"].append(result[0])
        else:
            places_dict.update({place_tupel: {}})
            places_dict[place_tupel]["place_string"] = encodePlace(place_tupel)
            places_dict[place_tupel]["ids"] = [result[0]]

    # for all location names get geo location
    updated_ids = []
    step = 0
    print("Updating geo location")
    for place in places_dict.values():
        response = requests.get(
            "https://geocode.xyz/{0}?json=1".format(place["place_string"]))
        location = response.json()

        if("error" in location):
            print("Error: " + location["error"]["description"])
        else:
            for id in place["ids"]:
                cursor.execute("UPDATE wikidata SET lon=%s, lat=%s WHERE id=%s RETURNING id;",
                               (location["longt"], location["latt"], id))

                step = step + 1
                print("Progress: {0}%".format(
                    round(step/len(results) * 100, 1)), end="\r")

                if(cursor.fetchone()):
                    updated_ids.append(cursor.fetchone())

                connection.commit()

    print("\nUpdated {0} rows".format(len(updated_ids)))

    # commit changes and close connections
    print("Done")

    cursor.close()
    connection.close()


main()
