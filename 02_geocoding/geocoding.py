import os
import psycopg2
import requests
import wikipedia
from dotenv import load_dotenv

# setup
load_dotenv("../.env")
wikipedia.set_lang("de")


def encode_place(place_tupel):
    return(place_tupel[0].replace(" ", "+").replace("?", "") + "," + place_tupel[1].replace(" ", "+").replace("?", ""))


def get_place_coords(place_tupel):
    response = requests.get(
        "https://geocode.xyz/{0}?json=1".format(encode_place(place_tupel)))
    return(response.json())


def print_progress(current, end, length=12):
    progress = current/end
    bar = ("#" * round(progress * length)) + \
        ("-" * round((1 - progress) * length))
    print("Progress: |{0}| {1:.1%}".format(
        bar, progress), end="\r" if progress < 1 else "\n")


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
            places_dict[place_tupel]["ids"] = [result[0]]

    # for all location names get geo location
    updated_ids = []
    step = 0
    print("Updating geo location")
    for place in places_dict:
        # if there is a wikipedia article associated try it first

        # as an alternative use the midpoint of the region + city string
        location = get_place_coords(place)

        if("error" in location):
            print("Error: " + location["error"]["description"])
        else:
            for id in places_dict[place]["ids"]:
                cursor.execute("UPDATE wikidata SET lon=%s, lat=%s WHERE id=%s RETURNING id;",
                               (location["longt"], location["latt"], id))

                step += 1   
                print_progress(step, len(results))

                if(cursor.fetchone()):
                    updated_ids.append(cursor.fetchone())

                connection.commit()

    print("\nUpdated {0} rows".format(len(updated_ids)))

    # commit changes and close connections
    print("Done")

    cursor.close()
    connection.close()


main()
