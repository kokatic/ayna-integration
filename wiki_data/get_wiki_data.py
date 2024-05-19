import os
import psycopg2
import logging
from datetime import datetime
from urllib.parse import urlparse
from wikidataintegrator import wdi_core
from config import dbname, user, password, host, port, folder_path, country_codes, table_name

# Define the folder path
script_path = os.path.dirname(os.path.abspath(__file__))
logs_folder = os.path.join(script_path, 'logs')
working_folder = os.path.join(script_path, 'working')  # Define the 'working' folder path beside the script

if not os.path.exists(logs_folder):
    os.makedirs(logs_folder)

# Setup logging
log_date = datetime.now().strftime("%Y-%m-%d")
log_file_path = os.path.join(script_path, f"logs/integration_wikidata_{log_date}.log")  # Define the log file path beside the script

logging.basicConfig(
    filename=log_file_path,
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(message)s',  # Add the timestamp to the logging format
    datefmt='%Y-%m-%d %H:%M:%S'  # Define the date format for the timestamp
)

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)
cur = conn.cursor()

# Iterate through the array of country codes
for country_code in country_codes:
    logging.info(f"Get wiki data for {country_code}...")

    # Fetch the list of IDs for the current country from the database
    cur.execute(f"SELECT geonameid, wikidataid FROM {table_name} WHERE country_code = '{country_code}'")
    rows = cur.fetchall()

    # Initialize a counter for the number of updates
    update_count = 0

    for row in rows:
        geonameid = row[0]
        wiki_id = row[1]

        # If WikiID is not already present, fetch it and update the database
        if wiki_id is None:
            query = f"""
            SELECT ?id ?idLabel WHERE {{
              ?id wdt:P1566 "{geonameid}".
              SERVICE wikibase:label {{
                bd:serviceParam wikibase:language "en" .
              }}
            }}
            """
            result = wdi_core.WDItemEngine.execute_sparql_query(query)
            
            for binding in result["results"]["bindings"]:
                full_wiki_id = binding["id"]["value"]
                # Extract the last part of the Wiki ID
                wiki_id = urlparse(full_wiki_id).path.split('/')[-1]

            # Update the database with the fetched WikiID
            cur.execute(f"UPDATE {table_name} SET wikidataid = '{wiki_id}' WHERE geonameid = '{geonameid}'")
            conn.commit()
            update_count += 1

    # Log the number of updates for the current country
    logging.info(f"Updated {update_count} wikidataid(s) for {country_code} in {table_name}")

# Close the database connection
cur.close()
conn.close()
