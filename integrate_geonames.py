import os
import shutil
import zipfile
import psycopg2
import logging
from config import dbname, user, password, host, port, folder_path, country_codes, table_name
from datetime import datetime
    
# Define the folder path
script_path = os.path.dirname(os.path.abspath(__file__))
logs_folder = os.path.join(script_path, 'logs')
working_folder = os.path.join(script_path, 'working')  # Define the 'working' folder path beside the script


# Create 'working' and 'logs' folders if they don't exist
if not os.path.exists(working_folder):
    os.makedirs(working_folder)

if not os.path.exists(logs_folder):
    os.makedirs(logs_folder)


# Setup logging
log_date = datetime.now().strftime("%Y-%m-%d")
log_file_path = os.path.join(script_path, f"logs/integration_{log_date}.log")  # Define the log file path beside the script

logging.basicConfig(
    filename=log_file_path,
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(message)s',  # Add the timestamp to the logging format
    datefmt='%Y-%m-%d %H:%M:%S'  # Define the date format for the timestamp
)

# Create a working folder for extraction and integration
os.makedirs(working_folder, exist_ok=True)

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)
cur = conn.cursor()

# Initialize a counter for successful integrations
success_count = 0
total_count=0

# Check if table exists, create if it doesn't
cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
table_exists = cur.fetchone()[0]

if not table_exists:
    logging.info(f"Table '{table_name}' does not exist. Creating a new table.")
    # Create table
    cur.execute(f"""
        CREATE TABLE {table_name} (
            geonameid BIGINT,
            name TEXT,
            asciiname TEXT,
            alternatenames TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            feature_class VARCHAR(10),
            feature_code TEXT,
            country_code VARCHAR(40),
            cc2 TEXT,
            admin1_code TEXT,
            admin2_code TEXT,
            admin3_code TEXT,
            admin4_code TEXT,
            population BIGINT,
            elevation TEXT NULL, 
            dem TEXT,
            timezone TEXT,
            modification_date DATE
        )
    """)
else:
    cur.execute(f"TRUNCATE TABLE {table_name};")
    conn.commit()
    logging.info(f"Table '{table_name}' exists. Truncated the table before data integration.")

# Iterate through the array of country codes
for country_code in country_codes:
    logging.info(f"Processing data for {country_code}...")

    # Copy the zip file from the folder path to the working folder
    shutil.copy(os.path.join(folder_path, f"{country_code}.zip"), working_folder)

    # Extract the contents of the zip file
    with zipfile.ZipFile(os.path.join(working_folder, f"{country_code}.zip"), 'r') as zip_ref:
        zip_ref.extractall(os.path.join(working_folder, country_code))

    # Get the path to the CSV file
    csv_file_path = os.path.join(working_folder, country_code, f"{country_code}.txt")

    
    with open(csv_file_path, 'r', encoding='utf-8', errors='ignore') as data_file:
       cur.copy_expert(f"""
            COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER E'\t', NULL 'NULL', QUOTE E'\b')
        """, data_file)

    # Log the number of integrated lines for the current country code
    integrated_lines_count = cur.rowcount
    total_count+=integrated_lines_count
    # Check if the integration is successful
    if integrated_lines_count > 0:
        logging.info(f"Integration successful for {country_code}. {cur.rowcount} lines integrated.")
        success_count += 1
    else:
        logging.warning(f"No data integrated for {country_code}.")

# Check if all integrations were successful
if success_count == (len(country_codes)-1):
    logging.info(f"Integration successful for all countries: {success_count} with {total_count} lines")
    # Remove the 'working' folder if the integration is successful
    shutil.rmtree(working_folder)
else:
    logging.warning("Integration failed for one or more countries.")

# Commit the changes and close the connection
conn.commit()
cur.close()
conn.close()
