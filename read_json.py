import ijson

# Specify the path to your large Wikidata JSON file
json_file_path = 'D:/work/toolkit/wikidata_row/latest-all.json'

# Specify the coordinates of Paris
paris_coordinates = {'latitude': 48.8566, 'longitude': 2.3522}

# Open the JSON file and create an iterator
with open(json_file_path, 'r', encoding='utf-8') as file:
    json_iterator = ijson.items(file, 'item')

    # Process the JSON data in chunks
    for chunk in json_iterator:
        # Filter records for Paris based on coordinates
        paris_records = [
            record for record in chunk
            if 'P625' in record and
               record['P625']['latitude'] == paris_coordinates['latitude'] and
               record['P625']['longitude'] == paris_coordinates['longitude']
        ]

        # Process each record in the filtered set
        for record in paris_records:
            print(record)
