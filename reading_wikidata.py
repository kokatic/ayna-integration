import ijson

def get_countries_economies_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        # Use ijson to parse the JSON file incrementally
        parser = ijson.parse(file)

        # Initialize variables to store data
        countries = []
        current_country = None
        current_label = None
        current_gdp_value = None

        for prefix, event, value in parser:
            # Look for relevant fields in the JSON data
            if prefix.endswith('.country.value'):
                current_country = value
            elif prefix.endswith('.countryLabel.value'):
                current_label = value
            elif prefix.endswith('.gdpValue.value.amount'):
                current_gdp_value = value

            # Check if all relevant fields have been found for a country
            if current_country and current_label and current_gdp_value:
                countries.append((current_country, current_label, current_gdp_value))
                current_country = current_label = current_gdp_value = None

    return countries

# Example usage
json_file_path = 'D:/work/toolkit/wikidata_row/latest-all.json'
countries_data = get_countries_economies_from_file(json_file_path)

# Print the results
for country_data in countries_data:
    country_uri, country_label, gdp_value = country_data
    print(f"Country: {country_label}, GDP: {gdp_value}")
