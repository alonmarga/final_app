import pandas as pd
import pycountry
import random
import logging

# Set up the logger for the current module (optional)
logger = logging.getLogger(__name__)


# Function that receives the dataframe, adds country code if possible, otherwise adds none
def get_country_code(df):
    def get_code(country_name):
        try:
            country = pycountry.countries.get(name=country_name)
            return country.alpha_3 if country else None
        except KeyError:
            return None

    # Logging the start of the function
    logger.info("get_country_code function called.")

    # Applying the get_code function to add country code
    df['country_code'] = df['country'].apply(get_code)

    # Logging the number of rows and columns in the DataFrame
    logger.info(f"get_country_code function processed {df.shape[0]} rows and {df.shape[1]} columns.")

    # Counting the number of unknown country codes
    num_unknown_countries = df['country_code'].isnull().sum()
    # Logging the number of unknown country codes
    logger.info(f"Number of unknown country codes: {num_unknown_countries}")

    # Logging the end of the function
    logger.info("get_country_code function execution completed.")
    return df


# Function to read the cities data file
def get_cities_file(cities_files_path):
    # Logging the start of the function
    logger.info("get_cities_file function called.")

    cities_df = pd.read_csv(cities_files_path)
    cities_df = cities_df[['city', 'iso3']]

    # Logging the number of rows and columns in the DataFrame
    logger.info(f"get_cities_file function processed {cities_df.shape[0]} rows and {cities_df.shape[1]} columns.")

    # Logging the end of the function
    logger.info("get_cities_file function execution completed.")
    return cities_df


# Function that receives two dataframes (cities and invoices), adds random city to every country possible, otherwise add unknown
def add_random_city(grouped_df, cities_df):
    # Create a dictionary mapping country codes to lists of cities
    cities_dict = cities_df.groupby('iso3')['city'].apply(list).to_dict()

    # Create a dictionary to store purchase_id-city mappings
    purchase_id_city_mapping = {}

    # Function to get a random city for a given country code
    def get_random_city(country_code):
        if country_code is None:
            return "Unknown"
        else:
            cities = cities_dict.get(country_code)
            if cities:
                return random.choice(cities)
            else:
                return "Unknown"

    # Logging the start of the function
    logger.info("add_random_city function called.")

    # Iterate over the rows of the grouped_df DataFrame
    for index, row in grouped_df.iterrows():
        purchase_id = row['purchase_id']
        if purchase_id not in purchase_id_city_mapping:
            # Generate a random city for the purchase_id if it's not already mapped
            purchase_id_city_mapping[purchase_id] = get_random_city(row['country_code'])

        # Assign the city to the 'city' column for the current row
        grouped_df.at[index, 'city'] = purchase_id_city_mapping[purchase_id]

    # Counting the number of unknown cities
    num_unknown_cities = (grouped_df['city'] == 'Unknown').sum()
    # Logging the number of unknown cities
    logger.info(f"Number of unknown cities: {num_unknown_cities}")

    # Logging the end of the function
    logger.info("add_random_city function execution completed.")
    return grouped_df
