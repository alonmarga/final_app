import requests
import logging

# The create_mock_pii function with added logging
def create_mock_pii():
    logger = logging.getLogger("main_logger.create_mock_pii")  # Get the logger specific to this function

    logger.info("create_mock_pii function called.")

    url = "https://api.mockaroo.com/api/generate.json?key=82bfd910&schema=pii"

    logger.debug(f"Fetching data from URL: {url}")
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        personal_data = []

        for item in data:
            personal_data_item = {}

            for key, value in item.items():
                personal_data_item[key] = value

            personal_data.append(personal_data_item)

        logger.info(f"Number of customers created:  {len(personal_data)}")
        logger.info("Successfully retrieved data from the API.")
        return personal_data

    else:
        logger.error("Failed to retrieve data from the API.")
        return []
