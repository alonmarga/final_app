import requests
from bs4 import BeautifulSoup

def get_city_population(place_name):
    # Prepare the place name for the Wikipedia URL
    formatted_place_name = place_name.title().replace(" ", "_")

    # Build the Wikipedia URL for the place
    url = f"https://en.wikipedia.org/wiki/{formatted_place_name}"

    # Send a GET request to the Wikipedia page
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        # Find the population information on the page
        population_section = soup.find("th", string="Population")
        if population_section:
            population = population_section.find_next("td")
            if population:
                return population.text.strip()
            else:
                return "Population information not found"
        else:
            return "Population information not found"
    else:
        # Request was not successful
        print(f"Failed to retrieve data from {url}. Status code: {response.status_code}")

    return None

# Example usage
place_name = input("Enter the city name: ")
population = get_city_population(place_name)
if population:
    print(f"The population of {place_name} is {population}")
else:
    print("Population information not found")