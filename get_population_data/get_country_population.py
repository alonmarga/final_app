import requests
from bs4 import BeautifulSoup


def get_country_population(country_name):
    formatted_country_name = country_name.replace(" ", "_")
    url = f"https://en.wikipedia.org/wiki/{formatted_country_name}"
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
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
        print(f"Failed to retrieve data from {url}. Status code: {response.status_code}")
    return None


country_name = input("Enter the country name: ")
population = get_country_population(country_name)
if population:
    print(f"The population of {country_name} is {population}")
else:
    print("Country not found")