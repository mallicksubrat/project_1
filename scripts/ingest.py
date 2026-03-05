import requests
import json


def ingest_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Check if the request was successful
    except requests.exceptions.RequestException as e:
        return None
    data = response.json()
    return data


if __name__ == "__main__":
    api_url = "https://fakestoreapi.com/products"
    data = ingest_data(api_url)
    with open("/Users/subrat/project1/data/product_data.json", "w") as f:
        json.dump(data, f, indent=4)
    print("Data ingested and saved to data.json")
