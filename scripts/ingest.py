import json
from pathlib import Path

import requests


def ingest_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Check if the request was successful
    except requests.exceptions.RequestException:
        raise RuntimeError("API ingestion failed")
    data = response.json()
    return data


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent.parent
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    api_url = "https://fakestoreapi.com/products"
    try:
        data = ingest_data(api_url)
    except RuntimeError as exc:
        print(str(exc))
    else:
        output_path = data_dir / "product_data.json"
        with open(output_path, "w") as f:
            json.dump(data, f, indent=4)
        print(f"Data ingested and saved to {output_path}")
