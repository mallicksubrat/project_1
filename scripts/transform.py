import csv
import json
from pathlib import Path


def transform(input_file, output_file):
    with open(input_file, "r") as infile:
        data = json.load(infile)

    if not data:
        # Nothing to transform; create an empty CSV with headers
        items = []
    elif isinstance(data, list):
        items = data
    else:
        # Some APIs wrap results; try a common "products" key, else treat as single item
        items = data.get("products", [data]) if isinstance(data, dict) else []

    fieldnames = [
        "id",
        "title",
        "price",
        "description",
        "category",
        "image",
        "rating",
    ]

    with open(output_file, "w", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in items:
            rating = row.get("rating")
            rating_value = rating.get("rate") if isinstance(rating, dict) else None

            transformed_row = {
                "id": row.get("id"),
                "title": row.get("title"),
                "price": row.get("price"),
                "description": row.get("description"),
                "category": row.get("category"),
                "image": row.get("image"),
                "rating": rating_value,
            }
            writer.writerow(transformed_row)


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent.parent
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    input_file = data_dir / "product_data.json"
    output_file = data_dir / "transformed_data.csv"
    transform(str(input_file), str(output_file))
    print(f"Data transformed and saved to {output_file}")
