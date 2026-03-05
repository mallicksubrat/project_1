import csv
import json


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
    input_file = "/Users/subrat/project1/data/product_data.json"
    output_file = "/Users/subrat/project1/data/transformed_data.csv"
    transform(input_file, output_file)
    print("Data transformed and saved to transformed_data.csv")
