import csv
import json
import sqlite3


def load_data(input_file, db_file):
    # Read transformed data from CSV instead of JSON
    with open(input_file, newline="") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            title TEXT,
            price REAL,
            description TEXT,
            category TEXT,
            image TEXT,
            rating REAL
        )
    """)

    for row in rows:
        # Handle rating which may be a plain value in CSV
        rating = row.get("rating")
        if isinstance(rating, dict):
            rating_value = rating.get("rate")
        else:
            try:
                rating_value = float(rating) if rating not in (None, "") else None
            except (TypeError, ValueError):
                rating_value = None

        # Convert numeric fields where possible
        try:
            id_value = int(row["id"]) if row.get("id") else None
        except (TypeError, ValueError, KeyError):
            id_value = None

        try:
            price_value = float(row["price"]) if row.get("price") else None
        except (TypeError, ValueError, KeyError):
            price_value = None

        cursor.execute(
            """
            INSERT OR REPLACE INTO products (id, title, price, description, category, image, rating)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                id_value,
                row.get("title"),
                price_value,
                row.get("description"),
                row.get("category"),
                row.get("image"),
                rating_value,
            ),
        )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    input_file = "/Users/subrat/project1/data/transformed_data.csv"
    db_file = "/Users/subrat/project1/data/products.db"
    load_data(input_file, db_file)
    print("Data loaded into SQLite database products.db")
