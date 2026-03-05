import csv
import sqlite3
from pathlib import Path


def load_data(input_file, db_file):
    # Read transformed data from CSV
    with open(input_file, newline="") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            title TEXT,
            price REAL,
            description TEXT,
            category TEXT,
            image TEXT,
            rating REAL
        )
        """
    )

    for row in rows:
        # Handle rating which may be a plain value in CSV
        rating = row.get("rating")
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
    base_dir = Path(__file__).resolve().parent.parent
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    input_file = data_dir / "transformed_data.csv"
    db_file = data_dir / "products.db"
    load_data(str(input_file), str(db_file))
    print(f"Data loaded into SQLite database {db_file}")
