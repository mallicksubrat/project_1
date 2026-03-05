import csv
import sqlite3
from pathlib import Path


def load_db(csv_file, db_file):
    """Load products from a CSV file into a SQLite database."""
    # Ensure paths are strings for sqlite3/open
    csv_file = str(csv_file)
    db_file = str(db_file)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create table with the requested schema.
    # Use product_id as PRIMARY KEY so INSERT OR IGNORE prevents duplicates.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            title TEXT,
            price REAL,
            category TEXT,
            rating REAL
        )
        """
    )

    # Read from CSV
    with open(csv_file, newline="") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    for row in rows:
        # Map CSV "id" to product_id
        try:
            product_id = int(row.get("id")) if row.get("id") else None
        except (TypeError, ValueError):
            # Skip rows without a valid id
            continue

        # Convert price and rating to numeric where possible
        try:
            price = float(row.get("price")) if row.get("price") else None
        except (TypeError, ValueError):
            price = None

        try:
            rating = float(row.get("rating")) if row.get("rating") else None
        except (TypeError, ValueError):
            rating = None

        cursor.execute(
            """
            INSERT OR IGNORE INTO products (product_id, title, price, category, rating)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                product_id,
                row.get("title"),
                price,
                row.get("category"),
                rating,
            ),
        )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent.parent
    csv_path = base_dir / "data" / "transformed_data.csv"
    db_path = base_dir / "data" / "store.db"

    load_db(csv_path, db_path)
    print(f"Data loaded into SQLite database {db_path}")
