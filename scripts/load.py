import csv
import logging
import sqlite3
from pathlib import Path


def load_full_db(input_file, db_file):
    """
    Load into a 'full' products table with description/image, using CSV id as PRIMARY KEY.
    """
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"CSV file not found: {input_path}")

    with input_path.open(newline="") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    if not rows:
        raise RuntimeError(f"No data rows found in CSV: {input_path}")

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
        rating = row.get("rating")
        try:
            rating_value = float(rating) if rating not in (None, "") else None
        except (TypeError, ValueError):
            rating_value = None

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


def load_db(csv_file, db_file):
    """
    Load products from CSV into an analytics-friendly products table.

    Schema:
      product_id INTEGER PRIMARY KEY
      title TEXT
      price REAL
      category TEXT
      rating REAL
    """
    csv_path = Path(csv_file)
    db_path = Path(db_file)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with csv_path.open(newline="") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    if not rows:
        raise RuntimeError(f"No data rows found in CSV: {csv_path}")

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

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

    for row in rows:
        try:
            product_id = int(row.get("id")) if row.get("id") else None
        except (TypeError, ValueError):
            continue

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
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    input_file = data_dir / "transformed_data.csv"

    # By default, load the analytics store DB
    store_db_file = data_dir / "store.db"
    try:
        load_db(str(input_file), str(store_db_file))
    except (FileNotFoundError, RuntimeError) as exc:
        logging.error(str(exc))
    else:
        print(f"Analytics data loaded into SQLite database {store_db_file}")
