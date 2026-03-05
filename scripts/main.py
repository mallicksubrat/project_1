import json
import logging
from pathlib import Path

from ingest import ingest_data
from transform import transform
from load_db import load_db


def main():
    base_dir = Path(__file__).resolve().parent.parent
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    api_url = "https://fakestoreapi.com/products"
    product_json = data_dir / "product_data.json"
    transformed_csv = data_dir / "transformed_data.csv"
    db_path = data_dir / "store.db"

    # Stage 1: Ingest
    logging.info("Fetching product data")
    try:
        data = ingest_data(api_url)
    except RuntimeError as exc:
        logging.error(str(exc))
        return

    # Log how many records we received (best-effort)
    try:
        count = len(data)
    except TypeError:
        count = 1
    logging.info("Received %d records", count)

    logging.info("Writing raw data file")
    with open(product_json, "w") as f:
        json.dump(data, f, indent=4)

    # Stage 2: Transform
    logging.info("Transforming data to CSV")
    transform(str(product_json), str(transformed_csv))

    # Stage 3: Load into SQLite
    logging.info("Loading data into SQLite")
    load_db(transformed_csv, db_path)
    logging.info("pipeline completed successfully")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    main()
