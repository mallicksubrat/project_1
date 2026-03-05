# Product Data Pipeline

This project ingests product data from an external API, transforms it, and loads it into a SQLite database for analytics.

## Structure

- `scripts/ingest.py` – fetches raw product data from the API.
- `scripts/transform.py` – converts raw JSON into a normalized CSV.
- `scripts/load.py` – loads the CSV into SQLite:
  - `load_db` for the analytics database (`data/store.db`).
  - `load_full_db` for a richer schema if needed.
- `scripts/main.py` – single entry point that runs ingest → transform → load.
- `data/` – generated files (JSON, CSV, SQLite DBs).
- `sql/analytics.sql` – example analytics queries.

## Requirements

- Python 3.11+ (or compatible Python 3)
- `pip`

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Running the pipeline

From the project root:

```bash
python -m venv venv_project1
source venv_project1/bin/activate   # On Windows: venv_project1\Scripts\activate
pip install -r requirements.txt

python scripts/main.py
```

This will:

1. Fetch products from `https://fakestoreapi.com/products`.
2. Save raw JSON to `data/product_data.json`.
3. Transform JSON to CSV `data/transformed_data.csv`.
4. Load analytics data into `data/store.db` (table `products`).

## Querying the analytics database

You can inspect `data/store.db` with the SQLite CLI:

```bash
sqlite3 data/store.db
```

Example queries:

```sql
-- Top 5 expensive products
SELECT title, price
FROM products
ORDER BY price DESC
LIMIT 5;

-- Average price per category
SELECT category, AVG(price)
FROM products
GROUP BY category;

-- Product count per category
SELECT category, COUNT(*)
FROM products
GROUP BY category;
```

