import logging
import os
import re
import sqlite3
import time

import pandas as pd
from pandas.errors import ParserError

print("ingestion.py is executing")

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

DB_PATH = "inventory.db"
DATA_DIR = "data"
READ_CHUNK_SIZE = 10_000


def normalize_identifier(name: str) -> str:
    name = re.sub(r"\W+", "_", name.strip())
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return name or "col"


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def csv_chunks(file_path: str):
    common_kwargs = {
        "chunksize": READ_CHUNK_SIZE,
        "dtype": str,
        "keep_default_na": False,
        "na_filter": False,
        "low_memory": True,
        "on_bad_lines": "skip",
        "encoding": "utf-8",
        "encoding_errors": "replace",
    }
    try:
        yield from pd.read_csv(file_path, engine="c", **common_kwargs)
    except ParserError:
        logging.warning("C engine parser failed for %s. Retrying with python engine.", file_path)
        yield from pd.read_csv(file_path, engine="python", **common_kwargs)


def prepare_table(cursor: sqlite3.Cursor, table_name: str, columns):
    quoted_table = quote_ident(table_name)
    quoted_cols = [quote_ident(c) for c in columns]
    col_defs = ", ".join(f"{c} TEXT" for c in quoted_cols)
    cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
    cursor.execute(f"CREATE TABLE {quoted_table} ({col_defs})")
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO {quoted_table} ({', '.join(quoted_cols)}) VALUES ({placeholders})"
    return insert_sql


def load_raw_data():
    start = time.time()
    print("Script started")
    logging.info("Ingestion started")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -200000")
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")
    cursor = conn.cursor()

    try:
        for file_name in sorted(os.listdir(DATA_DIR)):
            if not file_name.endswith(".csv"):
                continue

            file_path = os.path.join(DATA_DIR, file_name)
            table_name = normalize_identifier(file_name[:-4])
            logging.info("Ingesting file: %s -> table: %s", file_name, table_name)

            insert_sql = None
            total_rows = 0

            for chunk in csv_chunks(file_path):
                chunk.columns = [normalize_identifier(c) for c in chunk.columns]
                if insert_sql is None:
                    insert_sql = prepare_table(cursor, table_name, chunk.columns)

                rows = list(chunk.itertuples(index=False, name=None))
                if rows:
                    cursor.executemany(insert_sql, rows)
                    total_rows += len(rows)
                    if total_rows % (READ_CHUNK_SIZE * 5) == 0:
                        conn.commit()

            conn.commit()
            logging.info("Completed %s: %s rows", file_name, total_rows)
            print(f"{file_name}: {total_rows} rows ingested")
    finally:
        cursor.close()
        conn.close()

    total_time = (time.time() - start) / 60
    logging.info("Ingestion complete in %.2f minutes", total_time)
    print(f"Total Time Taken: {total_time:.2f} minutes")


if __name__ == "__main__":
    load_raw_data()