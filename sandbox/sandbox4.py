import re
import math
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import jetTools  # must contain cfg["postgres"][...] and your pgQuery function
import numpy as np

def clean_column_name(col):
    """
    Convert arbitrary CSV headers into safe PostgreSQL column names.
    Example: 'School Name' -> 'school_name'
    """
    col = col.strip().lower()
    col = re.sub(r'[^a-z0-9_]+', '_', col)
    col = re.sub(r'_+', '_', col).strip('_')
    if not col:
        col = "col"
    if col[0].isdigit():
        col = f"c_{col}"
    return col


def dedupe_columns(columns):
    """
    Ensure cleaned column names are unique.
    """
    seen = {}
    result = []
    for col in columns:
        if col not in seen:
            seen[col] = 0
            result.append(col)
        else:
            seen[col] += 1
            result.append(f"{col}_{seen[col]}")
    return result


def infer_pg_type(series):
    """
    Infer a reasonable PostgreSQL type from a pandas Series.
    Keeps it simple and safe.
    """
    non_null = series.dropna()

    if non_null.empty:
        return "TEXT"

    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"

    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"

    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"

    return "TEXT"


def csv_to_postgres(csv_path, table_name, cfg, if_exists="replace", chunksize=5000):
    """
    Import a CSV into PostgreSQL.

    Parameters
    ----------
    csv_path : str
        Path to CSV file.
    table_name : str
        Target PostgreSQL table name.
    cfg : dict
        Must contain:
        cfg["postgres"]["db"]
        cfg["postgres"]["user"]
        cfg["postgres"]["password"]
        cfg["postgres"]["host"]
        cfg["postgres"]["port"]
    if_exists : str
        One of: 'replace', 'append', 'fail'
    chunksize : int
        Rows per bulk insert batch.
    """

    df = pd.read_csv(csv_path)

    # Clean / dedupe column names
    original_cols = list(df.columns)
    cleaned_cols = [clean_column_name(c) for c in original_cols]
    cleaned_cols = dedupe_columns(cleaned_cols)
    df.columns = cleaned_cols

    conn = psycopg2.connect(
        dbname=cfg["postgres"]["db"],
        user=cfg["postgres"]["user"],
        password=cfg["postgres"]["password"],
        host=cfg["postgres"]["host"],
        port=cfg["postgres"]["port"]
    )

    try:
        with conn, conn.cursor() as cur:
            table_ident = sql.Identifier(table_name)

            if if_exists == "replace":
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table_ident))

            if if_exists in ("replace", "fail"):
                col_defs = []
                for col in df.columns:
                    pg_type = infer_pg_type(df[col])
                    col_defs.append(
                        sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(pg_type))
                    )

                create_sql = sql.SQL("""
                    CREATE TABLE {} (
                        {}
                    )
                """).format(
                    table_ident,
                    sql.SQL(", ").join(col_defs)
                )

                cur.execute(create_sql)

            elif if_exists == "append":
                pass
            else:
                raise ValueError("if_exists must be one of: replace, append, fail")

            # Convert NaN to None for psycopg2
            df = df.replace({np.nan: None})

            insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                table_ident,
                sql.SQL(", ").join(map(sql.Identifier, df.columns))
            )

            rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

            for i in range(0, len(rows), chunksize):
                batch = rows[i:i + chunksize]
                execute_values(cur, insert_sql.as_string(conn), batch)

        conn.commit()
        print(f"Imported {len(df)} rows into table '{table_name}'.")

    finally:
        conn.close()


# Example usage:
csv_to_postgres("hd2024.csv", "ipeds_schools", jetTools.cfg, if_exists="replace")