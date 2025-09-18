import os
import csv
import re
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
from google.cloud import bigquery

# ========= USER SETTINGS =========
PROJECT_ID   = "amer-mediadata-us-amer-pd"
TABLE_FQN    = "amer-mediadata-us-amer-pd.mediadata_marts.datamart_retail"
DATE_COLUMN  = "date"
OUTPUT_DIR   = os.path.expanduser("~/Desktop/Carole_2024Growth_MMX")

START_DATE   = date(2025, 8, 1)
END_DATE     = date(2025, 8, 31)
# =================================


def month_start(dt: date) -> date:
    return date(dt.year, dt.month, 1)


def next_month(dt: date) -> date:
    return month_start(dt) + relativedelta(months=1)


def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


def _clean_numeric_series(s: pd.Series, as_int: bool) -> pd.Series:
    """
    Keep only digits, optional leading '-', and one '.'.
    Strips quotes, whitespace, zero-width chars, etc.
    """
    if s is None:
        return s
    # Normalize to string and strip whitespace/zero-width
    s = (
        s.astype("string")
         .str.replace("\r\n", " ", regex=False)
         .str.replace("\r", " ", regex=False)
         .str.replace("\n", " ", regex=False)
         .str.replace("\u00A0", " ", regex=False)              # NBSP
         .str.replace(r"[\u200B-\u200D\uFEFF]", "", regex=True) # zero-width
         .str.strip()
    )
    # Remove quote characters (straight/curly)
    s = s.str.replace(r"[\"'\u2018\u2019\u201B\u2032\u201C\u201D]", "", regex=True)
    # Keep only digits, minus, and dot
    s = s.str.replace(r"[^0-9\.\-]", "", regex=True)

    out = pd.to_numeric(s, errors="coerce")
    if as_int:
        out = out.round().astype("Int64")  # nullable int
    return out


def _sanitize_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "impressions" in df.columns:
        df["impressions"] = pd.to_numeric(df["impressions"], errors="coerce").round().astype("Int64")
    if "spend" in df.columns:
        df["spend"] = pd.to_numeric(df["spend"], errors="coerce")
        df["spend"] = df["spend"].round(2)   # optional: 2 decimal places
    return df


def export_month(
    client: bigquery.Client,
    table_fqn: str,
    date_col: str,
    start_dt: date,
    end_dt: date,
    out_dir: str
):
    query = f"""
    SELECT
      {date_col} AS date,
      campaign,
      channel,
      ad_type,
      data_source,
      partner,
      division,
      brand,
      brand_category,
      impressions,
      spend
    FROM `{table_fqn}`
    WHERE DATE({date_col}) >= @start_date
      AND DATE({date_col}) <  @end_date
      AND partner In ('Amazon', 'Target', 'Walmart')
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_dt.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_dt.isoformat()),
        ]
    )

    print(f"Running query: {start_dt} to {end_dt} ...")
    df = client.query(query, job_config=job_config).result().to_dataframe(
        create_bqstorage_client=True
    )

    # Clean numeric columns
    df = _sanitize_numeric_columns(df)

    # File name like: <table>_YYYY_MM.csv
    table_leaf = table_fqn.split(".")[-1]
    fname = f"{table_leaf}_{start_dt.strftime('%Y_%m')}_v3.csv"
    fpath = os.path.join(out_dir, fname)

    # Write as standard CSV (minimal quoting, clean numeric values)
    df.to_csv(
        fpath,
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_MINIMAL,
        quotechar='"',
        doublequote=True,
        lineterminator="\n",
    )
    print(f"  -> {len(df):,} rows saved to {fpath}")


def main():
    ensure_dir(OUTPUT_DIR)
    client = bigquery.Client(project=PROJECT_ID)

    cur = month_start(START_DATE)
    while cur < END_DATE:
        chunk_end = min(next_month(cur), END_DATE + relativedelta(days=1))
        try:
            export_month(client, TABLE_FQN, DATE_COLUMN, cur, chunk_end, OUTPUT_DIR)
        except Exception as e:
            print(f"!! Failed for {cur.strftime('%Y-%m')}: {e}")
        cur = next_month(cur)

    print("\nAll done.")


if __name__ == "__main__":
    print("Executing monthly exports from BigQuery...")
    main()
