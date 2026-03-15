"""
Run a BigQuery query against Odoo source dataset.
Uses Application Default Credentials (run: gcloud auth application-default login).
"""
from __future__ import annotations

import bq_dataset

TABLE = "account_account"
LIMIT = 1000


def main() -> None:
    client = bq_dataset.get_source_client()
    fq_table = bq_dataset.source_table(TABLE)
    query = f"""
        SELECT * FROM {fq_table}
        LIMIT {LIMIT}
    """
    print(f"Running: {query.strip()}")
    print()
    df = client.query(query).to_dataframe()
    print(df.to_string())


if __name__ == "__main__":
    main()
