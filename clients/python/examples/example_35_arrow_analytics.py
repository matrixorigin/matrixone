# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""File exploration, import and Arrow batches on a local MatrixOne service.

Install the SDK analytics extra and pandas; create a database first. The input
files and staging directory must be readable by the MatrixOne server. Remote
deployment requires an explicit server path mapping or a configured stage.
"""

import argparse
import os

import pyarrow as pa
import pyarrow.parquet as pq

from matrixone import Client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging-dir", required=True)
    parser.add_argument("--database", required=True)
    args = parser.parse_args()
    source = os.path.join(args.staging_dir, "analytics_example.parquet")
    table = pa.table({"id": [1, 2, 3], "region": ["cn", "cn", "us"], "amount": [10.0, 20.0, 5.0]})
    pq.write_table(table, source)
    with Client(
        host="127.0.0.1",
        port=6001,
        user="root",
        password=os.environ.get("MO_PASSWORD", "111"),
        database=args.database,
        sql_log_mode="off",
    ) as client:
        with client.analytics.read_parquet(source) as files:
            sql = "SELECT region, sum(amount) AS total FROM " + files.sql + " AS f GROUP BY region ORDER BY region"
            with files.query(sql) as result:
                print(result.to_arrow().to_pandas())
        try:
            client.analytics.upload_dataframe("analytics_example", table.to_pandas(), args.staging_dir)
            with client.analytics.query("SELECT * FROM analytics_example ORDER BY id", batch_size=2) as batches:
                for batch in batches:
                    print(batch.to_pydict())
        finally:
            client.execute("DROP TABLE IF EXISTS analytics_example")
            os.unlink(source)


if __name__ == "__main__":
    main()
