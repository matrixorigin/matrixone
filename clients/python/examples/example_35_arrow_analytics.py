# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""File exploration, import and Arrow batches on a local MatrixOne service.

Install the SDK analytics extra and pandas; create a database first. The input
files and staging directory must be readable by the MatrixOne server. Remote
deployment requires an explicit server path mapping or a configured stage.
"""

import argparse
import os
import tempfile
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from matrixone import Client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging-dir", required=True)
    parser.add_argument("--database", required=True)
    args = parser.parse_args()
    fd, source = tempfile.mkstemp(prefix="analytics_example_", suffix=".parquet", dir=args.staging_dir)
    os.close(fd)
    target = "analytics_example_" + uuid.uuid4().hex
    created = False
    table = pa.table({"id": [1, 2, 3], "region": ["cn", "cn", "us"], "amount": [10.0, 20.0, 5.0]})
    try:
        pq.write_table(table, source)
        with Client(
            host="127.0.0.1",
            port=6001,
            user="root",
            password=os.environ.get("MO_PASSWORD", "111"),
            database=args.database,
            sql_log_mode="off",
        ) as client:
            try:
                with client.analytics.read_parquet(source) as files:
                    sql = (
                        "SELECT region, sum(amount) AS total FROM "
                        + files.sql
                        + " AS f GROUP BY region ORDER BY region"
                    )
                    with files.query(sql) as result:
                        print(result.to_arrow().to_pandas())
                client.analytics.upload_dataframe(target, table.to_pandas(), args.staging_dir)
                created = True
                with client.analytics.query("SELECT * FROM `" + target + "` ORDER BY id", batch_size=2) as batches:
                    for batch in batches:
                        print(batch.to_pydict())
            finally:
                if created:
                    client.execute("DROP TABLE IF EXISTS `" + target + "`")
    finally:
        os.unlink(source)


if __name__ == "__main__":
    main()
