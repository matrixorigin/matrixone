#!/usr/bin/env python3
"""Exercise Spark's JDBC write isolation against an isolated MatrixOne database."""

from __future__ import annotations

import os
import sys
import uuid

from pyspark.sql import SparkSession


DRIVER = "com.mysql.cj.jdbc.Driver"
EXPECTED_ROWS = [(1, 0.25), (2, 0.75)]


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def jdbc_connection(spark: SparkSession, url: str, user: str, password: str):
    return spark._jvm.java.sql.DriverManager.getConnection(url, user, password)


def query_rows(connection, sql: str):
    statement = connection.createStatement()
    result = statement.executeQuery(sql)
    try:
        rows = []
        while result.next():
            rows.append((int(result.getLong(1)), float(result.getDouble(2))))
        return rows
    finally:
        result.close()
        statement.close()


def row_count(connection, table: str) -> int:
    statement = connection.createStatement()
    result = statement.executeQuery(f"SELECT COUNT(*) FROM `{table}`")
    try:
        if not result.next():
            raise AssertionError(f"COUNT(*) returned no row for {table}")
        return int(result.getLong(1))
    finally:
        result.close()
        statement.close()


def exception_chain(exc: Exception) -> str:
    messages = [str(exc)]
    current = getattr(exc, "java_exception", None)
    for _ in range(20):
        if current is None:
            break
        message = str(current.toString())
        try:
            error_code = int(current.getErrorCode())
        except Exception:
            error_code = 0
        if error_code:
            message += f" [JDBC error code {error_code}]"
        messages.append(message)
        current = current.getCause()
    return "\n".join(messages)


def drop_owned_tables(spark: SparkSession, tables: list[str]) -> None:
    connection = jdbc_connection(
        spark,
        required_env("MO_JDBC_URL"),
        required_env("MO_USER"),
        required_env("MO_PASSWORD"),
    )
    try:
        for table in tables:
            statement = connection.createStatement()
            try:
                statement.execute(f"DROP TABLE IF EXISTS `{table}`")
                result = statement.executeQuery(f"SHOW TABLES LIKE '{table}'")
                try:
                    if result.next():
                        raise AssertionError(f"probe table was not removed: {table}")
                finally:
                    result.close()
            finally:
                statement.close()
    finally:
        connection.close()


def main() -> None:
    url = required_env("MO_JDBC_URL")
    user = required_env("MO_USER")
    password = required_env("MO_PASSWORD")
    connector_jar = os.path.abspath(required_env("MYSQL_CONNECTOR_JAR"))
    if not os.path.isfile(connector_jar):
        raise RuntimeError(f"Connector/J jar does not exist: {connector_jar}")

    suffix = uuid.uuid4().hex[:12]
    default_table = f"spark_jdbc_default_{suffix}"
    rc_table = f"spark_jdbc_rc_{suffix}"
    tables = [default_table, rc_table]

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("matrixone-spark-jdbc-isolation-29838")
        .config("spark.jars", connector_jar)
        .getOrCreate()
    )
    primary_error = None
    try:
        if spark.version != "3.5.3":
            raise RuntimeError(
                f"expected Spark 3.5.3, got {spark.version}; use the pinned probe environment"
            )

        jvm = spark._jvm
        jvm.java.lang.Class.forName(DRIVER)
        print(f"JDK: {spark._jvm.java.lang.System.getProperty('java.version')}")
        print(f"Spark: {spark.version}")

        setup_connection = jdbc_connection(spark, url, user, password)
        try:
            statement = setup_connection.createStatement()
            try:
                version_result = statement.executeQuery("SELECT VERSION()")
                try:
                    if not version_result.next():
                        raise AssertionError("SELECT VERSION() returned no row")
                    print(f"MatrixOne version: {version_result.getString(1)}")
                finally:
                    version_result.close()
                isolation_result = statement.executeQuery(
                    "SELECT @@session.transaction_isolation"
                )
                try:
                    if not isolation_result.next():
                        raise AssertionError(
                            "session isolation query returned no row"
                        )
                    print(
                        "Fresh JDBC session isolation: "
                        f"{isolation_result.getString(1)}"
                    )
                finally:
                    isolation_result.close()
                for table in tables:
                    statement.execute(
                        f"CREATE TABLE `{table}` (id BIGINT NOT NULL PRIMARY KEY, value DOUBLE NOT NULL)"
                    )
            finally:
                statement.close()
        finally:
            setup_connection.close()

        frame = spark.createDataFrame(EXPECTED_ROWS, "id long, value double")

        try:
            frame.coalesce(1).write.jdbc(
                url=url,
                table=default_table,
                mode="append",
                properties={"driver": DRIVER, "user": user, "password": password},
            )
        except Exception as exc:
            detail = exception_chain(exc)
            normalized = detail.lower()
            if (
                "20105" not in normalized
                or "read-uncommitted" not in normalized
                or "not supported" not in normalized
            ):
                raise AssertionError(
                    "default Spark JDBC write failed for a reason other than the expected "
                    "MatrixOne 20105 unsupported READ_UNCOMMITTED error:\n"
                    f"{detail}"
                ) from exc
            print("PASS: default Spark JDBC write rejects READ_UNCOMMITTED with MatrixOne 20105")
        else:
            raise AssertionError(
                "default Spark JDBC write unexpectedly succeeded; MatrixOne must not silently "
                "map READ_UNCOMMITTED to another isolation level"
            )

        check_connection = jdbc_connection(spark, url, user, password)
        try:
            if row_count(check_connection, default_table) != 0:
                raise AssertionError("default write inserted rows before the isolation error")
        finally:
            check_connection.close()
        print("PASS: default write left its pre-created table empty")

        writer_properties = {
            "driver": DRIVER,
            "user": user,
            "password": password,
            "isolationLevel": "READ_COMMITTED",
        }
        writer_rdd = spark.sparkContext.parallelize(EXPECTED_ROWS, 2)
        partition_row_counts = writer_rdd.mapPartitions(
            lambda rows: [sum(1 for _ in rows)]
        ).collect()
        if partition_row_counts != [1, 1]:
            raise AssertionError(
                "expected one row in each of two writer partitions, got "
                f"{partition_row_counts}"
            )
        writer_frame = spark.createDataFrame(writer_rdd, "id long, value double")
        print(f"Spark READ_COMMITTED writer partition row counts: {partition_row_counts}")
        writer_frame.write.jdbc(
            url=url,
            table=rc_table,
            mode="append",
            properties=writer_properties,
        )

        read_properties = {"driver": DRIVER, "user": user, "password": password}
        spark_rows = sorted(
            (int(row.id), float(row.value))
            for row in spark.read.jdbc(url=url, table=rc_table, properties=read_properties).collect()
        )
        if spark_rows != EXPECTED_ROWS:
            raise AssertionError(f"fresh Spark JDBC read mismatch: {spark_rows}")

        read_connection = jdbc_connection(spark, url, user, password)
        try:
            sql_rows = query_rows(read_connection, f"SELECT id, value FROM `{rc_table}` ORDER BY id")
            if sql_rows != EXPECTED_ROWS:
                raise AssertionError(f"independent JDBC SQL read mismatch: {sql_rows}")
            statement = read_connection.createStatement()
            result = statement.executeQuery(
                f"SELECT COUNT(*), SUM(value) FROM `{rc_table}`"
            )
            try:
                if not result.next() or int(result.getLong(1)) != 2 or float(result.getDouble(2)) != 1.0:
                    raise AssertionError("independent JDBC aggregate must be count=2, sum=1.0")
            finally:
                result.close()
                statement.close()
        finally:
            read_connection.close()

        print(f"PASS: READ_COMMITTED Spark writer committed both rows from two partitions: {spark_rows}")
        print("PASS: fresh Spark read and independent JDBC SQL read agree; count=2, sum=1.0")
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error = None
        try:
            drop_owned_tables(spark, tables)
        except BaseException as exc:
            cleanup_error = exc
        try:
            spark.stop()
        except BaseException as exc:
            if cleanup_error is None:
                cleanup_error = exc
            else:
                print(f"Spark shutdown also failed: {exc}", file=sys.stderr)
        if cleanup_error is not None:
            if primary_error is None:
                raise cleanup_error
            print(f"cleanup also failed: {cleanup_error}", file=sys.stderr)


if __name__ == "__main__":
    main()
