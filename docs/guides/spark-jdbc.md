# Spark JDBC writes to MatrixOne

[Spark's JDBC write option documentation](https://spark.apache.org/docs/3.5.6/sql-data-sources-jdbc.html#data-source-option)
lists `READ_UNCOMMITTED` as the default `isolationLevel`. MatrixOne rejects
that isolation level, so a Spark write that leaves the default in place fails
when Connector/J configures the writer connection. Set Spark's JDBC
`isolationLevel` option to `READ_COMMITTED` on every write:

```python
connection_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "your_user",
    "password": "your_password",
    "isolationLevel": "READ_COMMITTED",
}

df.write.jdbc(
    url="jdbc:mysql://127.0.0.1:6001/your_database",
    table="target_table",
    mode="append",
    properties=connection_properties,
)
```

`isolationLevel` is a Spark JDBC **write** option. Spark opens JDBC connections
for its writer tasks, so setting isolation on a separate SQL connection does
not configure those writes. MatrixOne supports `READ_COMMITTED`; its session
isolation variable displays this as `READ-COMMITTED`.

Read the committed output with a new JDBC read when checking a pipeline:

```python
read_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "your_user",
    "password": "your_password",
}
readback = spark.read.jdbc(
    url="jdbc:mysql://127.0.0.1:6001/your_database",
    table="target_table",
    properties=read_properties,
)
```

The repository includes a runnable compatibility probe that checks the default
failure, the explicit `READ_COMMITTED` write across two writer partitions, and
readback through both a fresh Spark JDBC read and a separate SQL connection. It
places one row in each writer partition to exercise both JDBC writer tasks:
[`test/manual/spark_jdbc/README.md`](../../test/manual/spark_jdbc/README.md).

This option selects a supported isolation level for each Spark write. It does
not make a multi-partition Spark append atomic or change Spark's task retry
behavior.
