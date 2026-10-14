# Spark JDBC isolation probe

This probe reproduces the Spark JDBC write-isolation compatibility boundary
with Spark 3.5.3 and MySQL Connector/J 8.0.33. It verifies that a writer with
Spark's default isolation fails with MatrixOne's explicit unsupported-level
error and inserts no rows, then writes the same two rows with
`isolationLevel=READ_COMMITTED` across two non-empty writer partitions and reads
them back through Spark and a separate JDBC connection. The probe creates uniquely named
tables and removes them when it exits.

Use an isolated MatrixOne instance and a dedicated database. Set the connection
environment variables without placing credentials in the source:

```sh
export MO_JDBC_URL='jdbc:mysql://127.0.0.1:6001/spark_jdbc_issue_29838'
export MO_USER='your_user'
export MO_PASSWORD='your_password'
export MYSQL_CONNECTOR_JAR='/absolute/path/to/mysql-connector-j-8.0.33.jar'
```

Create the database before running the probe. Install the pinned PySpark and
Connector/J dependencies, then launch it with both driver and executor
classpath entries for Connector/J:

```sh
python3 -m venv /tmp/spark-jdbc-29838-venv
/tmp/spark-jdbc-29838-venv/bin/pip install pyspark==3.5.3
curl -fL \
  https://repo.maven.apache.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar \
  -o /tmp/mysql-connector-j-8.0.33.jar
export MYSQL_CONNECTOR_JAR=/tmp/mysql-connector-j-8.0.33.jar
export PYSPARK_DRIVER_PYTHON=/tmp/spark-jdbc-29838-venv/bin/python
export PYSPARK_PYTHON=/tmp/spark-jdbc-29838-venv/bin/python

/tmp/spark-jdbc-29838-venv/bin/spark-submit \
  --master 'local[2]' \
  --jars "$MYSQL_CONNECTOR_JAR" \
  --driver-class-path "$MYSQL_CONNECTOR_JAR" \
  test/manual/spark_jdbc/spark_jdbc_isolation_probe.py
```

The command can be run again to check reuse and cleanup. It prints the MatrixOne
version, Spark version, and the verified default failure and `READ_COMMITTED`
readback. The probe fails if the default write succeeds, if an unrelated error
causes the default write to fail, if any default rows were inserted, or if the
explicit-isolation write/readback differs from the expected rows.
