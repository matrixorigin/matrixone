# Connecting to MatrixOne with Python

MatrixOne supports Python connection.

!!! Note
    MatrixOne only supports `pymysql` driver in 0.5 release, `sqlalchemy` and `mysql-connector` are not supported yet.

## Before you start

Make sure you have already [installed and launched MatrixOne](../../Get-Started/install-standalone-matrixone.md).

## Using Python pymysql connect to MatrixOne

1. Download and install pymysql tool:

    ```
    pip3 install pymysql
    ```

2. Call `import pymysql` in your application, for more information on  `pymysql`, see <https://pypi.org/project/PyMySQL/>.

## Reference

For the example about using pymysql conn to MatrixOne, see[Build a simple stock analysis Python App with MatrixOne](../../Tutorial/develop-python-application.md).
