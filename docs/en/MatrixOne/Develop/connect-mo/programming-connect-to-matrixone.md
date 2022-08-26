# Using programming connect to the MatrixOne server

MatrixOne now supports the following programming connections to the MatrixOne server:

- Java
- Python

## Before you start

Make sure you have already [installed and launched MatrixOne](../../Get-Started/install-standalone-matrixone.md).

## Using JDBC connector connect to MatrixOne

1. Download and install [Java JDBC Connector](https://dev.mysql.com/downloads/connector/j/)。

2. Call `com.mysql.cj.jdbc.Driver` in your application.

    !!! info
        For the example about using JDBC connect to MatrixOne, see[Using JDBC connector connect to MatrixOne](../../Tutorial/develop-java-connect-mo.md)。

## Using Python pymysql connect to MatrixOne

1. Download and install pymysql tool:

    ```
    pip3 install pymysql
    ```

2. Call `import pymysql` in your application, for more information on  `pymysql`, see <https://pypi.org/project/PyMySQL/>.

    !!! info
         For the example about usingpymysql conn to MatrixOne, see[Build a simple stock analysis Python App with MatrixOne](../../Tutorial/develop-python-application.md)。

<!--执行py脚本，1. python 脚本。或者是编程环境下，或者是终端，执行python脚本；2. 相应的数据库建表 3. python脚本完成对数据库的执行（操作） python3+python文件名称完成对数据库的操作-->
