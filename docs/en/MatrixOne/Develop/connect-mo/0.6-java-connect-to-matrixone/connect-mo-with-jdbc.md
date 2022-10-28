# How to connect MatrixOne with JDBC

In Java, we can connect to MatrixOne with JDBC(Java Database Connectivity) through the Java code. JDBC is one of the standard APIs for database connectivity, using it we can easily run our query, statement, and also fetch data from the database.

## Before you start

Prerequisite to understand Java Database Connectivity with MatrixOne, make sure you have installed these items as below:

1. Make sure you have already [installed and launched MatrixOne](../../../Get-Started/install-standalone-matrixone.md).
2. Make sure you have already installed [JDK 8+ version](https://www.oracle.com/sg/java/technologies/javase/javase8-archive-downloads.html).
3. To set up the connectivity user should have MySQL Download and install [JDBC Connector](https://dev.mysql.com/downloads/connector/j/) to the Java (JAR file), the ‘JAR’ file must be in classpath while compiling and running the code of JDBC.

    - *Platform-independent* Operating System is recommended.
    - connector version *mysql-connector-java-8.0.31.jar* is recommended.

4. Make sure you have already installed JAVA IDE, this document uses [Apache NetBeans 15](http://netbeans.org/downloads/index.html) as an example, you can also download other IDE.

## Steps

### Scenario 1: Use JDBC to connect to MatrixOne when creating a new project

1. Create a new database named *test* or other names you want in MatrixOne and create a new table named *t1*:

    ```sql
    create database test;
    use test;
    create table t1
    (
        code int primary key,
        title char(35)
    );
    ```

2. Add the downloaded connector into NetBeans. Select **Services > Databases > Drivers**, right click **MySQL(Connector/J driver)** and choose **Customize** in the Services sheet, click **Add**. add the *mysql-connector-java-8.0.31.jar*, enter *com.mysql.cj.jdbc.Driver* into Drive Class.

3. Right click **MySQL(Connector/J driver)** and choose **Connect Using** to configure as following:

    - Host: 127.0.0.1
    - Port: 6001
    - Database: test
    - User Name: dump
    - Password: 111
    - Remmember password: Yes

4. Click **Test Connection**， when the test **Connection Succeeded**, it means you can connect your MatrixOne with JDBC.

### Scenario 2: Use JDBC to connect to MatrixOne when using a existing project

If you want to use the connector in your existing project, you can follow these steps:

1. Add this *mysql-connector-java-8.0.31.jar* into the *lib* directory of the **[mo-tester](https://github.com/matrixorigin/mo-tester)** repository.

2. Add dependency into *pom.xml* file of the **[mo-tester](https://github.com/matrixorigin/mo-tester)** repository.

    ```
    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
        <version>8.0.xx</version>
    </dependency>
    ```

3. Add the following statements as example:

```
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

Connection conn = null;
...
try {
    conn =
       DriverManager.getConnection("jdbc:mysql://localhost:6001/test" +
                                   "user=account_name:user_name&password=your_password");

    // Do something with the Connection

   ...
} catch (SQLException ex) {
    // handle any errors
    System.out.println("SQLException: " + ex.getMessage());
    System.out.println("SQLState: " + ex.getSQLState());
    System.out.println("VendorError: " + ex.getErrorCode());
}
```

## Reference

For more information on MatrixOne support when developing applications using JDBC, see [JDBC supported features list in MatrixOne](../../../Reference/Limitations/mo-jdbc-feature-list.md).
