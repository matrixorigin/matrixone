# Build a simple Java CRUD demo with MatrixOne

!Note: The source code of this demo can be downloaded at [github](https://github.com/matrixorigin/matrixone_java_crud_example).  

## Setup your environment

Before you start, make sure you have downloaded and installed the following software.

* [MatrixOne 0.5.1](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/): Follow the installation instruction and launch MatrixOne 0.5.1. Create a database by MySQL client.

```
mysql> CREATE DATABASE TEST;
```

* [lntelIiJ IDEA(2022.2.1 or later version)](https://www.jetbrains.com/idea/download/).
* [JDK 8+ version](https://www.oracle.com/sg/java/technologies/javase/javase8-archive-downloads.html): Choose the version according to your OS.
* [MySQL JDBC connector 8.0+ version](https://dev.mysql.com/downloads/connector/j/): It's recommanded to download the platform independent version, and unzip the downloaded file.

!Note：We take IDEA as an IDE example to demonstrate the process, you are free to choose Eclipse or other IDE tools for the same purpose.

![image-20220927102516885](https://github.com/matrixorigin/artwork/blob/main/docs/reference/jdbc_download.png?raw=true)

## Initialize a new Java project

Launch IDEA, and create a new Java project as below:

![image-20220927104740221](https://github.com/matrixorigin/artwork/blob/main/docs/reference/jdbc_create_project.png?raw=true)

In your **Project Setting > Libraries**, import the *mysql-connector-java-8.0.xx.jar* file. 

![image-20220927104904770](https://github.com/matrixorigin/artwork/blob/main/docs/reference/jdbc_import_library.png?raw=true)

## Write  Java code to connect with MatrixOne

Firstly we create a Java class named as `JDBCUtils` as a connection utility. This class will serve as a tool to connect with MatrixOne and execute SQL queries.

```
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCUtils {
    private static String jdbcURL = "jdbc:mysql://127.0.0.1:6001/TEST";
    private static String jdbcUsername = "dump";
    private static String jdbcPassword = "111";

    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(jdbcURL, jdbcUsername, jdbcPassword);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return connection;
    }

    public static void printSQLException(SQLException ex) {
        for (Throwable e : ex) {
            if (e instanceof SQLException) {
                e.printStackTrace(System.err);
                System.err.println("SQLState: " + ((SQLException) e).getSQLState());
                System.err.println("Error Code: " + ((SQLException) e).getErrorCode());
                System.err.println("Message: " + e.getMessage());
                Throwable t = ex.getCause();
                while (t != null) {
                    System.out.println("Cause: " + t);
                    t = t.getCause();
                }
            }
        }
    }
}
```

Secondly we write example code for Create/Insert/Update/Delete operations with MatrixOne.

#### Create

```
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class Create {
    private static final String createTableSQL = "create table student (\r\n" + "  id int primary key,\r\n" +
            "  name varchar(20),\r\n" + "  email varchar(20),\r\n" + "  country varchar(20),\r\n" +
            "  age int\r\n" + "  );";

    public static void main(String[] argv) throws SQLException {
        Create createTable = new Create();
        createTable.createTable();
    }

    public void createTable() throws SQLException {

        System.out.println(createTableSQL);
        // Step 1: Establishing a Connection
        try (Connection connection = JDBCUtils.getConnection();
             // Step 2:Create a statement using connection object
             Statement statement = connection.createStatement();) {

            // Step 3: Execute the query or update query
            statement.execute(createTableSQL);
        } catch (SQLException e) {

            // print SQL exception information
            JDBCUtils.printSQLException(e);
        }

        // Step 4: try-with-resource statement will auto close the connection.
    }
}
```

Executing this code will create a table in the `TEST` database.

```
mysql> show create table student;
+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Table   | Create Table                                                                                                                                                                                        |
+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| student | CREATE TABLE `student` (
`id` INT DEFAULT NULL,
`name` VARCHAR(20) DEFAULT NULL,
`email` VARCHAR(20) DEFAULT NULL,
`country` VARCHAR(20) DEFAULT NULL,
`age` INT DEFAULT NULL,
PRIMARY KEY (`id`)
) |
+---------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)
```

#### Insert

```
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Insert {
    private static final String INSERT_STUDENT_SQL = "INSERT INTO student" +
            "  (id, name, email, country, age) VALUES " +
            " (?, ?, ?, ?, ?);";

    public static void main(String[] argv) throws SQLException {
        Insert insertTable = new Insert();
        insertTable.insertRecord();
    }

    public void insertRecord() throws SQLException {
        System.out.println(INSERT_STUDENT_SQL);
        // Step 1: Establishing a Connection
        try (Connection connection = JDBCUtils.getConnection();
             // Step 2:Create a statement using connection object
             PreparedStatement preparedStatement = connection.prepareStatement(INSERT_STUDENT_SQL)) {
            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, "Tony");
            preparedStatement.setString(3, "tony@gmail.com");
            preparedStatement.setString(4, "US");
            preparedStatement.setString(5, "20");

            System.out.println(preparedStatement);
            // Step 3: Execute the query or update query
            preparedStatement.executeUpdate();
        } catch (SQLException e) {

            // print SQL exception information
            JDBCUtils.printSQLException(e);
        }

        // Step 4: try-with-resource statement will auto close the connection.
    }

}
```

Execution result:

```
mysql> select * from student;
+------+------+----------------+---------+------+
| id   | name | email          | country | age  |
+------+------+----------------+---------+------+
|    1 | Tony | tony@gmail.com | US      |   20 |
+------+------+----------------+---------+------+
1 row in set (0.01 sec)
```

#### Update

```
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Update {
    private static final String UPDATE_STUDENT_SQL = "update student set name = ? where id = ?;";

    public static void main(String[] argv) throws SQLException {
        Update updateTable = new Update();
        updateTable.updateRecord();
    }

    public void updateRecord() throws SQLException {
        System.out.println(UPDATE_STUDENT_SQL);
        // Step 1: Establishing a Connection
        try (Connection connection = JDBCUtils.getConnection();
             // Step 2:Create a statement using connection object
             PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_STUDENT_SQL)) {
            preparedStatement.setString(1, "Ram");
            preparedStatement.setInt(2, 1);

            // Step 3: Execute the query or update query
            preparedStatement.executeUpdate();
        } catch (SQLException e) {

            // print SQL exception information
            JDBCUtils.printSQLException(e);
        }

        // Step 4: try-with-resource statement will auto close the connection.
    }
}

```

Execution result:

```
mysql> select * from student;
+------+------+----------------+---------+------+
| id   | name | email          | country | age  |
+------+------+----------------+---------+------+
|    1 | Ram  | tony@gmail.com | US      |   20 |
+------+------+----------------+---------+------+
1 row in set (0.00 sec)
```

#### SELECT

```
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Select {
    private static final String QUERY = "select id,name,email,country,age from student where id =?";

    public static void main(String[] args) {

        // using try-with-resources to avoid closing resources (boiler plate code)

        // Step 1: Establishing a Connection
        try (Connection connection = JDBCUtils.getConnection();

             // Step 2:Create a statement using connection object
             PreparedStatement preparedStatement = connection.prepareStatement(QUERY);) {
            preparedStatement.setInt(1, 1);
            System.out.println(preparedStatement);
            // Step 3: Execute the query or update query
            ResultSet rs = preparedStatement.executeQuery();

            // Step 4: Process the ResultSet object.
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String email = rs.getString("email");
                String country = rs.getString("country");
                String password = rs.getString("age");
                System.out.println(id + "," + name + "," + email + "," + country + "," + password);
            }
        } catch (SQLException e) {
            JDBCUtils.printSQLException(e);
        }
        // Step 4: try-with-resource statement will auto close the connection.
    }
}
```

Execution result:

![image-20220927113440917](https://github.com/matrixorigin/artwork/blob/main/docs/reference/jdbc_select.png?raw=true)
