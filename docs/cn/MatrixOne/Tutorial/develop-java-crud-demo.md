# Java CRUD 示例

!!! note
    本篇文档所介绍的演示程序的源代码下载地址为：[github](https://github.com/matrixorigin/matrixone_java_crud_example)。

## 配置环境

在开始之前，请确保已经下载并安装了以下软件。

* 安装部署并启动 [MatrixOne 0.5.1](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/)，通过 MySQL 客户端创建数据库。

```
mysql> CREATE DATABASE TEST;
```

* 下载安装[lntelIiJ IDEA(2022.2.1 or later version)](https://www.jetbrains.com/idea/download/)。
* 根据你的系统环境选择[JDK 8+ version](https://www.oracle.com/sg/java/technologies/javase/javase8-archive-downloads.html)版本进行下载安装。
* [MySQL JDBC connector 8.0+ version](https://dev.mysql.com/downloads/connector/j/)：推荐下载平台独立版本，并解压下载文件。

!!! note
     我们使用 IDEA 作为一个 IDE 示例来演示这个过程，你可以自由地选择 Eclipse 或其他 IDE 工具实践。

![image-20220927102516885](https://github.com/matrixorigin/artwork/blob/main/docs/reference/jdbc_download.png?raw=true)

## 初始化一个新的 Java 项目

启动 IDEA，并创建一个新的 Java 项目，如下所示:

![image-20220927104740221](https://github.com/matrixorigin/artwork/blob/main/docs/reference/jdbc_create_project.png?raw=true)

进入菜单 **Project Setting > Libraries**，导入 *mysql-connector-java-8.0.xx.jar* 文件。

![image-20220927104904770](https://github.com/matrixorigin/artwork/blob/main/docs/reference/jdbc_import_library.png?raw=true)

## 编写 Java 代码连接 MatrixOne

首先，创建一个名为 *JDBCUtils* 的 Java 类作为连接实用程序。这个类将作为连接 MatrixOne 和执行 SQL 查询的工具。

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

其次，我们用 MatrixOne 编写创建、插入、更新和删除操作的示例代码。

#### 创建

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

执行上述代码将在 *TEST* 数据库中创建一个表。

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

#### 插入

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

执行结果：

```sql
mysql> select * from student;
+------+------+----------------+---------+------+
| id   | name | email          | country | age  |
+------+------+----------------+---------+------+
|    1 | Tony | tony@gmail.com | US      |   20 |
+------+------+----------------+---------+------+
1 row in set (0.01 sec)
```

#### 升级数据

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

运行结果：

```sql
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

运行结果：

![image-20220927113440917](https://github.com/matrixorigin/artwork/blob/main/docs/reference/jdbc_select.png?raw=true)
