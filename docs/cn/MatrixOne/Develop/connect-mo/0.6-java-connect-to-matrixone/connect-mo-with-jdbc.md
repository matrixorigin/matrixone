# 如何使用 JDBC 连接 MatrixOne

在 Java 中，我们可以通过 Java 代码使用 JDBC 连接器（Java Database Connectivity）连接到 MatrixOne。 JDBC 是用于数据库连接的标准 API 之一，使用它我们可以轻松地运行 SQL 语句并且从数据库中获取数据。

## 开始前准备

使用 MatrixOne 进行 Java 数据库连接前，需要完成以下下载安装任务：

1. 已完成[安装并启动 MatrixOne](../../../Get-Started/install-standalone-matrixone.md)。
2. 下载安装[JDK 8+ version](https://www.oracle.com/sg/java/technologies/javase/javase8-archive-downloads.html)。
3. 下载安装 Java 的 MySQL [connector](https://dev.mysql.com/downloads/connector/j/)（JAR 文件），在编译和运行 JDBC 代码时，JAR 文件必须在类路径中。

    - 系统环境推荐安装：选择 *Platform-independent* 版本。
    - 版本推荐安装：connector 版本 *mysql-connector-java-8.0.31.jar*。

4. 下载安装 JAVA IDE，本篇文档以 [Apache NetBeans 15](http://netbeans.org/downloads/index.html) 为例，你也可以下载其他 IDE 工具。

## 步骤

### 场景一：新建项目时使用 JDBC 连接 MatrixOne

1. 在 MatrixOne 新建一个名为 *test* 数据库和一个新的表 *t1*：

    ```sql
    create database test;
    use test;
    create table t1
    (
        code int primary key,
        title char(35)
    );
    ```

2. 将下载的 JAVA 连接器添加到 NetBeans 中：在左侧菜单栏中选择 **Services** 栏，点击 **Databases > Drivers**，鼠标右键单击 **MySQL(Connector/J driver)** 选择 **Customize**，在弹窗 **Customize JDBC Driver** 中点击 **Add**，添加 connector 的 *mysql-connector-java-8.0.31.jar* 包，**Drive Class** 为 *com.mysql.cj.jdbc.Driver*。

3. 鼠标右键单击 **MySQL(Connector/J driver)** 选择 **Connect Using**，在弹窗中填写如下配置：

    - Host：127.0.0.1
    - Port：6001
    - Database：test
    - User Name：dump
    - Password：111
    - Remmember password：勾选

4. 点击 **Test Connection**，提示 **Connection Succeeded**，表示连接成功。

### 场景二：在现有的项目中使用 JDBC 连接 MatrixOne

如果想要在现有的项目中使用，可以参考如下步骤:

1. 将 *.jar* 文件到 **[mo-tester](https://github.com/matrixorigin/mo-tester)** 仓库的 *lib* 目录。

2. 添加依赖项到 **[mo-tester](https://github.com/matrixorigin/mo-tester)** 仓库的 *pom.xml* 文件内：

    ```
    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
        <version>8.0.xx</version>
    </dependency>
    ```

3. 修改 JAVA 代码，可参考如下例子：

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

## 参考文档

有关使用 JDBC 开发应用时的 MatrixOne 支持情况，参见[MatrixOne 的 JDBC 功能支持列表](../../../Reference/Limitations/mo-jdbc-feature-list.md)。
