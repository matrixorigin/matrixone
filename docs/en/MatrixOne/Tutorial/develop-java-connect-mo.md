# **Using JDBC connector connect to MatrixOne Server**

This tutorial will show you how to connect to a MatrixOne service using a JDBC connector.

## Before you start

### Preparation 1: Install basic software

- Download and install [JDBC Connector](https://dev.mysql.com/downloads/connector/j/)。

- Download and install [JDK](https://www.oracle.com/java/technologies/javase-downloads.html)。

- Download and install [Eclipse](http://www.eclipse.org/home/index.php)。

- Make sure you have already [installed and launched MatrixOne](../Get-Started/install-standalone-matrixone.md).

### Preparation 2: command Description

Loading the driver and connecting to the MatriOne are as below:

```
Class.forName("com.mysql.cj.jdbc.Driver");
conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test_demo?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC","root","password");
```

## **Step 1: Prepare database and dataset**

Create database and tables in MatrixOne:

```sql
CREATE DATABASE test;
USE  test;
CREATE TABLE `user` (`id` int(11) ,`user_name` varchar(255) ,`sex` varchar(255));
insert into user(id,user_name,sex) values('1', 'weder', 'man'), ('2', 'tom', 'man'), ('3', 'wederTom', 'man');
select * from user;
+------+-----------+------+
| id   | user_name | sex  |
+------+-----------+------+
|    1 | weder     | man  |
|    2 | tom       | man  |
|    3 | wederTom  | man  |
+------+-----------+------+            
```

## **Step 2: Import JDBC Connector into Eclipse**

1. Start Eclipse and select **File > New > Java Project**, name the java project *jdbc_demo*, click **Finish**.

2. Right-click the project *jdbc_demo* and select **Built Path > Add External Archives**, import the downloaded JDBC connector (<https://dev.mysql.com/downloads/connector/j/>).

3. Create a new class: right-click on the project *jdbc_demo*, select **src > New > Class**, name it *demo*, and click **Submit**.

## **Step 3: Connect to MatrixOne and Run Java**

```
package jdbc_demo;

import java.sql.*;

public class demo {

	    //test为数据库名称
	    // MySQL 8.0 以下版本选择
	    //static final String JdbcDriver = "com.mysql.jdbc.Driver";  
	    //static final String Url = "jdbc:mysql://localhost:3306/test";

	    // MySQL 8.0 以上版本选择
	    static final String JdbcDriver = "com.mysql.cj.jdbc.Driver";  
	    static final String Url = "jdbc:mysql://127.0.0.1:6001/test?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";


	    //输入连接数据库的用户名与密码
	    static final String User = "dump";//输入你的数据库库名
	    static final String PassWord = "111";//输入你的数据库连接密码

	    public static void main(String[] args) {
	        Connection conn = null;
	        Statement stmt = null;
	        try{
	            // 注册 JDBC 驱动
	            Class.forName(JdbcDriver);

	            // 打开链接
	            System.out.println("连接数据库...");
	            conn = DriverManager.getConnection(Url,User,PassWord);

	            // 执行查询
	            System.out.println("输入sql语句后并执行...");
	            stmt = conn.createStatement();
	            String sql;
	            sql = "select * from user";// 这里填写需要的sql语句
	            //执行sql语句
	            ResultSet rs = stmt.executeQuery(sql);

	            // 展开结果集数据库
	            while(rs.next()){
	                // 通过字段检索
	                int id  = rs.getInt("id");//获取id值
	                String name = rs.getString("user_name");//获取user_name值
	                String sex = rs.getString("sex");//获取sex值

	                // 输出数据
	                System.out.println("id: " + id);
	                System.out.println("名字: " + name);
	                System.out.println("性别: " + sex);
	            }
	            // 完成后关闭
	            rs.close();
	            stmt.close();
	            conn.close();
	        }catch(SQLException se){
	            // 处理 JDBC 错误
	            se.printStackTrace();
	        }catch(Exception e){
	            // 处理 Class.forName 错误
	            e.printStackTrace();
	        }finally{
	            // 关闭资源
	            try{
	                if(stmt!=null) stmt.close();
	            }catch(SQLException se2){
	            }
	            try{
	                if(conn!=null) conn.close();
	            }catch(SQLException se){
	                se.printStackTrace();
	            }
	        }
	        System.out.println("\n执行成功！");
	    }
}
```

## Expected result

```
连接数据库...
输入sql语句后并执行...
id: 1
名字: weder
性别: man
id: 2
名字: tom
性别: man
id: 3
名字: wederTom
性别: man

执行成功！
```
