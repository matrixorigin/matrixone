# **Using JDBC connector connect to MatrixOne Server**

This tutorial will show you how to connect to a MatrixOne service using a JDBC connector.

## Before you start

### Preparation 1: Install basic software

- Download and install [JDBC Connector](https://dev.mysql.com/downloads/connector/j/).

- Download and install [JDK](https://www.oracle.com/java/technologies/javase-downloads.html).

- Download and install [Eclipse](http://www.eclipse.org/home/index.php).

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

	    //The database name is "test"
	    // MySQL 8.0 and earlier version, configuration as below:
	    //static final String JdbcDriver = "com.mysql.jdbc.Driver";  
	    //static final String Url = "jdbc:mysql://localhost:3306/test";

	    // MySQL 8.0 and later version, configuration as below:
	    static final String JdbcDriver = "com.mysql.cj.jdbc.Driver";  
	    static final String Url = "jdbc:mysql://127.0.0.1:6001/test?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";


	    //Enter the username and password to connect to database
	    static final String User = "dump";//Enter the database name
	    static final String PassWord = "111";//Enter the password

	    public static void main(String[] args) {
	        Connection conn = null;
	        Statement stmt = null;
	        try{
	            // Register JDBC driver
	            Class.forName(JdbcDriver);

	            // Open the link
	            System.out.println("Connect to database...");
	            conn = DriverManager.getConnection(Url,User,PassWord);

	            // Run query
	            System.out.println("Enter the sql statements and run sql...");
	            stmt = conn.createStatement();
	            String sql;
	            sql = "select * from user";// Enter the sql statements
	            //Run sql
	            ResultSet rs = stmt.executeQuery(sql);

	            // Export database
	            while(rs.next()){
	                // Retrieval by field
	                int id  = rs.getInt("id");//Get id value
	                String name = rs.getString("user_name");//Get user_name value
	                String sex = rs.getString("sex");//Get sex value

	                // Export data
	                System.out.println("id: " + id);
	                System.out.println("Name: " + name);
	                System.out.println("Sex: " + sex);
	            }
	            // Close
	            rs.close();
	            stmt.close();
	            conn.close();
	        }catch(SQLException se){
	            // Fix JDBC error
	            se.printStackTrace();
	        }catch(Exception e){
	            // Fix Class.forName error
	            e.printStackTrace();
	        }finally{
	            // Close the source
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
	        System.out.println("\nSuccessed!");
	    }
}
```

## Expected result

```
Connect to database...
Enter the sql statements and run sql...
id: 1
Name: weder
Sex: man
id: 2
Name: tom
Sex: man
id: 3
Name: wederTom
Sex: man

Successed!
```
