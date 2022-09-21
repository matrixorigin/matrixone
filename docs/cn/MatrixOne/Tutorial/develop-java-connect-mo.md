# **用 JDBC 连接器连接 MatrixOne 服务示例**

本教程示例将向你展示如何使用 JDBC 连接器连接 MatrixOne 服务。

## **开始前准备**

### 准备 1：安装基础软件

- 下载安装 [Java 语言 JDBC 连接器](https://dev.mysql.com/downloads/connector/j/)。

- 下载安装 [JDK](https://www.oracle.com/java/technologies/javase-downloads.html)。

- 下载安装 [Eclipse](http://www.eclipse.org/home/index.php)。

- 完成 MatrixOne [搭建](../Get-Started/install-standalone-matrixone.md)与[连接](../Get-Started/connect-to-matrixone-server.md)

### 准备 2：代码说明

加载驱动与连接数据库方式如下：

```
Class.forName("com.mysql.cj.jdbc.Driver");
conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test_demo?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC","root","password");
```

## **步骤 1：准备数据库和数据集**

在 MatrixOne 完成建数据库以及数据表：

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

## **步骤 2：在 Eclipse 导入 JDBC 连接器**

1. 启动 Eclipse，依次选择 **File > New > Java project**，命名为 *jdbc_demo*，点击 **Finish**，新项目 *jdbc_demo* 创建完成。
2. 右击项目 *jdbc_demo*，依次选择 **Built path > Add External Archives**，导入已下载好的 [Java 语言 JDBC 连接器](https://dev.mysql.com/downloads/connector/j/)。
3. 新建一个类：鼠标右键点击项目 *jdbc_demo* 下的 **src > New > Class**, 命名为 *demo*，提交即可。

## **步骤 3：连接 MatrixOne，执行 Java 程序**

```
package jdbc_demo;

import java.sql.*;

public class demo {

	    //test为数据库名称
	    // MySQL 8.0 以下版本选择
	    //static final String JdbcDriver = "com.mysql.jdbc.Driver";  
	    //static final String Url = "jdbc:mysql://localhost:6001/test";

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

## 执行结果

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
