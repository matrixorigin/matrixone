# SpringBoot 和 MyBatis CRUD 示例

本篇文档将介绍一个基于 **SpringBoot + Mybatis + MatrixOne+Intellij IDEA** 的简单示例，并实现基本的 CRUD 功能 。

## 开始之前

本篇教程涉及到的软件介绍如下：

* MyBatisMyBatis 是一款优秀的持久层框架，它支持自定义 SQL、存储过程以及高级映射。我们只需要关注项目中的 SQL 本身。

* Intellij IDEA：IntelliJ IDEA是一种商业化销售的 Java 集成开发环境（Integrated Development Environment，IDE）工具软件。它所拥有诸多插件，可以提高我们的工作效率。

* Maven：Maven是 Java 中功能强大的项目管理工具，可以根据 *pom.xml* 文件中的配置自动下载和导入 *Jar* 文件。这个特性减少了不同版本 Jar 文件之间的冲突。

* Spring：Spring是 Java 中最流行的框架之一，越来越多的企业使用 Spring 框架来构建他们的项目。Spring Boot 构建在传统的 Spring 框架之上。因此，它提供了 Spring 的所有特性，而且比 Spring 更易用。

## 配置环境

### 1. 安装构建 MatrixOne

按照步骤介绍完成[安装单机版 MatrixOne 0.6](../../../Get-Started/install-standalone-matrixone.md)，在 MySQL 客户端新建一个命名为 `test` 数据库。

```
mysql> CREATE DATABASE test;
```

### 2. 使用 IntelliJ IDEA 创建一个新的 Spring Boot 项目

选择 **Spring Initializer**，按需命名项目名称。

![image-20221026152318567](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026152318567.png)

选择如下依赖项：

- **Spring Web**
- **MyBatis Framework**
- **JDBC API**
- **MySQL Driver**

![image-20221026152447954](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026152447954.png)

点击 **Create** 创建项目。依赖项列在 *pom.xml* 文件中。通常你无需修改任何东西。

```
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>mybatis-demo</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>mybatis-demo</name>
    <description>mybatis-demo</description>

    <properties>
        <java.version>1.8</java.version>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>
        <spring-boot.version>2.3.7.RELEASE</spring-boot.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-jdbc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.mybatis.spring.boot</groupId>
            <artifactId>mybatis-spring-boot-starter</artifactId>
            <version>2.1.4</version>
        </dependency>

        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
            <exclusions>
                <exclusion>
                    <groupId>org.junit.vintage</groupId>
                    <artifactId>junit-vintage-engine</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
    </dependencies>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>${spring-boot.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                    <encoding>UTF-8</encoding>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>2.3.7.RELEASE</version>
                <configuration>
                    <mainClass>com.example.mybatisdemo.MybatisDemoApplication</mainClass>
                </configuration>
                <executions>
                    <execution>
                        <id>repackage</id>
                        <goals>
                            <goal>repackage</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

</project>

```

### 3. 修改 *application.properties* 文件

进入到 *resources* 文件目录下，配置 *application.properties* 文件，完成 MatrixOne 连接。

```
# Application Name
spring.application.name=MyBatisDemo
# Database driver
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
# Data Source name
spring.datasource.name=defaultDataSource

# Database connection url, modify to MatrixOne address and port, with paratemers
spring.datasource.url=jdbc:mysql://127.0.0.1:6001/test?characterSetResults=UTF-8&continueBatchOnError=false&useServerPrepStmts=true&alwaysSendSetIsolation=false&useLocalSessionState=true&zeroDateTimeBehavior=CONVERT_TO_NULL&failoverReadOnly=false&serverTimezone=Asia/Shanghai&socketTimeout=30000
# Database username and password
spring.datasource.username=dump
spring.datasource.password=111

# Mybatis mapper location
mybatis.mapper-locations=classpath:mapping/*xml
# Mybatis entity package
mybatis.type-aliases-package=com.example.mybatisdemo.entity
# Web application port
server.port=8080
```

## 编写代码

完成环境配置后，我们编写代码来实现一个简单的 CRUD 应用程序。

在完成编写编码后，你将拥有一个如下所示的项目结构。你可以预先创建这些包和 java 类。

我们将为这个演示应用程序编写创建、更新、插入、删除和选择操作。

![image-20221026155656694](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026155656694.png)

### 1. UserController.java

```
package com.example.mybatisdemo.controller;

import com.example.mybatisdemo.entity.User;
import com.example.mybatisdemo.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class UserController {

    String tableName = "user";
    @Autowired
    private UserService userService;

    @RequestMapping(value = "/create", produces = "application/json;charset=UTF-8", method = RequestMethod.GET)
    @ResponseBody
    public String createTable(){
        return userService.createTable(tableName);
    }

    @RequestMapping(value = "/selectUserByid", produces = "application/json;charset=UTF-8", method = RequestMethod.GET)
    @ResponseBody
    public String GetUser(User user){
        return userService.Sel(user).toString();
    }

    @RequestMapping(value = "/add", produces = "application/json;charset=UTF-8", method = RequestMethod.GET)
    public String Add(User user){
        return userService.Add(user);
    }

    @RequestMapping(value = "/update", produces = "application/json;charset=UTF-8", method = RequestMethod.GET)
    public String Update(User user){
        return userService.Update(user);
    }

    @RequestMapping(value = "/delete", produces = "application/json;charset=UTF-8", method = RequestMethod.GET)
    public String Delete(User user){
        return userService.Delete(user);
    }
}
```

### 2. User.java

```
package com.example.mybatisdemo.entity;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

public class User {
    private Integer id;
    private String username;
    private String password;
    private String address;

    public User(Integer id, String username, String password, String address) {
        this.id = id;
        this.username = username;
        this.password = password;
        this.address = address;
    }

    public Integer getId() {
        return id;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getAddress() {
        return address;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", address='" + address + '\'' +
                '}';
    }
}
```

### 3. UserMapper.java

```
package com.example.mybatisdemo.mapper;

import com.example.mybatisdemo.entity.User;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface UserMapper {

    int createTable(@Param("tableName") String tableName);

    User Sel(@Param("user")User user);

    int Add(@Param("user")User user);

    int Update(@Param("user")User user);

    int Delete(@Param("user")User user);

}
```

### 4. UserService.java

```
package com.example.mybatisdemo.service;

import com.example.mybatisdemo.entity.User;
import com.example.mybatisdemo.mapper.UserMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UserService {
    @Autowired
    UserMapper userMapper;

    public String createTable(String table){
        int a = userMapper.createTable(table);
        if (a == 1) {
            return "Create table failed";
        } else {
            return "Create table successfully";
        }
    }

    public User Sel(User user) {
        return userMapper.Sel(user);
    }

    public String Add(User user) {
        int a = userMapper.Add(user);
        if (a == 1) {
            return "Add user successfully";
        } else {
            return "Add user failed";
        }
    }

    public String Update(User user) {
        int a = userMapper.Update(user);
        if (a == 1) {
            return "Update user successfully";
        } else {
            return "Update user failed";
        }
    }

    public String Delete(User user) {
        int a = userMapper.Delete(user);
        if (a == 1) {
            return "Delete user successfully";
        } else {
            return "Delete user failed";
        }
    }

};
```

### 5. MyBatisDemoApplication.java

```
package com.example.mybatisdemo;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan("com.example.mybatisdemo.mapper")
@SpringBootApplication
public class MyBatisDemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyBatisDemoApplication.class, args);
    }
}
```

### 6. UserMapper.xml

```
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">

<mapper namespace="com.example.mybatisdemo.mapper.UserMapper">

    <resultMap id="BaseResultMap" type="com.example.mybatisdemo.entity.User">
        <result column="id" jdbcType="INTEGER" property="id"/>
        <result column="userName" jdbcType="VARCHAR" property="username"/>
        <result column="passWord" jdbcType="VARCHAR" property="password"/>
        <result column="realName" jdbcType="VARCHAR" property="address"/>
    </resultMap>

    <update id="createTable" parameterType="string">
        CREATE TABLE ${tableName} (
            `id` int(11) NOT NULL AUTO_INCREMENT,
            `username` varchar(255) DEFAULT NULL,
            `password` varchar(255) DEFAULT NULL,
            `address` varchar(255) DEFAULT NULL,
            PRIMARY KEY (`id`)
        );
    </update>

    <select id="Sel" resultType="com.example.mybatisdemo.entity.User">
        select * from user where 1=1
        <if test="user.id != null">
            AND id = #{user.id}
        </if>
    </select>

    <insert id="Add" parameterType="com.example.mybatisdemo.entity.User">
        INSERT INTO user
        <trim prefix="(" suffix=")" suffixOverrides=",">
            <if test="user.username != null">
                username,
            </if>
            <if test="user.password != null">
                password,
            </if>
            <if test="user.address != null">
                address,
            </if>
        </trim>
        <trim prefix="VALUES (" suffix=")" suffixOverrides=",">
            <if test="user.username != null">
                #{user.username,jdbcType=VARCHAR},
            </if>
            <if test="user.password != null">
                #{user.password,jdbcType=VARCHAR},
            </if>
            <if test="user.address != null">
                #{user.address,jdbcType=VARCHAR},
            </if>
        </trim>
    </insert>

    <update id="Update" parameterType="com.example.mybatisdemo.entity.User">
        UPDATE user
        <set>
            <if test="user.username != null">
                username = #{user.username},
            </if>
            <if test="user.password != null">
                password = #{user.password},
            </if>
            <if test="user.address != null">
                address = #{user.address},
            </if>
        </set>
        WHERE
        id=#{user.id}
    </update>

    <delete id="Delete"  parameterType="com.example.mybatisdemo.entity.User">
        DELETE FROM user WHERE id = #{user.id}
    </delete>

</mapper>
```

## 测试

构建并启动这个项目。

![image-20221026161226923](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026161226923.png)

当出现下面的消息时，表示应用程序已经正常启动，你可以打开浏览器并发送 HTTP 请求。

```
2022-10-26 16:13:24.030  INFO 60253 --- [           main] c.e.mybatisdemo.MyBatisDemoApplication   : Starting MyBatisDemoApplication on nandeng-macbookpro.local with PID 60253 (/Users/nandeng/IdeaProjects/MyBatisDemo/target/classes started by nandeng in /Users/nandeng/IdeaProjects/MyBatisDemo)
2022-10-26 16:13:24.035  INFO 60253 --- [           main] c.e.mybatisdemo.MyBatisDemoApplication   : No active profile set, falling back to default profiles: default
2022-10-26 16:13:25.415  INFO 60253 --- [           main] o.s.b.w.embedded.tomcat.TomcatWebServer  : Tomcat initialized with port(s): 8080 (http)
2022-10-26 16:13:25.421  INFO 60253 --- [           main] o.apache.catalina.core.StandardService   : Starting service [Tomcat]
2022-10-26 16:13:25.421  INFO 60253 --- [           main] org.apache.catalina.core.StandardEngine  : Starting Servlet engine: [Apache Tomcat/9.0.41]
2022-10-26 16:13:25.476  INFO 60253 --- [           main] o.a.c.c.C.[Tomcat].[localhost].[/]       : Initializing Spring embedded WebApplicationContext
2022-10-26 16:13:25.477  INFO 60253 --- [           main] w.s.c.ServletWebServerApplicationContext : Root WebApplicationContext: initialization completed in 1390 ms
2022-10-26 16:13:26.020  INFO 60253 --- [           main] o.s.s.concurrent.ThreadPoolTaskExecutor  : Initializing ExecutorService 'applicationTaskExecutor'
2022-10-26 16:13:26.248  INFO 60253 --- [           main] o.s.b.w.embedded.tomcat.TomcatWebServer  : Tomcat started on port(s): 8080 (http) with context path ''
2022-10-26 16:13:26.272  INFO 60253 --- [           main] c.e.mybatisdemo.MyBatisDemoApplication   : Started MyBatisDemoApplication in 2.669 seconds (JVM running for 3.544)
```

### 1. 测试新建表

![image-20221026161929338](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026161929338.png)

在 MySQL 客户端中，验证表是否已成功创建。

```
mysql> use test;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> show tables;
+----------------+
| tables_in_test |
+----------------+
| user           |
+----------------+
1 row in set (0.00 sec)

mysql> show create table user;
+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Table | Create Table                                                                                                                                                                                  |
+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| user  | CREATE TABLE `user` (
`id` INT NOT NULL AUTO_INCREMENT,
`username` VARCHAR(255) DEFAULT null,
`password` VARCHAR(255) DEFAULT null,
`address` VARCHAR(255) DEFAULT null,
PRIMARY KEY (`id`)
) |
+-------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)
```

### 2. 测试增加用户

![image-20221026162317800](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026162317800.png)

```
mysql> select * from user;
+------+----------+----------+----------+
| id   | username | password | address  |
+------+----------+----------+----------+
|    1 | tom      | 123456   | shanghai |
+------+----------+----------+----------+
1 row in set (0.00 sec)
```

### 3. 测试选择用户

![image-20221026162455058](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026162455058.png)

### 4. 测试更新用户

![image-20221026162613066](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026162613066.png)

```
mysql> select * from user;
+------+----------+----------+---------+
| id   | username | password | address |
+------+----------+----------+---------+
|    1 | tom      | 654321   | beijing |
+------+----------+----------+---------+
1 row in set (0.00 sec)
```

### 5. 测试删除用户

![image-20221026162756460](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026162756460.png)

```
mysql> select * from user;
Empty set (0.00 sec)
```
