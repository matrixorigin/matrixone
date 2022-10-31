# Build a SpringBoot+MyBatis CRUD demo with MatrixOne

This tutorial completes a simple Demo based on **SpringBoot+Mybatis+MatrixOne+Intellij IDEA** , and realizes the basic CRUD function.

## Before you start

A brief introduction about these softwares concerned:

* MyBatis: It is a popular persistence framework that can customize SQL and support for complex reports and advanced mappings. We only need to focus on the SQL itself in our project.
* Intellij IDEA: IntelliJ IDEA is a popular IDE for Java developers. It has a lot of plugins that can enhance our efficiency.
* Maven: Maven is a powerful management tool in Java that can automatically download and import Jar file according to the configuration in the pom.xml file. This feature reduces the conflicts between different versions of Jar files.
* Spring: Spring is one of the most popular frameworks in Java and more and more enterprise is using the Spring framework to build their project. Spring Boot is built on top of the conventional spring framework. So, it provides all the features of spring and is yet easier to use than spring.

## Set up environment

### 1. Install and Launch MatrixOne

Follow the [installation instruction and launch MatrixOne 0.6](../Get-Started/install-standalone-matrixone.md). Create a database `test` by MySQL client.

```
mysql> CREATE DATABASE test;
```

### 2. Create a new Spring Boot Project Using IntelliJ IDEA

Choose **Spring Initializer**, and name the project as you want.

![image-20221026152318567](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026152318567.png)

Choose **Spring Web**, **MyBatis Framework**, **JDBC API** and **MySQL Driver** as dependencies for this project.

![image-20221026152447954](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026152447954.png)

Click **Create**,  the project will be created. The dependencies are listed in the *pom.xml* file. Usually you don't need to modify anything.

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

### 3. Modify the *application.properties* file

Under *resources* folder, the MatrixOne connection need to be configured in *application.properties* file.

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

## Write Code

After setting up the environment, we write code to implement a simple CRUD application. After finishing coding, you'll have a project structure as below. You can create these packages and java class in advance. We will code the Create, Update, Insert, Delete, Select operations for this demo application.

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

## Test

Build and launch this project.

![image-20221026161226923](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026161226923.png)

When you see the following messages, the application is well launched and you can open your browser and send HTTP request.

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

### 1. Test Create Table

![image-20221026161929338](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026161929338.png)

In MySQL client, we can verify if the table has been successfully created.

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

### 2. Test Add User

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

### 3. Test Select User

![image-20221026162455058](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026162455058.png)

### 4. Test Update User

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

### 5. Test Delete User

![image-20221026162756460](https://github.com/matrixorigin/artwork/blob/main/docs/tutorial/mybatis/image-20221026162756460.png)

```
mysql> select * from user;
Empty set (0.00 sec)
```
