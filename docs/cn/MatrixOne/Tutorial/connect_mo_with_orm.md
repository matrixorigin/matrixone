# 如何使用 ORMs 连接 MatrixOne

除了使用 JDBC 连接 MatrixOne 之外，我们还可以使用对象关系映射(ORM)框架连接到 MySQL 数据库。在本篇文档中，介绍了如何使用 Spring Data JPA 和 MyBatis 连接到 MatrixOne。

## MyBatis

[MyBatis](https://github.com/mybatis/mybatis-3) 是 SQL 映射框架，它的优点是简单易用。在另一个教程中，我们讨论了如何使用 [MyBatis 和 Spring Boot](springboot-mybatis-crud-demo.md) 构建一个 CRUD 应用程序。在本篇文档中，将介绍如何直接使用 MatrixOne 配置MyBatis。

### 1. 在 *Pom.xml* 中添加 *MyBatis-Spring-Boot-Starter*

在 Spring Boot 上构建 MyBatis 应用程序，你需要将 *MyBatis-Spring-Boot-Starter* 模块添加到 *pom.xml* 中，*MyBatis-Spring-Boot-Starter* 模块则是在选择 Maven 项目时进行创建的。

```
<dependency>
    <groupId>org.mybatis.spring.boot</groupId>
    <artifactId>mybatis-spring-boot-starter</artifactId>
    <version>2.1.4</version>
</dependency>
```

### 2. 添加配置

在 `application.properties` 中需要修改的参数如下，其余参数可以保存默认值：

- `spring.datasource.driver-class-name`：MySQL 连接器的驱动程序名称。
- `spring.datasource.url`：JDBC 连接 URL 参数。
- `spring.datasource.username`：数据库用户名。
- `spring.datasource.password`：数据库密码。
- `mybatis.mapper-locations`：Mapper XML 配置文件的位置。

MatrixOne 中推荐配置如下：

```
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
spring.datasource.url=jdbc:mysql://127.0.0.1:6001/test?characterSetResults=UTF-8&continueBatchOnError=false&useServerPrepStmts=true&alwaysSendSetIsolation=false&useLocalSessionState=true&zeroDateTimeBehavior=CONVERT_TO_NULL&failoverReadOnly=false&serverTimezone=Asia/Shanghai&socketTimeout=30000
spring.datasource.username=dump
spring.datasource.password=111
mybatis.mapper-locations=classpath:mapping/*xml
```

!!! note
    需要使用推荐配置的 JDBC 连接 URL，否则将导致连接失败。

## Spring Data JPA

Spring Data JPA 是 Spring 基于 ORM 框架、JPA 规范的基础上封装的一套 JPA 应用框架，可使开发者用极简的代码即可实现对数据库的访问和操作，它有助于减少样板代码，并提供了一种通过几个预定义的存储库接口之一实现基本 CRUD 操作的机制，并且它也提供了包括增删改查等在内的常用功能，且易于扩展。

### 1. 在 *Pom.xml* 中添加 *spring-boot-starter-data-jpa*

在 Spring Boot 上构建 Spring Data JPA 应用程序，你需要将 *spring-boot-starter-data-jpa* 模块添加到 *pom.xml* 中，*spring-boot-starter-data-jpa* 模块则是在选择 Maven 项目时进行创建的。

```
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
</dependency>
```

### 2. 添加配置

在 `application.properties` 中需要修改的参数如下，其余参数可以保存默认值：

- `spring.datasource.driver-class-name`：MySQL 连接器的驱动程序名称。
- `spring.datasource.url`：JDBC 连接 URL 参数。
- `spring.datasource.username`：数据库用户名。
- `spring.datasource.password`：数据库密码。
- `spring.jpa.properties.hibernate.dialect`：*SQL dialect* （即 SQL 方言）使 Hibernate 为所选数据库生成更好的 SQL。MatrixOne 当前仅支持 `org.hibernate.dialect.MySQLDialect`。

* `spring.jpa.hibernate.ddl-auto`：`spring.jpa.hibernate.ddl-auto` 属性采用一个枚举，该枚举以更可控的方式控制模式生成。可能的选项和效果如下表所示。MatrixOne 当前仅支持 *none* 和 *validate*。

| 选项      | 效果                                                       |
| ----------- | ------------------------------------------------------------ |
| none        | 无数据库架构初始化                            |
| create      | 在应用程序启动时删除并创建模式。使用此选项，每次启动时你所有的数据都会消失。 |
| create-drop | 在启动时创建模式并在上下文关闭时销毁模式。可用于单元测试。 |
| validate    | 仅检查模式是否与实体匹配。如果模式不匹配，则应用程序启动将失败。不更改数据库。 |
| update      | 仅在必要时更新模式。例如，如果在实体中添加了一个新字段，那么它将简单地为新列更改表，而不会破坏数据。 |

MatrixOne 中推荐配置如下：

```
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
spring.datasource.url=jdbc:mysql://127.0.0.1:6001/test?characterSetResults=UTF-8&continueBatchOnError=false&useServerPrepStmts=true&alwaysSendSetIsolation=false&useLocalSessionState=true&zeroDateTimeBehavior=CONVERT_TO_NULL&failoverReadOnly=false&serverTimezone=Asia/Shanghai&socketTimeout=30000
spring.datasource.username=dump
spring.datasource.password=111
spring.jpa.properties.hibernate.dialect = org.hibernate.dialect.MySQLDialect
spring.jpa.hibernate.ddl-auto = validate
```
