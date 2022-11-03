# How to connect MatrixOne with ORMs

Apart from connecting with JDBC, more typically, we'll connect to our MySQL database using an Object Relational Mapping (ORM) Framework. We will introduce how to connect to MatrixOne with Spring Data JPA and MyBatis in this article.

## MyBatis

[MyBatis](https://github.com/mybatis/mybatis-3) was introduced in 2010 and is a SQL mapper framework with simplicity as its strength. In another tutorial, we talked about how to build a CRUD application with [MyBatis and Spring Boot](springboot-mybatis-crud-demo.md).Here, we'll focus on how to configure MyBatis with MatrixOne directly.

### 1. Add MyBatis-Spring-Boot-Starter in Pom.xml

MyBatis applications are built on top of the Spring Boot. For that, you need to add this module to pom.xml which is created when you choose a Maven project.

```
<dependency>
    <groupId>org.mybatis.spring.boot</groupId>
    <artifactId>mybatis-spring-boot-starter</artifactId>
    <version>2.1.4</version>
</dependency>
```

### 2. Add Configuration

You need to store some of the given parameters inside the `application.properties`. Five properties usually need to be modified:

- `spring.datasource.driver-class-name` : Driver name for MySQL connector.
- `spring.datasource.url`: JDBC connection URL with its parameters.
- `spring.datasource.username`: Database username.
- `spring.datasource.password`: Database password.
- `mybatis.mapper-locations` : Locations of Mapper XML config file.

MatrixOne's recommended configuration is as below:

```
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
spring.datasource.url=jdbc:mysql://127.0.0.1:6001/test?characterSetResults=UTF-8&continueBatchOnError=false&useServerPrepStmts=true&alwaysSendSetIsolation=false&useLocalSessionState=true&zeroDateTimeBehavior=CONVERT_TO_NULL&failoverReadOnly=false&serverTimezone=Asia/Shanghai&socketTimeout=30000
spring.datasource.username=dump
spring.datasource.password=111
mybatis.mapper-locations=classpath:mapping/*xml
```

!!! note
    JDBC connection URL with recommended configuration is necessary, otherwise the connection will fail.

## Spring Data JPA

Spring Data JPA is a robust framework that helps reduce boilerplate code and provides a mechanism for implementing basic CRUD operations via one of several predefined repository interfaces. In addition to this, it has many other useful features.

### 1. Add spring-boot-starter-data-jpa in Pom.xml

Spring Data JPA applications are built on top of the Spring Boot. For that, you need to add this module to pom.xml which is created when you choose a Maven project.

```
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
</dependency>
```

### 2. Add Configuration

You need to store some of the given parameters inside the `application.properties`. Five properties usually need to be modified:

- `spring.datasource.driver-class-name` : Driver name for MySQL connector.
- `spring.datasource.url`: JDBC connection URL with its parameters.
- `spring.datasource.username`: Database username.
- `spring.datasource.password`: Database password.
- `spring.jpa.properties.hibernate.dialect` : The SQL dialect makes Hibernate generate better SQL for the chosen database. MatrixOne only supports `org.hibernate.dialect.MySQLDialect`.

* `spring.jpa.hibernate.ddl-auto`：This property takes an enum that controls the schema generation in a more controlled way. The possible options and effects are in the following table. MatrixOne only supports `none` and `validate`.

| Option      | Effect                                                       |
| ----------- | ------------------------------------------------------------ |
| none        | No database Schema initialization                            |
| create      | Drops and creates the schema at the application startup. With this option, all your data will be gone on each startup. |
| create-drop | Creates schema at the startup and destroys the schema on context closure. Useful for unit tests. |
| validate    | Only checks if the Schema matches the Entities. If the schema doesn’t match, then the application startup will fail. Makes no changes to the database. |
| update      | Updates the schema only if necessary. For example, If a new field was added in an entity, then it will simply alter the table for a new column without destroying the data. |

MatrixOne's recommended configuration is as below:

```
spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver
spring.datasource.url=jdbc:mysql://127.0.0.1:6001/test?characterSetResults=UTF-8&continueBatchOnError=false&useServerPrepStmts=true&alwaysSendSetIsolation=false&useLocalSessionState=true&zeroDateTimeBehavior=CONVERT_TO_NULL&failoverReadOnly=false&serverTimezone=Asia/Shanghai&socketTimeout=30000
spring.datasource.username=dump
spring.datasource.password=111
spring.jpa.properties.hibernate.dialect = org.hibernate.dialect.MySQLDialect
spring.jpa.hibernate.ddl-auto = validate
```
