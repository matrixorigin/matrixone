# **CREATE DATABASE**

## **语法说明**
`CREATE DATABASE` 语句同于创建一个数据库。

## **语法结构**

```
> CREATE DATABASE [IF NOT EXISTS] <database_name> [create_option] ...

> create_option: [DEFAULT] {
	CHARACTER SET [=] charset_name
  | COLLATE [=] collation_name
  | ENCRYPTION [=] {'Y' | 'N'}
}
```

#### 语法图:

![Create Database Diagram](https://github.com/matrixorigin/artwork/blob/main/docs/reference/create_database_statement.png?raw=true)

## **示例**
```
> CREATE DATABASE test01;

> CREATE DATABASE IF NOT EXISTS test01;

> CREATE DATABASE test03 DEFAULT CHARACTER SET utf8 collate utf8_general_ci ENCRYPTION 'Y';

> CREATE DATABASE test04 CHARACTER SET=utf8 collate=utf8_general_ci ENCRYPTION='N';
```

## **限制**
 
目前只支持 `UTF-8` 字符集。