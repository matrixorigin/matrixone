# **数据类型**

MatrixOne 的数据类型与MySQL数据类型的定义一致，可参考：
<https://dev.mysql.com/doc/refman/8.0/en/data-types.html>

## **整数类型**



|  数据类型   | 存储空间  |  最小值  | 最大值  |
|  ----  | ----  |  ----  | ----  |
| TINYINT  | 1 byte | 	-128  | 127 |
| SMALLINT  | 2 byte | -32768  | 32767 |
| INT  | 4 byte | 	-2147483648	  | 2147483647 |
| BIGINT  | 8 byte | -9223372036854775808	  | 9223372036854775807 |
| TINYINT UNSIGNED | 1 byte | 0	  | 255 |
| SMALLINT UNSIGNED | 2 byte | 0	  | 65535 |
| INT UNSIGNED | 4 byte | 0	  | 4294967295 |
| BIGINT UNSIGNED | 8 byte | 0	  | 18446744073709551615 |

## **浮点类型**

|  数据类型   | 存储空间  |  精度  | 语法表示 |
|  ----  | ----  |  ----  | ----  |
| FLOAT32  | 4 byte | 	23 bits  | FLOAT |
| FLOAT64  | 8 byte |  53 bits  | DOUBLE |

## **字符串类型**

|  数据类型  | 存储空间  | 语法表示 |
|  ----  | ----  |   ----  |
| String  | 24 byte | CHAR, VARCHAR |

## **时间与日期**

|  数据类型  | 存储空间  | 精度 |  最小值   | 最大值  | 语法表示 |
|  ----  | ----  |   ----  |  ----  | ----  |   ----  |
| Date  | 4 byte | day | 1000-01-01  | 9999-12-31 | YYYY-MM-DD |
| DateTime  | 4 byte | second | 1970-01-01 00:00:00  | 2105-12-31 23:59:59 | YYYY-MM-DD hh:mm:ss |


## **定点类型Decimal(Beta)**

|  数据类型   | 存储空间  |  精度   | 语法表示 |
|  ----  | ----  |  ----  | ----  |
| Decimal64  | 8 byte | 	19位  | Decimal(N,S), N范围(1,18), S范围(0,N) |
| Decimal128  | 16 byte | 	38位  | Decimal(N,S), N范围(19,38), S范围(0,N) |

## **示例**

```sql
//Create a table named "numtable" with 3 attributes of an "int", a "float" and a "double"
> create table numtable(id int,fl float, dl double);

//Insert a dataset of int, float and double into table "numtable"
> insert into numtable values(3,1.234567,1.2345678912345678912);

// Create a table named "numtable" with 2 attributes of an "int" and a "float" up to 5 digits in total, of which 3 digits may be after the decimal point.
> create table numtable(id int,fl float(5,3));

//Insert a dataset of int, float into table "numtable"
> insert into numtable values(3,99.123);

//Create a table named "numtable" with 2 attributes of an "int" and a "float" up to 23 digits in total.
> create table numtable(id int,fl float(23));

//Insert a dataset of int, float into table "numtable"
> insert into numtable values(1,1.2345678901234567890123456789);

//Create a table named "numtable" with 4 attributes of an "unsigned tinyint", an "unsigned smallint", an "unsigned int" and an "unsigned bigint"
> create table numtable(a tinyint unsigned, b smallint unsigned, c int unsigned, d bigint unsigned);

//Insert a dataset of unsigned (tinyint, smallint, int and bigint) into table "numtable"
> insert into numtable values(255,65535,4294967295,18446744073709551615);

//Create a table named "names" with 2 attributes of a "varchar" and a "char"
> create table names(name varchar(255),age char(255));

//Insert a data of "varchar" and "char" into table "names" 
> insert into names(name, age) values('Abby', '24');

//Create a table named "calendar" with 2 attributes of a "date" and a "datetime"
> create table calendar(a date, b datetime);

//Insert a data of "date" and "datetime" into table "calendar" 
> insert into calendar(a, b) values('20220202', '2022-02-02 00:10:30');
> insert into calendar(a, b) values('2022-02-02', '2022-02-02 00:10:30');


//Create a table named "decimalTest" with 2 attribute of a "decimal" and b "decimal"
> create table decimalTest(a decimal(6,3), b decimal(24,18));
> insert into decimalTest values(123.4567, 123456.1234567891411241355);
> select * from decimalTest;
+---------+---------------------------+
| a       | b                         |
+---------+---------------------------+
| 123.456 | 123456.123456789141124135 |
+---------+---------------------------+
```
