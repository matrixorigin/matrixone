# **Data Types Overview**

MatrixOne Data types conforms with MySQL Data types definition.

Reference: <https://dev.mysql.com/doc/refman/8.0/en/data-types.html>

## **Integer Numbers**

|  Data Type   | Size  |  Min Value   | Max Value  |
|  ----  | ----  |  ----  | ----  |
| TINYINT  | 1 byte | 	-128  | 127 |
| SMALLINT  | 2 byte | -32768  | 32767 |
| INT  | 4 byte | 	-2147483648	  | 2147483647 |
| BIGINT  | 8 byte | -9223372036854775808	  | 9223372036854775807 |
| TINYINT UNSIGNED | 1 byte | 0	  | 255 |
| SMALLINT UNSIGNED | 2 byte | 0	  | 65535 |
| INT UNSIGNED | 4 byte | 0	  | 4294967295 |
| BIGINT UNSIGNED | 8 byte | 0	  | 18446744073709551615 |

## **Real Numbers**

|  Data Type   | Size  |  Precision   | Syntax |
|  ----  | ----  |  ----  | ----  |
| FLOAT32  | 4 byte | 	23 bits  | FLOAT |
| FLOAT64  | 8 byte |  53 bits  | DOUBLE |

## **String Types**

|  Data Type   | Size  | Syntax |
|  ----  | ----  |   ----  |
| String  | 24 byte | CHAR, VARCHAR |

## **Time and Date Types**

|  Data Type   | Size  | Resolution |  Min Value   | Max Value  | Precision |
|  ----  | ----  |   ----  |  ----  | ----  |   ----  |
| Date  | 4 byte | day | 1000-01-01  | 9999-12-31 | YYYY-MM-DD/YYYYMMDD |
| DateTime  | 8 byte | second | 0001-01-01 00:00:00.000000  | 9999-12-31 23:59:59.999999 | YYYY-MM-DD hh:mi:ssssss |
| TIMESTAMP|8 byte|second|1970-01-01 00:00:01.000000|2038-01-19 03:14:07.999999|YYYYMMDD hh:mi:ss.ssssss|

## **Bool**

|  Data Type   | Size  |
|  ----  | ----  |
| True  | 1 byte |
|False|1 byte|

## **Decimal Types(Beta)**

|  Data Type   | Size  |  Precision   | Syntax |
|  ----  | ----  |  ----  | ----  |
| Decimal64  | 8 byte | 	19 digits  | Decimal(N,S), N range(1,18), S range(0,N) |
| Decimal128  | 16 byte | 	38 digits  | Decimal(N,S), N range(19,38), S range(0,N) |

## **Examples**

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
> insert into calendar(a, b) values('20220202, '2022-02-02 00:10:30');
> insert into calendar(a, b) values('2022-02-02, '2022-02-02 00:10:30');

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
