# **SUBSTRING()**

## **Description**

SUBSTR() is a synonym of SUBSTRING().
The forms without a len argument return a substring from string str starting at position pos. The forms with a len argument return a substring len characters long from string str, starting at position pos.

## **Syntax**

```
> SUBSTRING(str,pos) 
> SUBSTR(str,pos,len) 
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| str | Required. CHAR and VARCHAR are both supported |
| pos | Required. |
| len | Optional. |


## **Examples**


```SQL
> CREATE TABLE IF NOT EXISTS t1 (
pub_id varchar(8) COLLATE latin1_general_ci NOT NULL DEFAULT '',
pub_name varchar(50) COLLATE latin1_general_ci NOT NULL DEFAULT '',
pub_city varchar(25) COLLATE latin1_general_ci NOT NULL DEFAULT '',
country varchar(25) COLLATE latin1_general_ci NOT NULL DEFAULT '',
country_office varchar(25) COLLATE latin1_general_ci NOT NULL DEFAULT '',
no_of_branch int NOT NULL DEFAULT 0,
estd date NOT NULL DEFAULT '2000-01-01'
);

> INSERT INTO t3 (pub_id, pub_name, pub_city, country, country_office, no_of_branch, estd) VALUES
('P001', 'Jex Max Publication', 'New York', 'USA', 'New York', 15, '1969-12-25'),
('P002', 'BPP Publication', 'Mumbai', 'India', 'New Delhi', 10, '1985-10-01'),
('P003', 'New Harrold Publication', 'Adelaide', 'Australia', 'Sydney', 6, '1975-09-05'),
('P004', 'Ultra Press Inc.', 'London', 'UK', 'London', 8, '1948-07-10'),
('P005', 'Mountain Publication', 'Houstan', 'USA', 'Sun Diego', 25, '1975-01-01'),
('P006', 'Summer Night Publication', 'New York', 'USA', 'Atlanta', 10, '1990-12-10'),
('P007', 'Pieterson Grp. of Publishers', 'Cambridge', 'UK', 'London', 6, '1950-07-15'),
('P008', 'Novel Publisher Ltd.', 'New Delhi', 'India', 'Bangalore', 10, '2000-01-01');

> SELECT pub_name, SUBSTR(pub_name,4,5) FROM t1 WHERE country='USA';
+--------------------------+------------------------+
| pub_name                 | substr(pub_name, 4, 5) |
+--------------------------+------------------------+
| Jex Max Publication      |  Max                   |
| Mountain Publication     | ntain                  |
| Summer Night Publication | mer N                  |
+--------------------------+------------------------+
3 rows in set (0.04 sec)

> SELECT pub_name, SUBSTR(pub_name,5) FROM t1 WHERE country='USA';
+--------------------------+----------------------+
| pub_name                 | substr(pub_name, 5)  |
+--------------------------+----------------------+
| Jex Max Publication      | Max Publication      |
| Mountain Publication     | tain Publication     |
| Summer Night Publication | er Night Publication |
+--------------------------+----------------------+
3 rows in set (0.03 sec)
```

## Constraints
* Currently, MatrixOne doesn't support select function() without from tables.
* Currently, Substring() doesn't support FROM and FOR clauses.
