# **LIKE**

## **Description**
The LIKE operator is used in a WHERE clause to search for a specified pattern in a column.

There are two wildcards often used in conjunction with the LIKE operator:

* The percent sign (%) represents zero, one, or multiple characters
* The underscore sign (_) represents one, single character

## **Syntax**
```
> SELECT column1, column2, ...
FROM table_name
WHERE columnN LIKE pattern;
```
## **Examples**
```sql
> SELECT * FROM Customers
WHERE CustomerName LIKE 'a%'; //The following SQL statement selects all customers with a CustomerName starting with "a"

> SELECT * FROM Customers
WHERE CustomerName LIKE '%a'; //The following SQL statement selects all customers with a CustomerName ending with "a"

> SELECT * FROM Customers
WHERE CustomerName LIKE '%or%'; //The following SQL statement selects all customers with a CustomerName that have "or" in any position

> SELECT * FROM Customers
WHERE CustomerName LIKE '_r%'; //The following SQL statement selects all customers with a CustomerName that have "r" in the second position

> SELECT * FROM Customers
WHERE CustomerName LIKE 'a__%'; //The following SQL statement selects all customers with a CustomerName that starts with "a" and are at least 3 characters in length

> SELECT * FROM Customers
WHERE ContactName LIKE 'a%o'; //The following SQL statement selects all customers with a ContactName that starts with "a" and ends with "o"

> SELECT * FROM Customers
WHERE CustomerName NOT LIKE 'a%'; //The following SQL statement selects all customers with a CustomerName that does NOT start with "a"

```

## **Constraints**

NOT LIKE is not supported for now. 