# **INNER JOIN**

## **Description**
The INNER JOIN keyword selects records that have matching values in both tables.


## **Syntax**
```
> SELECT column_name(s)
FROM table1
INNER JOIN table2
ON table1.column_name = table2.column_name;

```

## **Parameter Values**
|  Parameter   | Description  |
|  ----  | ----  |
| value  | Required. The value to convert |
| datatype  | Required. The datatype to convert to |


The datatype can be one of the following:

|  Value   | Description  |
|  ----  | ----  |
| DATE  | Converts value to DATE. Format: "YYYY-MM-DD" |
| DATETIME  | Converts value to DATETIME. Format: "YYYY-MM-DD HH:MM:SS" |
| DECIMAL  | Converts value to DECIMAL. Use the optional M and D parameters to specify the maximum number of digits (M) and the number of digits following the decimal point (D). |
| TIME  | Converts value to TIME. Format: "HH:MM:SS" |
| CHAR  | Converts value to CHAR (a fixed length string) |
| NCHAR  | Converts value to NCHAR (like CHAR, but produces a string with the national character set) |
| SIGNED  | Converts value to SIGNED (a signed 64-bit integer) |
| UNSIGNED  | Converts value to UNSIGNED (an unsigned 64-bit integer) |
| BINARY  | Converts value to BINARY (a binary string) |


## **Examples**


Let's look at a selection from the "Orders" table:

|  OrderID   | CustomerID  | OrderDate | 
|  ----  | ----  | ---- |
| 10308  | 2 |  	1996-09-18 |
| 10309  | 37  | 1996-09-19 |
| 10310  | 77  | 1996-09-20 |


Then, look at a selection from the "Customers" table:

|  CustomerID   | CustomerName  | ContactName | Country |
|  ----  | ----  | ---- | ---- | 
| 1  | Alfreds Futterkiste	 |  	Maria Anders	 | Germany | 
| 2  | Ana Trujillo Emparedados y helados	  | Ana Trujillo	| Mexico | 
| 3  | Antonio Moreno Taquería	 | Antonio Moreno	 | Mexico |


Notice that the "CustomerID" column in the "Orders" table refers to the "CustomerID" in the "Customers" table. The relationship between the two tables above is the "CustomerID" column.

Then, we can create the following SQL statement (that contains an INNER JOIN), that selects records that have matching values in both tables:

```

> SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate
FROM Orders
INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;

```

and it will produce something like this:

|  OrderID   | CustomerName  | OrderDate | 
|  ----  | ----  | ---- |
| 10308  | Ana Trujillo Emparedados y helados	 |  	9/18/1996 |
| 10365  | Antonio Moreno Taquería	  | 	11/27/1996|
| 10383  | Around the Horn	  | 12/16/1996 |
| 10355  | Around the Horn	  | 11/15/1996 |
| 10278  | Berglunds snabbköp		  | 8/12/1996 |


