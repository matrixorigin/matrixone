# **Playground**

[MatrixOne Playground](https://playground.matrixorigin.io/?tutorial=SSB-test-with-matrixone&step=1) allows you to try SQL statements and explore features of MatrixOne instantly from your web browser with interactive tutorials.  

* For docs about our SQL commands, you can see [SQL Reference](../Reference/SQL-Reference/Data-Definition-Statements/create-database.md).  
* For tutorials about **SSB** or **TPCH** test, you can see [Tutorial](../Tutorial/SSB-test-with-matrixone.md).

## **Limitations**

You can only operate with the SQLs that are already in **Tutorials** in MatrixOrigin Playground. 

* **max_result_rows**=2000  

## **Examples**

In Playground, You can follow different interactive tutorials and pick one in the list box, and now the supported tutorials are shown below:  

* [**SSB Test with MatrixOne**](https://playground.matrixorigin.io/?tutorial=SSB-test-with-matrixone&step=1)

We will use **SSB Test** as an example to show you the overall operation process of Playground.  

### **Test Preperations**  

This tutorial walks you through the most popular **Star Schema Benchmark（SSB）**Test SQL statements with MatrixOne. To better experience MatrixOne's features and performance, test queries in this tutorial will run without filters.  
Before you start, the test datasets have been pre-loaded in  database `ssb`. To list available tables in the database you can query :

```
SHOW TABLES;
```

!!! note  "<font size=4>Tips</font>"
    <font size=3>You can click on the command to copy and **Run** it in the terminal on the right.</font>  
    <font size=3>The query results are displayed in the lower right.</font>  
    <font size=3>Then you can run the queries on SSB datasets, click **Continue**.</font>  

### **Run Query Command**

Now, You can query the table with SQL commands we provide.  
For example:  

* **Run Q1.1 query**

```
select sum(lo_revenue) as revenue
from lineorder join dates on lo_orderdate = d_datekey;
```

And you can use ```join``` in queries:

* **Run Q1.2 query**

```
select sum(lo_revenue) as revenue
from lineorder
join dates on lo_orderdate = d_datekey;
```

Additionally, ```group by``` and ```order by``` can be used:

* **Run Q2.1 query**

```
select sum(lo_revenue) as lo_revenue, d_year, p_brand
from lineorder
join dates on lo_orderdate = d_datekey
join part on lo_partkey = p_partkey
join supplier on lo_suppkey = s_suppkey
group by d_year, p_brand
order by d_year, p_brand;
```

More query commands are provided in **Playground**, you can test on your own.

## **Learn More**

This page describes the features, limitations, and examples of Playground. For information on other options that are available when trying out MatrixOne, see the following:

* [Install MatrixOne](install-standalone-matrixone.md)
* [What‘s New](../Overview/whats-new.md)
