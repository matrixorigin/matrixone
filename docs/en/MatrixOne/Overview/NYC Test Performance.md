# **NYC Test Performance**

## **Overview**
With MatrixOne v0.2.0, we follow the [Tutorial](../Get-Started/Tutorial/NYC-test-with-matrixone.md) to execute **NYC Taxi** test with a standalone serve, and compare the results with that of other products, which shows the performance of MatrixOne in query processing time.  


!!! note  "<font size=4>note</font>"
    <font size=3>The following test results are the average of the three tests in seconds.</font>  


## **Standalone node**

!!! info 
    The following server was used:
    AMD EPYC™ Rome CPU 2.6GHz/3.3GHz, 16 physical cores total, 32 GiB Memory.

|  Query ID  | MatrixOne 0.2.0   |  ClickHouse v21.11.4.14 | Starrocks v1.19.3
|  :----:  | :----:  |  :----:  |:----:
| Q1 | 8.37|5.99 |2.58	
| Q2 | 2.67|4.13 |2.18
| Q3 | 3.48|4.56 |3.43
| Q4 | 5.34|7.09 |4.19
| SUM| 19.86|21.77|12.38

![柱状图](https://github.com/matrixorigin/artwork/blob/main/docs/overview/NYC_standalone.png?raw=true)

## **Limitations**
There are some limitations on quiries in MatrixOne v0.2.0:

* The filter command `WHERE` is not surpported
* The table cannot be partitioned currently, so commands as `PARTITION` series are not surpported

## **Learn More**
This page shows the results of NYC Test with MatrixOne. For information on other benchmarks that are available when trying out MatrixOne, see the following:

* [SSB Test with MatrixOne](../Get-Started/Tutorial/SSB-test-with-matrixone.md)  
* [SSB Test Performance](SSB Test Performance.md)