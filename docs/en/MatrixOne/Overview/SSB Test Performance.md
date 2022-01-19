# **SSB Test Performance**

## **Overview**
We present the **SSB Test** results for MatrixOne with different hardwares and query modes.

For instructions to reproduce the SSB test results listed here, see [SSB Test with MatrixOne](../Get-Started/Tutorial/SSB-test-with-matrixone.md), and if you fail to achieve similar results, there is likely a problem in either the hardware, workload, or test design.  

In v0.2.0, both single table and multiple tables can work in MatrixOne, and we compared results of MatrixOne with other similar database products. Through the clear comparison of the bar chart, you will see that MatrixOne has an obvious advantage in the query processing time.


!!! note  "<font size=4>note</font>"
    <font size=3>The following test results are the average of the three tests in seconds.</font>  

 
In order to show the single-table query performance, we combine five tables into a flat table called `lineorder_flat`.  


## **Standalone node&Single table**
We use standalone service to execute flat table(`lineorder_flat`) queries with MatrixOne, and compared the results with ClickHouse(可能需要遮掩名字) in the same configuration.  
The table `lineorder_flat` has 600 million rows data and takes up 220 GB of space.  

!!! info 
    The following server was used:
    AMD EPYC™ Rome CPU 2.6GHz/3.3GHz, 16 physical cores total, 32 GiB Memory.



|  Query ID  | MatrixOne v0.2.0  |  ClickHouse v21.11.4.14 
|  :----:  | :----:  |  :----:  
| Q2  | 2.71 |3.82 	
| Q3.1 | 4.23|5.01 
| Q3.2  | 11.05|21.34
| Q4.1  | 2.94|3.1
| Q4.2  | 4.27|5.32
| Q4.3  | 16.91|26.32
| SUM  | 42.11|64.91

![柱状图](https://github.com/matrixorigin/artwork/blob/main/docs/overview/SSB_standalone_single.png?raw=true)

## **Standalone node&Multiple tables**
With a standalone server again, now we execute multiple tables queries on the original tables of SSB Test.  
There are 600 million rows of data in all, and the main table takes up 67 GB of space.  

|  Query ID  | MatrixOne v0.2.0   |  ClickHouse v21.11.4.14| Starrocks v1.19.3
|  :----:  | :----:  |  :----:  |:----:
| Q2  | 13.6|28.05 |15.83	
| Q3.1 | 12.94|27.81 |16.98
| Q3.2  | 23.56|54.84 |29.25
| Q4.1  | 13.96|27.2 |16.77
| Q4.2  | 19.72|41.82|21.54
| Q4.3  | 46.07|85.99|35.95
| SUM  | 129.85|265.71|136.37

![柱状图](https://github.com/matrixorigin/artwork/blob/main/docs/overview/SSB_standalone_multi.png?raw=true)
## **Cluster&Multiple tables**

There are three nodes in the cluster, each of which is installed as standalone version, and process time is shorter than that of standclone node, demonstrating the overall performance will improve as the number of nodes increases. 

|  Query ID  | MatrixOne v0.2.0  |  Starrocks v1.19.3
|  :----:  | :----:  |  :----:  
| Q2 | 4.94 |6.08 	
| Q3.1 | 5.85|6.27 
| Q3.2  | 9.67|9.79
| Q4.1  | 6.05|6.87
| Q4.2  | 6.87|9.51
| Q4.3  | 20.1|15.55
| SUM  | 53.48|54.07

![柱状图](https://github.com/matrixorigin/artwork/blob/main/docs/overview/SSB_cluster_multi.png?raw=true)

## **Limitations**
We didn't finished all the quiries of SSB Test in MatrixOne v0.2.0 because:  

* The filter command `WHERE` is not surpported
* The table cannot be partitioned currently, so commands as `PARTITION` series are not surpported

## **Learn More**
This page shows the results of SSB Test with MatrixOne. For information on other benchmarks that are available when trying out MatrixOne, see the following:

* [NYC Test with MatrixOne](../Get-Started/Tutorial/NYC-test-with-matrixone.md)  
* [NYC Test Performance](NYC Test Performance.md)