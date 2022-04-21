# **NYC性能测试结果**

## **概述**
在MatrixOne v0.2.0版本，我们在单机上遵照[相关流程](../Get-Started/Tutorial/NYC-test-with-matrixone.md)实现了**纽约出租车数据测试**，并将测试结果与其他产品相比较，体现了MatrixOne在查询处理时间上的优异性。  



!!! note  <font size=4>说明</font>
    <font size=3>以下测试结果均取三次最好结果的平均值，以秒为单位</font>  


## **单机测试**

!!! info 测试配置
    测试所用服务器配置如下：  
    AMD EPYC™ Rome CPU 2.6GHz/3.3GHz，16核，32GiB内存。

|  查询编号 | MatrixOne 0.2.0   |  ClickHouse v21.11.4.14 | Starrocks v1.19.3<br>(仅有3000万行独立数据)
|  :----:  | :----:  |  :----:  |:----:
| Q1 | 8.37|5.99 |2.58	
| Q2 | 2.67|4.13 |2.18
| Q3 | 3.48|4.56 |3.43
| Q4 | 5.34|7.09 |4.19
| 总和| 19.86|21.77|12.38

![柱状图](https://github.com/matrixorigin/artwork/blob/main/docs/overview/NYC_standalone.png?raw=true)

## **限制**
在测试中对MatrixOne v0.2.0的查询语句进行了一些限制：

* 不使用 `WHERE`过滤语句
* 暂不支持表分区，因此不使用`PARTITION`相关命令

## **相关信息**
本节内容展示了使用MatrixOne进行纽约出租车数据测试的结果。若想获取更多相关信息，请见：

* [完成SSB测试](../Get-Started/Tutorial/SSB-test-with-matrixone.md)  
* [SSB测试报告](SSB-Test-Performance.md)
