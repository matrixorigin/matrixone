# **SSB测试性能报告**

## **概述**
在此，我们给出了使用MatrixOne在不同的硬件和查询条件下进行SSB测试的性能结果。  
可参见[完成SSB测试](../Get-Started/Tutorial/SSB-test-with-matrixone.md)来复现整个测试结果，如果未能实现类似结果，可能需要检查硬件、负载或或测试流程是否存在问题，可以在GitHub上提出[issue](https://github.com/matrixorigin/matrixone/issues/new/choose)来报告具体问题。  
在v0.2.0中，MatrixOne在单表、多表查询方面均有优异的表现，我们将MatrixOne的性能测试结果与其他数据库产品进行了比较。通过柱状图，可以看出MatrixOne在查询处理时间上有明显的优势。

推荐测试使用的服务器规格型号：x86 CPU，16核，64GB内存，CentOS 7+操作系统。


!!! note  <font size=4>说明</font>
    <font size=3>以下测试结果均取三次最好结果的平均值，以秒为单位</font>  


为了展示单表查询能力，将五张表合成为一张宽表：`lineorder_flat`。

## **单机单表**
使用单机搭载MatrixOne执行对宽表`lineorder_flat`的查询，并与相同配置下的其他数据库进行比较。
`lineorder_flat`共有6亿行数据，共占据220 GB空间。  

!!! info 测试配置
    测试所用服务器配置如下：  
    AMD EPYC™ Rome CPU 2.6GHz/3.3GHz，16核，32GiB内存。


|  查询编号  | MatrixOne v0.2.0  |  ClickHouse v21.11.4.14 
|  :----:  | :----:  |  :----:  
| Q2  | 2.71 |3.82 	
| Q3.1 | 4.23|5.01 
| Q3.2  | 11.05|21.34
| Q4.1  | 2.94|3.1
| Q4.2  | 4.27|5.32
| Q4.3  | 16.91|26.32
| 总和  | 42.11|64.91

![柱状图](https://github.com/matrixorigin/artwork/blob/main/docs/overview/SSB_standalone_single.png?raw=true)

## **单机多表**
仍然使用单机环境，现对原始SSB测试数据集中的多个数据表进行查询，数据共有6亿行，主表占据67GB空间。

|  查询编号  | MatrixOne v0.2.0   |  ClickHouse v21.11.4.14| Starrocks v1.19.3
|  :----:  | :----:  |  :----:  |:----:
| Q2  | 13.6|28.05 |15.83	
| Q3.1 | 12.94|27.81 |16.98
| Q3.2  | 23.56|54.84 |29.25
| Q4.1  | 13.96|27.2 |16.77
| Q4.2  | 19.72|41.82|21.54
| Q4.3  | 46.07|85.99|35.95
| 总和  | 129.85|265.71|136.37

![柱状图](https://github.com/matrixorigin/artwork/blob/main/docs/overview/SSB_standalone_multi.png?raw=true)
## **集群多表**
使用由三个节点组成的集群进行多表查询，其中每一个节点都按照单机进行安装部署。由测试结果可知，集群的查询时间明显短于单机查询，证明了MatrixOne的总体性能随着集群中节点个数增加而提高。

|  查询编号  | MatrixOne v0.2.0  |  Starrocks v1.19.3
|  :----:  | :----:  |  :----:  
| Q2 | 4.94 |6.08 	
| Q3.1 | 5.85|6.27 
| Q3.2  | 9.67|9.79
| Q4.1  | 6.05|6.87
| Q4.2  | 6.87|9.51
| Q4.3  | 20.1|15.55
| 总和  | 53.48|54.07

![柱状图](https://github.com/matrixorigin/artwork/blob/main/docs/overview/SSB_cluster_multi.png?raw=true)

## **限制**

MatrixOne v0.2.0并没有完成所有SSB查询语句，原因如下：

* 不使用包含`WHERE`过滤语句的查询命令
* 暂不支持表分区，因此不使用`PARTITION`相关命令

## **相关信息**

本节内容展示了使用MatrixOne进行SSB测试的结果。若想获取更多相关信息，请见：  

* [完成NYC测试](../Get-Started/Tutorial/NYC-test-with-matrixone.md)  
* [NYC测试结果](NYC-Test-Performance.md)
