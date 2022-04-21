# **Playground**
[MatrixOne Playground](https://playground.matrixorigin.io/?tutorial=SSB-test-with-matrixone&step=1)提供了一个交互式工作台，可以让您直接通过浏览器来运行MatrixOne的SQL语句等功能，快速体验MatrixOne的最新能力。
 

* 你可以参考[SQL参考指南](../Reference/SQL-Reference/Data-Definition-Statements/create-database.md)来熟悉相关SQL语句
* 你可以查看[实践教程](Tutorial/SSB-test-with-matrixone.md)来学习如何使用MatrixOne来进行SSB、NYC测试等最佳实践。

## **限制**
MatixOrigin Playground只能在**只读**模式下进行操作，因此相关DDL命令和部分改变数据的DML命令不可用，具体限制如下：

* **DDL** 命令不可用:  
```create/drop table``` , ```truncate``` , ```update``` , ```set``` ,```use```  
  
* 以下展示的**DML** 命令不可用：  
```insert``` , ```replace``` , ```delete```,```select into ```  

* ```commit``` 不可用

* ```call``` 不可用

* 结果最大展示行**max_result_rows**=2000  
  
## **示例**

在Playground中，你可以使用工作台左侧提供的互动实践教程来快速启动对MatrixOne的体验操作。现在平台支持的互动实践教程如下:

* [**用MatrixOne完成SSB测试**](https://playground.matrixorigin.io/?tutorial=SSB-test-with-matrixone&step=1) 
     
此处，我们以**SSB测试实践** 为例来展示Playground的大致流程。

### **数据准备**  

为了更好地体验MatrixOne特性和性能，本教程中的测试查询将在不使用过滤语句的情况下运行。  
在开始之前测试数据集就已经预先加载到数据库`ssb`中。可以列出数据库中你可以进行查询的表：

```
SHOW TABLES；
```

!!! note  <font size=4>Tips</font>
    <font size=3>点击命令语句可以直接复制该语句，然后点击右侧的**Run**按键来运行 </font>  
    <font size=3>查询结构显示于右下侧</font>  
    <font size=3>点击**Continue**按键来进行下一步</font>  



### **运行查询语句**

现在，您可以使用我们提供的SQL命令来查询数据。 
例如：
* **运行 Q1.1**

```
select sum(lo_revenue) as revenue
from lineorder join dates on lo_orderdate = d_datekey；
```
此外，你可以在查询中使用`join`连接：

* **运行 Q1.2**

```
select sum(lo_revenue) as revenue
from lineorder
join dates on lo_orderdate = d_datekey；
```

也可以使用```group by``` 与```order by``` 命令：
* **运行 Q2.1**

```
select sum(lo_revenue) as lo_revenue, d_year, p_brand
from lineorder
join dates on lo_orderdate = d_datekey
join part on lo_partkey = p_partkey
join supplier on lo_suppkey = s_suppkey
group by d_year, p_brand
order by d_year, p_brand;
```

**Playground**提供了更多查询命令，您可以自行测试。


## **相关信息**
本页面介绍了Playground的特性、限制和示例。有关使用MatrixOne时所需的其他更多信息，可参阅以下内容：

* [安装单机版MatrixOne](install-standalone-matrixone.md)
* [部署分布式MatrixOne集群](install-distributed-matrixone.md)
* [产品最新发布](../Overview/whats-new.md)
