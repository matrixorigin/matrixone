**通用问答**

## **产品问题**

* <font size=4>**MatrixOne究竟是什么?**</font>
  
  MatrixOne是一款面向未来的超融合异构云原生数据库，通过超融合数据引擎支持事务/分析/流处理等混合工作负载，通过异构云原生架构支持跨机房协同/多地协同/云边协同。简化开发运维，消简数据碎片，打破数据的系统、位置和创新边界。  
  想了解更多关于MatrixOne的信息，您可以浏览[MatrixOne简介](../Overview/matrixone-introduction.md)。
<br>

* <font size=4>**MatrxOne可以用来做什么**</font>
  
  MatrixOne为用户提供了极致的HTAP服务，可以支持事物、分析、流处理与机器学习等混合工作负载。MO的一体化架构大大简化了数据库的运维，单个数据库便可以服务多个数据应用。
<br>

* <font size=4>**MatrixOne是基于MySQL开发的吗?**</font>
  
  尽管MatrixOne兼容MySQL的部分语法与语义，但是MatrixOne仍然独立设计，并且在未来将会产生更多与MySQL不同的语义，以便我们将之打造为一款更强大的超融合数据库。
  关于与MySQL的兼容性，您可参见[MySQL兼容性](../Overview/mysql-compatibility.md)。
<br>

* <font size=4>**MatrixOne与MatrixCube有什么关系?**</font>
  
  MatrixOne是我们数据库的主体部分，其包括了查询解析层、计算层、存储层，因此它可以实现单机数据库的基本功能。而在MatrixCube的加持下，我们可以通过多个服务器部署分布式集群，实现分布式数据库的功能。简而言之，MatrixCube是实现分布式系统的核心模块。  
  若想了解更多关于MatrixCube的信息，您可参见[MatrixCube简介](../Overview/matrixcube/matrixcube-introduction.md)。
<br>

* <font size=4>**为什么MatrixOne如此高效?**</font>
  
  MatrixOne主要利用了向量化执行与因子化计算技术，来实现更高效的查询。  
  此外，在分布式体系中，查询性能也会随着集群中节点数目增多而提高。
<br>

* <font size=4>**MatrixOne使用什么编程语言?**</font>
  
  目前，我们主要使用**Golang**作为最主要的编程语言。
<br>

* <font size=4>**MatrixOne可以在什么系统上部署?**</font>
  
  目前，MatrixOne支持Linux与MacOS系统，未来将具有更广泛的适用性。
<br>

* <font size=4>**MatrixOne提供了哪些引擎?**</font>
  
  目前，MatrixOne包含两种存储引擎，分别是**AOE**引擎以及**Key-Value**引擎。前者是一个列存引擎，意味着"Append Only Engine"，提供了OLAP能力；后者存储了`Catalog`（包含了数据库中的各种元数据）。  
  在未来，我们将进一步提升**AOE**引擎，将之升级为一个支持完整ACID规则并提供强大OLAP能力的HTAP引擎。
<br>

* <font size=4>**MatrixOne中有哪些与MySQL兼容的变量?**</font>
  
  您可以查看[MatrixOne中的数据类型](../Reference/SQL-Reference/data-types.md)来了解目前的情况。
<br>

## **Deployment FAQs**

* <font size=4>**MatrixOne对服务器的的配置要求如何？**</font>

<br>

## **SQL FAQs**

* <font size=4>**MatrixOne中的函数和关键字是否区分大小写？**</font>

<br>

* <font size=4>**MatrixOne是否支持`JOIN?`，支持程度如何？**</font>
  
  目前，在多表场景下，我们支持在有`GROUP BY`与`ORDER BY`参与的语句中使用`INNER JOIN`。并且，在`INNER JOIN`不支持表的别名。
<br>

* <font size=4>**一个事务中最大的语句数是多少?**</font>
<br>

* <font size=4>**如何将数据从MatrixOne导出到文件?**</font>
  
  你可以使用`SELECT INTO OUTFILE`命令来将数据导出为**csv**文件（只能导出到服务器主机，无法到远程客户端）。  
  对于该命令，您可查阅[SELECT参考指南](../Reference/SQL-Reference/Data-Manipulation-Statements/select.md)来了解具体用法。  
