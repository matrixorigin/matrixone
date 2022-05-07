# **通用问题解答**

## **产品常见问题**

* <font size=4>**MatrixOne是什么?**</font>
  
  MatrixOne是一款面向未来的超融合异构云原生数据库，通过超融合数据引擎支持事务/分析/流处理等混合工作负载，通过异构云原生架构支持跨机房协同/多地协同/云边协同。MatrixOne希望简化数据系统开发和运维的成本，消减复杂系统间的数据碎片，打破数据融合的各种边界。  
  想了解更多关于MatrixOne的信息，您可以浏览[MatrixOne简介](../Overview/matrixone-introduction.md)。
<br>

* <font size=4>**MatrxOne支持哪些应用？**</font>
  
  MatrixOne为用户提供了极致的HTAP服务，MatrixOne可以被应用在企业数据中台，大数据分析等场景中。
<br>

* <font size=4>**MatrixOne是基于MySQL或者其他数据库开发的吗?**</font>
  
  MatrixOne是一个从零打造的全新数据库。MatrixOne兼容MySQL的部分语法与语义，并且在未来将会产生更多与MySQL不同的语义，以便我们将之打造为一款更强大的超融合数据库。
  关于与MySQL的兼容性，您可参见[MySQL兼容性](../Overview/mysql-compatibility.md)。
<br>

* <font size=4>**MatrixOne与MatrixCube有什么关系?**</font>
  
  MatrixOne是我们数据库的主体部分，其包括了查询解析层、计算层、存储层，因此它可以实现数据库的基本功能。MatrixCube是我们的分布式框架，它目前是一个独立的仓库，他可以与MatrixOne结合起来形成一个分布式数据库，也可以与其他的存储引擎适配使其具有分布式扩展能力。  
  若想了解更多关于MatrixCube的信息，您可参见[MatrixCube简介](../Overview/matrixcube/matrixcube-introduction.md)。
<br>

* <font size=4>**为什么MatrixOne的SQL引擎如此高效?**</font>
  
  MatrixOne的SQL引擎主要利用了向量化执行与因子化计算技术，来实现更高效的查询。  
<br>

* <font size=4>**MatrixOne使用什么编程语言开发的?**</font>
  
  目前，我们主要使用**Golang**作为最主要的编程语言。
<br>

* <font size=4>**MatrixOne可以在什么操作系统上部署?**</font>
  
  目前，MatrixOne支持Linux与MacOS系统。
<br>

* <font size=4>**可以参与贡献MatrixOne项目吗?**</font>
  
  MatrixOne是一个完全在Github上进行的开源项目，欢迎所有开发者的贡献。可以参考我们的[贡献指南](../Contribution-Guide/make-your-first-contribution.md)
<br>


## **部署常见问题**

* <font size=4>**MatrixOne对部署硬件的配置要求如何？**</font>

  单机推荐配置：4核 X86架构的CPU，32GB内存，CentOS 7.0及以上操作系统
  
  分布式推荐配置: 3台 * 16核 X86架构的CPU，63GB内存， CentOS 7.0及以上操作系统
<br>

## **SQL常见问题**

* <font size=4>**MatrixOne中的函数和关键字是否区分大小写？**</font>

  不区分大小写。
<br>

* <font size=4>**如何将数据从MatrixOne导出到文件?**</font>
  
  你可以使用`SELECT INTO OUTFILE`命令来将数据导出为**csv**文件（只能导出到服务器主机，无法到远程客户端）。  
  对于该命令，您可查阅[SELECT参考指南](../Reference/SQL-Reference/Data-Manipulation-Statements/select.md)来了解具体用法。  
