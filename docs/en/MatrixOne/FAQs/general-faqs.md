# **General FAQs**

## **Product FAQs**

* <font size=4>**What is MatrixOne?**</font>
    
  MatrixOne is a future-oriented hyperconverged cloud & edge native DBMS that supports transactional, analytical, and streaming workload with a simplified and distributed database engine, across multiple datacenters, clouds, edges and other heterogenous infrastructures.
  For information about MatrixOne, you can see [MatrixOne Introduction](../Overview/matrixone-introduction.md).

<br>

* <font size=4>**What could I do with MatrxOne**</font>
    
  MatrixOne provides users with HTAP services to support hybrid workloads: transactional, analytical, streaming, time-series, machine learning, etc. The all-in-one architecture of MO will significantly simplify database management and maintenance, single database can serve multiple data applications.  
<br>

* <font size=4>**Is MatrixOne based on MySQL?**</font>
    
  Despite compatible with part of MySQL syntax and semantics, MatrixOne is developed independently and in the future there will be more different semantics from MySQL because of we intend to develop MO as a hyperconverged database.  
  About the compatibility with MySQL, you can see [MySQL-Compatibility](../Overview/mysql-compatibility.md).
<br>

* <font size=4>**What's the relationship between MatrixOne and MatrixCube?**</font>
    
  MatrixOne is the main schema of our database, including query parser layer, compute layer, and storage layer, and thus it could achieve the functionality of standalone databases. Further, with MatrixCube, we can demploy a disturbted cluster by several servers. In a nutshell, MatrixCube is the module providing distributed system.  
  For information about MatrixCube, you can see [MatrixCube Introduction](../Overview/matrixcube/matrixcube-introduction.md).
<br>

* <font size=4>**Why MO is so fast?**</font>
    
  MatrixOne achieves accelerated queries using patented vectorized execution as well as optimal computation push down strategies through factorization techniques.  
  Additionally, in a distributed system, the query capability will increase by the number of nodes increases.
<br>

* <font size=4>**Which language can I use to work with MO?**</font>
    
  Currntly, the primary programming language used for our codes is **Golang**.
<br>

* <font size=4>**What platforms does MO support?**</font>
    
  MatrixOne supports Linux and MacOS currently and in the future it will surpport more systems.
<br>

* <font size=4>**Which engines does MO support?**</font>
    
  Currently, there are two storage engines in MatrixOne including **AOE** engine and **Key-Value** engine. The former one is a column-storage engine represents "Append Only Engine", surpport OLAP capability:, and the later one can store `Catalog`(a component that holds the overall metadata of the database).  
  In the future we will promote **AOE** to upgrade to an HTAP engine providing complete ACID and strong OLAP capability.
<br>

* <font size=4>**Which MO variables are compatible with MySQL?**</font>
    
  You can see [data tpyes in MatrixOne](../Reference/SQL-Reference/data-types.md) to learn more about the data types we surpport.
<br>

## **Deployment FAQs**
* <font size=4>Are there any hardware requirements for the server using MO?</font>

<br>

## **SQL FAQs**

* <font size=4>**Whether functions and other keywords are case sensitive？**</font>

<br>

* <font size=4>**Does MatrixOne support** `JOIN?`</font>
    
  Only `INNER JOIN` with `GROUP BY` and `ORDER BY` is supported in multi table scenarios. And, table alias is not supported in `INNER JOIN`.
<br>

* <font size=4>**What is the maximum number of statements in a transaction?**</font>
<br>

* <font size=4>**How do I export data from MO to a file?**</font>
  
  You can use `SELECT INTO OUTFILE` command to export data from MO to a **csv** file (only to the server host, not to the remote client).  
  For this command, you can see [SELECT Reference](../Reference/SQL-Reference/Data-Manipulation-Statements/select.md).  
