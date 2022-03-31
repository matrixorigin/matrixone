# **General FAQs**

## **Product FAQs**

* <font size=4>**What is MatrixOne?**</font>
    
  MatrixOne is a future-oriented hyperconverged cloud & edge native DBMS that supports transactional, analytical, and streaming workload with a simplified and distributed database engine, across multiple datacenters, clouds, edges and other heterogenous infrastructures. The all-in-one architecture of MatrixOne will significantly simplify database management and maintenance, single database can serve multiple data applications. 
  For information about MatrixOne, you can see [MatrixOne Introduction](../Overview/matrixone-introduction.md).

<br>

* <font size=4>**Where can I apply MatrxOne?**</font>
    
  MatrixOne provides users with HTAP services to support hybrid workloads, it can be used to build data warehouse or data platform. 
<br>

* <font size=4>**Is MatrixOne based on MySQL or some other database?**</font>
    
  MatrixOne is a totally redesigned database. It's compatible with part of MySQL syntax and semantics, and we are working to support more database semantics such as PostgreSQL, Hive, Clickhouse, as we intend to develop MatrixOne as a hyperconverged database.  
  About the compatibility with MySQL, you can see [MySQL-Compatibility](../Overview/mysql-compatibility.md).
<br>

* <font size=4>**What's the relationship between MatrixOne and MatrixCube?**</font>
    
  MatrixOne is the main database project, including query parser layer, compute layer, and storage layer, it could work as a standalone database system.
  MatrixCube is a independant library who doesn't work alone, it's a distributed system framework. It gives MatrixOne the ability to extend as a distributed database. It also supports to be mounted with other storage engines. 
  For information about MatrixCube, you can see [MatrixCube Introduction](../Overview/matrixcube/matrixcube-introduction.md).
<br>

* <font size=4>**Why MatrixOne is so fast?**</font>
    
  MatrixOne achieves accelerated queries using patented vectorized execution as well as optimal computation push down strategies through factorization techniques.  
<br>

* <font size=4>**Which programming language is MatrixOne developed with ?**</font>
    
  Currently, the primary programming language used for our codes is **Golang**.
<br>

* <font size=4>**What operating system does MatrixOne support?**</font>
    
  MatrixOne supports Linux and MacOS.
<br>

* <font size=4>**Which MatrixOne data types are supported?**</font>
    
  You can see [data tpyes in MatrixOne](../Reference/SQL-Reference/data-types.md) to learn more about the data types we support.
<br>

* <font size=4>**Can I contribute to MatrixOne?**</font>
    
  Yes, MatrixOne is an open-source project developed on GitHub. Contribution instructions are published in [Contribution Guide](../Contribution-Guide/make-your-first-contribution.md). We welcome developers to contribute to the MatrixOne community.
  
<br>

## **Deployment FAQs**
* <font size=4>Are there any hardware requirements for deploying MatrixOne?</font>

   Standalone setting specification: x86 CPU with 4 cores and 32GB memory, with CentOS 7+ Operating System.
   
   Distributed setting specification: 3 servers, each one with x86 CPU with 16 cores and 64GB memory, with CentOS 7+ Operating System.
<br>

## **SQL FAQs**

* <font size=4>**Whether functions and other keywords are case sensitive？**</font>
 
   No, they are not case sensitive.
<br>


* <font size=4>**How do I export data from MatrixOne to a file?**</font>
  
  You can use `SELECT INTO OUTFILE` command to export data from MatrixOne to a **csv** file (only to the server host, not to the remote client).  
  For this command, you can see [SELECT Reference](../Reference/SQL-Reference/Data-Manipulation-Statements/select.md).  
