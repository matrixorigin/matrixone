# **MatrixOne Tech Design**

In [MatrixOne Introduction](../matrixone-introduction.md) we have introduced the key features and core concepts of MatrixOne and in [MatrixOne Architecture](../matrixone-architecture.md) we show you the overall architecture of MatrixOne. 
This chapter will unfold the architecture and explain about the tech design. 

## **MatrixOne Architectural Features  **
The current design of MatrixOne is a combination of NewSQL and MPP, and we call it an HTAP DBMS with OLAP enhancement. **Simplicity** is the most important design principle for both usage, deployment and maintenance. Despite being a distributed database, MatrixOne only provides a single binary for deployment, with each node running exactly the same single process.  

### **NewSQL**
As we all know, relational database has existed for more than 30 years with `relational model`, `SQL` and `transaction processing`, until recently NewSQL emerged.   

NewSQL refers to a distributed architecture, starting with Google Spanner, using **Replicate State Machine** as the core, to solve the scalability and high availability problems of traditional standalone relational databases.
Replicate State Machine is the main method to implement fault-tolerant services. The state machine starts at a given initial state, and each input received is used by a state transition mechanism to generate a new state and corresponding output. In replicate state machines, the state machines of a set of servers compute copies of the same state and can continue to run even if a portion of the servers go down. In the context of replicate state machines, consistency protocols are proposed to ensure the consistency of replicate logs. Common consistency protocols include Paxos and Raft.  

In the architecturet of replicate state machine, OLTP capability is realized by mounting the Key Value storage engine, which is the main design of NewSQL.  

The biggest difference between MatrixOne's design and other NewSQL databases is that the former one can mount various storage engines. In the 0.2 version, the Key-Value storage engine is mounted to store `Catalog` and other metadata and a column storage engine is mounted to provide OLAP capability. In fact, any storage engine can be mounted including but not limited to row storage, graph, time series, dedicated NoSQL and other multimodal engines. Different storage engines can be adapted for different scenarios. We welcome community developers to contribute ideas and code to this part. 

![Replicate state machine implementation for Raft](https://github.com/matrixorigin/artwork/blob/main/docs/overview/consensus-modules.png?raw=true)

### **MPP**
MPP(Massively Parallel Processing) is a computing architecture that is used to analyze large-scale data. In simple terms, MPP is to distribute tasks to multiple servers and nodes in parallel. After the computation is completed on each node, the results of each part are aggregated together to obtain the final result. This architecture was adopted by first-generation OLAP databases such as Teradata and Greenplum. MapReduce, the key component for Hadoop, also borrowed from the MPP architecture. However, between Hadoop and MPP OLAP databases, differences are obvious in the amount of data processed, SQL support, data processing types and efficiency. Hadoop is more like a data lake, which can store and process hundreds of PB data, define schema when reading, and store a large amount of unstructured and semi-structured data. However, SQL support, query performance and real-time streaming processing are not ideal. A database solution based on the MPP architecture is more like a relational database with substantially enhanced query capabilities, still with good SQL support and ACID transaction properties.  
The latest generation of open-source MPP compute engines includes: Clickhouse, Presto, Impala, SparkSQL, Apache Doris, etc.  

MatrixOne also provides great OLAP performance based on the MPP architecture. But differently from other projects, MPP compute engine in MatrixOne is based on Golang. Even compared to C++ computing engines, it can compete in performance. After acceleration through vectorization and factorization, it performs even better in scenarios such as non-primary key join, multi-table complex join and other aspects. 

![MPP architecture](https://github.com/matrixorigin/artwork/blob/main/docs/overview/mpp_architecture.png?raw=true)

## **MatrixOne's Designed Modules and Current Progress**
MatrixOne is divided into **SQL Frontend**, **Computing Layer**, **Metadata Layer**, **Distributed Framework** and **Storage Layer**.  

![MatrixOne Modules](https://github.com/matrixorigin/artwork/blob/main/docs/overview/matrixone-modules.png?raw=true)

#### *SQL Frontend*
The entry of the MatrixOne. Currently MatrixOne provides MySQL compatibility protocol and support part of MySQL dialact. SQL Frontend receives requests from MySQL clients and pass to next layer after parsing them.  
Learn more about MatrixOne [MySQL Compatibility](../mysql-compatibility.md)

#### *SQL Parser in Computing Layer*
After receiving a request from the SQL Frontend, SQL Parser parses it and transforms it into an abstract syntax tree(AST). MatrixOne doesn't use the popular open-source query parser, such as TiDB, Vitess parser, etc. In fact, TiDB parser was used in MatrixOne version 0.1, but we developed a new parser for version 0.2, mainly because:
* MatrixOne intends to create a hyperconverged database where lots of custom syntax not necessarily consistent to MySQL. 
* Currently, MatrixOne enhances more OLAP capabilities, whereas the current open-source parser is basically designed for OLTP scenarios, and has high performance overhead for some OLAP scenarios, such as mass insertion.  
* MatrixOne also plans to offer multi-dialect protocol compatibility in the future, including PostgreSQL, Hive, ClickHouse and so on. Therefore, MatrixOne needs to own its own Query Parser, which can be customized arbitrarily, and provides the basis for multi-dialect syntactic compatibility.

#### *MPP SQL Execution in Computing Layer*
This part is the MPP compute engine established by Golang. MatrixOne compute engine accelerates SQL excutions by vectorization acceleration. The vectorization is achieved by leveraging hardware features. At present, only the adaptation and acceleration of AVX2 and AVX512 instruction sets in Intel X86 architecture are realized. More hardware platform support for accleration can be an interesting subject for community participations.
Another important design in compute engine is factorization, which breaks down the complex SQL join into an linear polynomial calculation, and significantly reduces the time consumed for complex join.  
#### *Catalog as Metadata Layer*
It's the component that holds the overall metadata of the database, such as Table/Schema definitions. Currently, Catalog is still a temporary solution using Key-Value engine, and the subsequent Catalog will be migrated to a standard OLTP engine, providing further and more complete ACID capability to support the Catalog component.  
 
#### *MatrixCube as Distributed Framework*

This component is a distributed infrastructure library that implements the NewSQL architecture and is currently a separate repository. It contains two parts of functionality, one is to provide the consensus protocol of the Replicate State Machine implementation, currently using Multi Raft mechanism and the other is to provide a replica scheduling mechanism based on Raft, which is called Prophet in the code. MatrixCube is a universal library that can interface with various storage engines, which is why we set it as a separate repository currently. Any third party developer can easily use it to implement distributed storage engines and databases with strong consistency. Another important functionality of MatrixCube is to provide distributed transaction capabilities, which is currently being designed and will soon be available for discussion by developers.  
  
![MatrixCube architecture](https://github.com/matrixorigin/artwork/blob/main/docs/overview/matrixcube-architecture.svg?raw=true)

Please refer to [MatrixCube introduction](../matrixcube/matrixcube-introduction.md) for more details. 

#### *Storage Engine*

In MatrixOne, storage engine is selectable, and thus you can specify an engine to determine which one will be used to store data when creating tables with DDL syntax. 
In the current version, only one engine is implemented: AOE Engine(which stood for "Analytics Optimized Engine", evolving from "Append Only Engine", which is a column storage engine with "Append Only" mode.
An evolution of AOE is called **TAE(Transactional Analytical Engine)**, a column-based HTAP Engine that provides complete ACID capability and powerful OLAP capability, and it's currently under development. When completed, MatrixOne will have complete distribution HTAP ability.  

Please refer to [AOE Technical Design](https://github.com/matrixorigin/matrixone/blob/main/docs/rfcs/20211210_aoe_overall_design.md) for more details. 

Another **TPE(Transaction Processing Engine)** is under development. TPE is designed to support `catalog` usage, for now, it will not provide transactional service for the external users.  
In the future, it's possible that TPE provides complete SQL capabilities externally. 



## **Learn More**

If you are interested in a particular module and would like to contribute, please refer to [contribution guide](../../Contribution-Guide/How-to-Contribute/preparation.md) to check out MatrixOne codebase structure. 
