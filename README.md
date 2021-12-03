[![LICENSE](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![CodeFactor](https://www.codefactor.io/repository/github/matrixorigin/matrixone/badge?s=7280f4312fca2f2e6938fb8de5b726c5252541f0)](https://www.codefactor.io/repository/github/matrixorigin/matrixone)

## What is MatrixOne?

MatrixOne is a future-oriented hyperconverged cloud and edge native DBMS that supports transactional, analytical, and streaming workload with a simplified and distributed database engine, across multiple data centers, clouds, edges and other heterogenous infrastructures.

## **Key Features** 
### **Hyperconverged Engine**
* **Monolitic Engine**
  
     A monolithic database engine is designed to support hybrid workloads: transactional, analytical, streaming, time-series, machine learning, etc.

* **Built-in Streaming Engine**
  
     With the built-in streaming engine, MatrixOne supports in-database streaming processing by groundbreaking incremental materialized view maintenance.

### **Cloud & Edge Native**
* **Real Infrastructure Agnostic**
  
     MatrixOne supports seemless workload migration and bursting among different locations and infrastructures.

* **Multi-site Active/Active**
  
     MatrixOne provides industry-leading latency control with optimized consistency protocol.

### **Extreme Performance**
* **High Performance**
  
     Accelerated queries supported by patented vectorized execution as well as optimal computation push down strategies through factorization techniques.

* **Strong Consistency**
  
     MatrixOne introduces a global, high-performance distributed transaction protocol across storage engines.

* **High Scalability**
  
     Seamless and non-disruptive scaling by disaggregated storage and compute.   

## **User Values**
* **Simplify Database Management and Maintenance**
  
     To solve the problem of high and unpredictable cost of database selection process, management and maintenance due to database overabundance, MatrixOne all-in-one architecture will significantly simplify database management and maintenance, single database can serve multiple data applications.

* **Reduce Data Fragmentation and Inconsistency**
  
     Data flow and copy between different databases makes data sync and consistency increasingly difficult. The unified incrementally materialized view of MatrixOne makes the downstream can support real-time upstream update, achieve the end-to-end data processing without redundant ETL process.

* **Decoupling Data Architecture From Infrastructure**
  
     Currently the architecture design across different infrastructures is complicated, causes new data silos between cloud and edge, cloud and on-premise. MatrixOne is designed with unified architecture to support simplified data management and operations across different type of infrastructures.

* **Extremely Fast Complex Query Performance**
  
     Poor business agility as a result of slow complex queries and redundant intermediate tables in current data warehousing solutions. MatrixOne  supports blazing fast experience even for star and snowflake schema queries, improving business agility by real-time analytics.
     
* **An Solid OLTP-like OLAP Experience**
  
     Current data warehousing solutions have the following problems such as high latency and absence of immediate visibility for data updates. MatrixOne brings OLTP (Online Transactional Processing) level consistency and high availability to CRUD operations in OLAP (Online Analytical Processing).

* **Seamless and Non-disruptive Scaling**
  
     It is difficult to balance performance and scalability to achieve optimum price-performance ratio in current data warehousing solutions. MatrixOne's disaggregated storage and compute architecture makes it fully automated and efficient scale in/out and up/down without disrupting applications.

## Architecture
![Architecture](https://github.com/matrixorigin/artwork/blob/main/docs/overview/overall-architecture.png?raw=true)

### Query Parser Layer
-   **Parser**: Parses SQL, Streaming Query, or Python language into an abstract syntax tree for further processing.
-   **Planner**: Finds the best execution plan through rule-based, cost-based optimization algorithms, and transfers abstract syntax tree to plan tree.
-   **IR Generator**: Converts Python code into an intermediate representation.
### Computation Layer
-   **JIT Compilation**: Turns SQL plan tree or IR code into a native program using LLVM at runtime.
-   **Vectorized Execution**: MatrixOne leverages SIMD instructions to construct vectorized execution pipelines.
-   **Cache**: Multi-version cache of data, indexes, and metadata for queries.
### Cluster Management Layer
MatrixCube is a fundamental library for building distributed systems, which offers guarantees about reliability, consistency, and scalability. It is designed to facilitate distributed, stateful application building to allow developers only need to focus on the business logic on a single node. MatrixCube is currently built upon multi-raft to provide replicated state machine and will migrate to Paxos families to increase friendliness to scenarios spanning multiple data centers.
-   **Prophet**: Used by MatrixCube to manage and schedule the MatrixOne cluster.
-   **Transaction Manager**: MatrixOne supports distributed transaction of snapshot isolation level.
-   **Replicated State Machine**: MatrixOne uses RAFT-based consensus algorithms and hybrid logic clocks to implement strong consistency of the clusters. Introduction of more advanced state-machine replication protocols is yet to come.
### Replicated Storage Layer
-   **Row Storage**: Stores serving workload, metadata, and catalog.
-   **Column Storage**: Stores analytical workload and materialized views.
### Storage Provision Layer
MatrixOne stores data in shared storage of S3 / HDFS, or the local disk, on-premise server, hybrid and any cloud, or even smart devices.
## Quick Start
Get started with MatrixOne quickly by the following steps.
### Installation 

MatrixOne supports Linux and MacOS. You can install MatrixOne either by [building from source](#building-from-source) or [using docker](#using-docker).
#### Building from source

1. Install Go (version 1.17 is required).
  
2. Get the MatrixOne code:

```
$ git clone https://github.com/matrixorigin/matrixone.git
$ cd matrixone
```

3. Run make:

   You can run `make debug`, `make clean`, or anything else our Makefile offers.

```
$ make config
$ make build
```

4. Boot MatrixOne server:

```
$ ./mo-server system_vars_config.toml
```
#### Using docker

1. Install Docker, then verify that Docker daemon is running in the background:

```
$ docker --version
```
2. Create and run the container for the latest release of MatrixOne. It will pull the image from Docker Hub if not exists.
   
```
$ docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:latest
```
### Connecting to MatrixOne server

1. Install MySQL client.
   
   MatrixOne supports the MySQL wire protocol, so you can use MySQL client drivers to connect from various languages. Currently, MatrixOne is only compatible with Oracle MySQL client. This means that some features might not work with MariaDB client.

2. Connect to MatrixOne server:

```
$ mysql -h IP -P PORT -uUsername -p
```
   The connection string is the same format as MySQL accepts. You need to provide a user name and a password. 

   Use the built-in test account for example:

   - user: dump
   - password: 111

```
$ mysql -h 127.0.0.1 -P 6001 -udump -p
Enter password:
```

Now, MatrixOne only supports the TCP listener. 
## Contributing
See [Contributing Guide](CONTRIBUTING.md) for details on contribution workflows.

## Roadmap
Check out [Roadmap](https://github.com/matrixorigin/matrixone/issues/613) for MatrixOne development plan.

## Community
You can join [MatrixOne community](https://join.slack.com/t/matrixoneworkspace/shared_invite/zt-voce6d82-C8vdyHNcv11l430D0QKZlw) on Slack to discuss and ask questions.

## License
MatrixOne is licensed under the [Apache License, Version 2.0](LICENSE).
