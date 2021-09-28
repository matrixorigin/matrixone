[![LICENSE](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![CodeFactor](https://www.codefactor.io/repository/github/matrixorigin/matrixone/badge?s=7280f4312fca2f2e6938fb8de5b726c5252541f0)](https://www.codefactor.io/repository/github/matrixorigin/matrixone)

## What is MatrixOne?
MatrixOne is a planet scale, cloud-edge native big data engine crafted for heterogeneous workloads. With minimal operation and management, MatrixOne can provide end-to-end data processing automation to help users store, manipulate and analyze data cross devices, zones, regions and clouds.

## Features

### Planet Scalability
MatrixOne cluster can easily expand capacity in SQL processing, computation, and storage, by adding nodes and without the need to disable any function.

### Cloud-Edge Native
Not limited to public clouds, hybrid clouds, on-premise servers, or smart devices, MatrixOne accommodates itself to myriads of infrastructure while still providing top services with low latency and high throughput.

### Hybrid Streaming, Transaction and Analytical Processing Engine
By converging multiple engines, MatrixOne can support hybrid streaming, analytical and transactional workloads; with its pluggable architecture, MatrixOne allows for easy integration with third-party engines.

### High Availability
MatrixOne uses RAFT-based consensus algorithm to provide fault tolerance in one zone, and plans to use more advanced state-machine replication protocol to achieve geo-distributed active-active.

### Ease of use
- MatrixOne does not require dependency to download, install, or start up.
- Re-balancing, failover and system tuning are automatic.
- MatrixOne supports MySQL-compatible syntax.

### End to End Data Science automation
By streaming SQL and user defined function, MatrixOne provides end-end data processing pipeline.

## Architecture
![Architecture](https://github.com/matrixorigin/artwork/blob/main/diagram/overall-architecture.png)

### Query Parser Layer
-   **Parser** parse SQL, Streaming Query or Python language into an abstract syntax tree for further processing.
-   **Planner**: Finds the best execution plan through rule-based, cost-based optimization **algorithms**, and transfers abstract syntax tree to plan tree.
-   **IR Generator**: Converts Python code into intermediate representation.
### Computation Layer
-   **JIT compilation**: Turns SQL plan tree or IR code into a native program using LLVM, during run time.
-   **Vectorized Execution**: MatrixOne leverages SIMD instructions to construct vectorized execution pipelines.
-   **Cache**: Multiple versions of data, indexes and metadata are cached for queries.
### Cluster Management Layer
MatrixCube is a fundamental library for building distributed systems without the need to consider reliability, consistency or scalability. It is designed to facilitate distributed, stateful application building because developers only need to care about the business logic on a single node. MatrixCube is currently built upon multi-raft to provide replicated state machine and will migrate to Paxos families to increase friendliness to scenarios spanning multiple data centers.
-   **Prophet**: Used by MatrixCube to manage and schedule the MatrixOne cluster.
-   **Transaction Manager**: MatrixOne supports distributed transaction of snapshot isolation level.
-   **Replicated State Machine**: MatrixOne uses RAFT-based consensus algorithms and hyper logic clocks to implement strong consistency of the clusters. Introduction of more advanced state-machine replication protocols is yet to come.
### Replicated Storage Layer
-   **Row Storage**: stores serving workload, metadata and catalog.
-   **Column Storage**: stores analytical workload, materialized views.
### Storage Provision Layer
MatrixOne stores data in shared storage of S3 / HDFS, or even in the local disk, public clouds, on-premise server, hybrid cloud, or smart devices.
## Quick Start
### Building

**Get the MatrixOne code:**

```
$ git clone https://github.com/matrixorigin/matrixone.git
$ cd matrixone
```

**Run make:**

Run `make debug`, `make clean`, or anything else our Makefile offers. You can just run the following command to build quickly.

```
$ make config
$ make build
```

### Starting

**Prerequisites**

- MySQL client

  MatrixOne supports the MySQL wire protocol, so you can use MySQL client drivers to connect from various languages.
  
  Currently, MatrixOne only keeps compatible with the Oracle Mysql client that is a little different from the MariaDB mysql client.

**Boot MatrixOne server:**

```
$ ./mo-server system_vars_config.toml
```

**Connect MatrixOne server:**

```
$ mysql -h IP -P PORT -uUsername -p
```

**For example:**

Test account:

- user: dump
- password: 111

```
$ mysql -h 127.0.0.1 -P 6001 -udump -p
```

Now, MatrixOne only supports the TCP listen. 

## Contributing
See [Contributing Guide](CONTRIBUTING.md) for details on contribution workflows.

## Roadmap
Check out [Roadmap](https://github.com/matrixorigin/matrixone/issues/613) for MatrixOne development plan.

## Community
You can join [MatrixOne community](https://join.slack.com/t/matrixoneworkspace/shared_invite/zt-voce6d82-C8vdyHNcv11l430D0QKZlw) on Slack to discuss and ask questions.

## License
MatrixOne is licensed under the [Apache License, Version 2.0](LICENSE).
