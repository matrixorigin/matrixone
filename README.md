## What is MatrixOne?
MatrixOne is a planet scale, cloud-edge native big data engine crafted for heterogeneous workloads. With minimal operation and management, MatrixOne can provide end-to-end data processing automation to help users store, manipulate and analyze data cross devices, zones, regions and clouds.

## Features

### Planet Scalability
By adding nodes, MatrixOne cluster can easily expand SQL processing, computation and storage capacity without turning off any functions.

### Cloud-Edge Native
MatrixOne can leverage effectively benefits of different infrastructure, not limited to public clouds, on-premise server, hybrid cloud and smart devices etc., to provide services with low latency and high throughput.

### Hybrid Streaming, Transaction and Analytical Processing Engine
By converging multiple engine tightly, MatrixOne can support hybrid streaming, analytical and transactional workloads. And the pluggable architecture also support MatrixOne integrating third-party engine easily.

### High Availability
MatrixOne currently uses RAFT based consensus algorithm to provide fault tolerance in one zone. We plan to use cutting edge state-machine replication protocol to achieve GEO-distributed active-active.

### Ease of use
MatrixOne's principle is to download, install and startup without any dependency. Rebalancing, failover and system tuning are all automatic. MatrixOne supports MySQL compatible syntax.

### End to End Data Science automation
By streaming SQL and user defined function, MatrixOne provides end-end data processing pipeline.

## Architecture
![Architecture](https://github.com/matrixorigin/artwork/blob/main/diagram/overall-architecture.png)

### Query parser layer
-   Parser: parse SQL, Streaming Query or Python language into abstract syntax tree for further processing.
-   Planner: also be considered as optimizer to transfer abstract syntax tree to plan tree. Planner use rule based optimization and cost based optimization to find the best execution plan.
-   IR generator: For python language, MatrixOne will use IR code generator to transfer Python code to intermediate representation.
### Computation layer
-   JIT compilation: This module will use LLVM to turn SQL plan tree or IR code into a native program at run time.
-   Vectorized Execution: MatrixOne leverages SIMD instructions to construct vectorized execution pipeline.
-   Cache: multiple version data, indexes and meta data are cached for query execution.
### Cluster management layer
MatrixCube is a fundamental library to build distributed systems without considering reliability, consistency as well as scalability, it facilitates building the distributed stateful applications since the developers only need to care about business logic on single node. Currently, it's based on multi-raft to provide replicated state machine and would evolve to paxos families to be more friendly for scenarios across multiple datacenters.
-   Prophet: Used by MatrixCube to manage and schedule the MatrixOne cluster.
-   Transaction Management: MatrixOne supports distributed transaction of snapshot isolation level.
-   Replicated State Machine: MatrixOne currently uses RAFT based consensus algorithm and hyper logic clocks to archive strong consistency of the clusters. Cutting edge state-machine replication protocol will be used in the future.
### Replicated storage layer
-   Row storage: Serving workload, meta data and catalog are stored in row storage.
-   Column storage: Analytical workload, materialized view are stored in columnar storage.
### Storage provision layer
MatrixOne data can be stored in shared storage of S3 / HDFS, even in local disk of public clouds, on-premise server, hybrid cloud and smart devices

## Quick Start
### Building

**Get the MatrxiOne code:**

```
git clone https://github.com/matrixorigin/matrixone.git matrixone
cd matrixone
```

**Run make:**

Run `make debug`, `make clean`, or anything else our Makefile offers. You can just run the following command to build quickly.

```
make config
make build
```

### Starting

**Prerequisites**

- MySQL client

  MatrxiOne supports the MySQL wire protocol, so you can use MySQL client drivers to connect from various languages.

**Boot MatrxiOne server:**

```
./mo-server system_vars_config.toml
```

**Connect MatrxiOne server:**

```
mysql -h IP -P PORT -uUsername -p
```

**For example:**

Test Account:

- user: dump
- password: 111

```
mysql -h 127.0.0.1 -P 6001 -udump -p
```

## Contributing
See [Contributing Guide](CONTRIBUTING.md) for details on contribution workflows.

## Roadmap
Check out [Roadmap](https://github.com/matrixorigin/matrixone/issues/613) for MatrixOne development plan.

## License
MatrixOne is licensed under the [Apache License, Version 2.0](LICENSE)
