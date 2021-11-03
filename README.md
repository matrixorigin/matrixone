[![LICENSE](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![CodeFactor](https://www.codefactor.io/repository/github/matrixorigin/matrixone/badge?s=7280f4312fca2f2e6938fb8de5b726c5252541f0)](https://www.codefactor.io/repository/github/matrixorigin/matrixone)

## What is MatrixOne?
MatrixOne is a planet scale, cloud-edge native big data engine crafted for heterogeneous workloads. It provides an end-to-end data processing platform that is highly autonomous and easy to use, to empower users to store, manipulate, and analyze data across devices, edges, and clouds, with minimal operational overheads.

## Features

### Planet Scalability
MatrixOne cluster can easily expand capacity during SQL processing, computation, and storage, by adding nodes to the cluster on the fly.

### Cloud-Edge Native
Not limited to public clouds, hybrid clouds, on-premise servers, or smart devices, MatrixOne accommodates itself to myriads of infrastructure while still providing top services with low latency and high throughput.

### Hybrid Streaming, Transactional and Analytical Processing Engine
By converging multiple engines, MatrixOne can support hybrid streaming, transactional, and analytical workloads; with its pluggable architecture, MatrixOne allows for easy integration with third-party engines.

### High Availability
MatrixOne uses a RAFT-based consensus algorithm to provide fault tolerance in one zone. And a more advanced state-machine replication protocol is planned for the future to achieve geo-distributed active-active.

### Ease of Use
An important goal of MatrixOne is to make it easy for users to operate and manage data, making daily work almost effortless.
- **No Dependency**: Download, install, or start MatrixOne straightforwardly without depending on external toolings.
- **Simplify Administration**: Re-balancing, failover, system tuning, and other administrative tasks are fully automatic.
- **MySQL-compatible Syntax**: MatrixOne allows you to query data using traditional SQL queries.

### End-to-End Automated Data Science
By streaming SQL and user-defined functions, MatrixOne provides end-to-end data processing pipelines to deliver productive data science applications.

## Architecture
![Architecture](https://github.com/matrixorigin/artwork/blob/main/diagram/overall-architecture.png)

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
-   **Replicated State Machine**: MatrixOne uses RAFT-based consensus algorithms and hyper logic clocks to implement strong consistency of the clusters. Introduction of more advanced state-machine replication protocols is yet to come.
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
