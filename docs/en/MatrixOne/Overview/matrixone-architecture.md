# **MatrixOne Architecture**

## **Architecture**

MatrixOne is the first open-source distributed HSTAP database that supports OLTP, OLAP, and Streaming with a single auto-tuning storage engine.
As a redefinition of the HTAP database, HSTAP aims to meet all the needs of Transactional Processing (TP) and Analytical Processing (AP) within a single database. Compared with the traditional HTAP, HSTAP emphasizes its built-in streaming capability used for connecting TP and AP tables. This provides users with an experience that a database can be used just like a Big Data platform, with which many users are already familiar thanks to the Big Data boom.

With minimal integration efforts, MatrixOne frees users from the limitations of Big Data and provides one-stop coverage for all TP and AP scenarios for enterprises.

MatrixOne is redesigned from scratch and introduces many innovations as well as many best practices in the other DBMS. It adopts a disaggregated storage and computes architecture, where data are stored on S3 object storage in the cloud to achieve low cost, and the compute nodes are stateless and can be started at will to achieve the ultimate elasticity. In the case of on-premises deployment, MatrixOne can utilize the user's HDFS or other supported distributed file system to save data. MatrixOne also comes with an S3-compatible built-in alternative to ensure the elasticity, high reliability, and high performance of its storage without relying on any external components. The architecture is as follows:
![MatrixOne Architecture](https://github.com/matrixorigin/artwork/blob/main/docs/overview/matrixone_new_arch.png?raw=true)

The architecture of MatrixOne is divided into several layers:

## **Cluster Management Layer**

Being responsible for cluster management, it interacts with Kubernetes to obtain resources dynamically when in the cloud-native environment, while in the on-premises deployment, it gets hold of resources based on the configuration. Cluster status is continuously monitored with the role of each node allocated based on resource information. Maintenance works are carried out to ensure that all system components are up and running despite occasional node and network failures. It rebalances the loads on nodes when necessary as well. Major components in this layer are:

* Prophet Scheduler: take charge of load balancing and node keep-alive.
* Resource Manager: being responsible for physical resources provision.

## **Serverless Layer**

Serverless Layer is a general term for a series of stateless nodes, which, as a whole, contains three categories:

* Background tasks: the most important one is called Offload Worker, which is responsible for offloading expensive compaction tasks and flushing data to S3 storage.
* SQL compute nodes: responsible for executing SQL requests, here divided into write nodes and read nodes. The former also provides the ability to read the freshest data.
* Stream task processing node: responsible for executing stream processing requests.

## **Log(Reliability) Layer**

As MatrixOne's Single Source of Truth, data is considered as persistently stored in MatrixOne once it is written into the Log Layer. It is built upon our world-class expertise in the Replicated State Machine model to guarantee state-of-the-art high throughput, high availability, and strong consistency for our data. Following a fully modular and disaggregated design by itself, it is also the central component that helps to decouple the storage and compute layers. This in turn earns our architecture much higher elasticity when compared with traditional NewSQL architecture.

## **Storage Layer**

The storage layer transforms the incoming data from the Log Layer into an efficient form for future processing and storage. This includes cache maintenance for fast accessing data that has already been written to S3. In MatrixOne, TAE (Transactional Analytic Engine) is the primary interface exposed by the Storage Layer, which can support both row and columnar storage together with transaction capabilities. Besides, the Storage Layer includes other internally used storage capabilities as well, e.g. the intermediate storage for streaming.

## **Storage Provision Layer**

As an infrastructure agnostic DBMS, MatrixOne stores data in shared storage of S3 / HDFS, or local disks, on-premise servers, hybrid, and any cloud, or even smart devices. The Storage Provision Layer hides such complexity from upper layers by just presenting them with a unified interface for accessing such diversified storage resources.

## **Learn More**

This page outlines the overall architecture design of MatrixOne. For information on other options that are available when trying out MatrixOne, see the following:

* [Install MatrixOne](../Get-Started/install-standalone-matrixone.md)
* [MySQL Compatibility](mysql-compatibility.md)
* [What's New](whats-new.md)
