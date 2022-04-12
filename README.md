<div class="column" align="middle">
  <a href="https://github.com/matrixorigin/matrixone/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-red.svg" alt="license"/>
  </a>
  <a href="https://golang.org/">
    <img src="https://img.shields.io/badge/Language-Go-blue.svg" alt="language"/>
  </a>
  <img src="https://img.shields.io/badge/platform-MacOS-white.svg" alt="macos"/>
  <img src="https://img.shields.io/badge/platform-Linux-9cf.svg" alt="linux"/>
  <a href="https://www.codefactor.io/repository/github/matrixorigin/matrixone">
    <img src="https://www.codefactor.io/repository/github/matrixorigin/matrixone/badge?s=7280f4312fca2f2e6938fb8de5b726c5252541f0" alt="codefactor"/>
  </a>
  <a href="https://docs.matrixorigin.io/0.3.0/MatrixOne/Release-Notes/v0.3.0/">
   <img src="https://img.shields.io/badge/Release-v0.3.0-green.svg" alt="release"/>
  </a>
  <br>
  <a href="https://docs.matrixorigin.io/0.3.0/">
    <b>Docs</b>
  </a>
  <b>||</b>
  <a href="https://www.matrixorigin.io/">
   <b> Official Website</b>
  </a>
  <br>
  <a href="https://github.com/matrixorigin/matrixone/blob/main/README.md">
    <b>English</b>
  </a>
  <b>||</b>
  <a href="https://github.com/matrixorigin/matrixone/blob/main/README_CN.md">
    <b>简体中文</b>
  </a>
</div>


<h3 align="center">Connect with us:</h3>
<p align="center">
<a href="https://twitter.com/matrixone16" target="blank"><img align="center" src="https://raw.githubusercontent.com/rahuldkjain/github-profile-readme-generator/master/src/images/icons/Social/twitter.svg" alt="matrixone16" height="30" width="40" /></a>
<a href="http://matrixoneworkspace.slack.com" target="blank"><img align="center" src="https://github.com/dengn/CASAUVSQ/blob/priority/slack_icon.png" alt="matrixone16" height="30" width="30" /></a>

</p>

<h5 align="center">If you are interested in MatrixOne project, please kindly give MatrixOne a triple `Star`, `Fork` and `Watch`, Thanks!</h5>




Contents
========

* [What is MatrixOne](#what-is-matrixone)
* [KeyFeatures](#key-features)
* [User Values](#user-values)
* [Architecture](#architecture)
* [Quick start](#quick-start)
* [Contributing](#contributing)
* [License](#license)

## What is MatrixOne?



MatrixOne is a future-oriented hyper-converged cloud and edge native DBMS that supports transactional, analytical, and streaming workloads with a simplified and distributed database engine, across multiple data centers, clouds, edges and other heterogeneous infrastructures.


##  🎯 <a id="key-features">Key Features</a> 
### 💥 **Hyper-converged Engine**

<details>
  <summary><b><font size=4>Monolithic Engine</b></font></summary> 
          A monolithic database engine is designed to support hybrid workloads: transactional, analytical, streaming, time-series, machine learning, etc.
</details>

<details>
  <summary><b><font size=4>Built-in Streaming Engine</b></font></summary> 
               With the built-in streaming engine, MatrixOne supports in-database streaming processing by groundbreaking incremental materialized view maintenance.
</details>


### ☁️ **Cloud & Edge Native**

<details>
  <summary><b><font size=4>Real Infrastructure Agnostic</b></font></summary> 
               MatrixOne supports seemless workload migration and bursting among different locations and infrastructures.
</details>

<details>
  <summary><b><font size=4>Multi-site Active/Active</b></font></summary> 
                    MatrixOne provides industry-leading latency control with optimized consistency protocol.
</details>

### 🚀 **Extreme Performance**

<details>
  <summary><b><font size=4>High Performance</b></font></summary> 
     Accelerated queries supported by patented vectorized execution as well as optimal computation push down strategies through factorization techniques.
</details>

<details>
  <summary><b><font size=4>Strong Consistency</b></font></summary> 
     MatrixOne introduces a global, high-performance distributed transaction protocol across storage engines.
</details>

<details>
  <summary><b><font size=4>High Scalability</b></font></summary> 
     Seamless and non-disruptive scaling by disaggregated storage and compute.   
</details>

## 💎 **<a id="user-values">User Values</a>**
<details>
  <summary><b><font size=4>Simplify Database Management and Maintenance</b></font></summary> 
     To solve the problem of high and unpredictable cost of database selection process, management and maintenance due to database overabundance, MatrixOne all-in-one architecture will significantly simplify database management and maintenance, single database can serve multiple data applications.
</details>
<details>
  <summary><b><font size=4>Reduce Data Fragmentation and Inconsistency</b></font></summary>
     Data flow and copy between different databases makes data sync and consistency increasingly difficult. The unified incrementally materialized view of MatrixOne makes the downstream can support real-time upstream update, achieve the end-to-end data processing without redundant ETL process.
</details>
<details>
  <summary><b><font size=4>Decoupling Data Architecture From Infrastructure</b></font></summary>
     Currently the architecture design across different infrastructures is complicated, causes new data silos between cloud and edge, cloud and on-premise. MatrixOne is designed with unified architecture to support simplified data management and operations across different type of infrastructures.
</details>
<details>
  <summary><b><font size=4>Extremely Fast Complex Query Performance</b></font></summary>  
     Poor business agility as a result of slow complex queries and redundant intermediate tables in current data warehousing solutions. MatrixOne  supports blazing fast experience even for star and snowflake schema queries, improving business agility by real-time analytics.
</details>
<details>
  <summary><b><font size=4>A Solid OLTP-like OLAP Experience</b></font></summary>   
     Current data warehousing solutions have the following problems such as high latency and absence of immediate visibility for data updates. MatrixOne brings OLTP (Online Transactional Processing) level consistency and high availability to CRUD operations in OLAP (Online Analytical Processing).
</details>
<details>
  <summary><b><font size=4>Seamless and Non-disruptive Scalability</b></font></summary>   
     It is difficult to balance performance and scalability to achieve optimum price-performance ratio in current data warehousing solutions. MatrixOne's disaggregated storage and compute architecture makes it fully automated and efficient scale in/out and up/down without disrupting applications.
</details>

<br>


## 🔎 <a id="architecture">Architecture</a>
MatrixOne's architecture is as below:   
<p align="center">
  <img alt="MatrixOne" height="500" width="700" src="https://github.com/matrixorigin/artwork/blob/main/docs/overview/overall-architecture.png?raw=true">
</p>

For more details, you can checkout [MatrixOne Architecture](https://docs.matrixorigin.io/0.3.0/MatrixOne/Overview/matrixone-architecture/) and [MatrixOne Tech Design](https://docs.matrixorigin.io/0.3.0/MatrixOne/Overview/MatrixOne-Tech-Design/matrixone-techdesign/).


## ⚡️ <a id="quick-start">Quick start</a>


### ⚙️ Install MatrixOne
MatrixOne supports Linux and MacOS. You can install MatrixOne either by [building from source](#building-from-source) or [using docker](#using-docker).
For other installation types, please refer to [MatrixOne installation](https://docs.matrixorigin.io/0.3.0/MatrixOne/Get-Started/install-standalone-matrixone/) for more details.
#### **Building from source**

1. Install Go (version 1.18 is required).
  
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


#### **Using docker**
1. Make sure Docker is installed, verify Docker daemon is running in the background:

```
$ docker --version
```

2. Create and run the container for the latest release of MatrixOne. It will pull the image from Docker Hub if not exists.
   
```
$ docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:latest
```

### 🌟 Connecting to MatrixOne server

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




## 🙌 <a id="contributing">Contributing</a>

Contributions to MatrixOne are welcome from everyone.  
 See [Contribution Guide](https://docs.matrixorigin.io/0.3.0/MatrixOne/Contribution-Guide/make-your-first-contribution/) for details on submitting patches and the contribution workflow. 

### 👏 All contributors

<!-- readme: contributors -start -->
<!-- readme: contributors -end -->



## <a id="license">License</a>
MatrixOne is licensed under the [Apache License, Version 2.0](LICENSE).


