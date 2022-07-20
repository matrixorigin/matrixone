<div class="column" align="middle">
  <p align="center">
   <img alt="MatrixOne All in One" height="50" src="https://github.com/matrixorigin/artwork/blob/main/docs/overview/logo.png?raw=true">
  </p>
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
  <a href="https://docs.matrixorigin.io/0.5.0/MatrixOne/Release-Notes/v0.5.0/">
   <img src="https://img.shields.io/badge/Release-v0.5.0-green.svg" alt="release"/>
  </a>
  <br>
  <a href="https://docs.matrixorigin.io/cn/0.5.0/">
    <b>Docs</b>
  </a>
  <b>||</b>
  <a href="https://www.matrixorigin.cn/">
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

<h3 align="center">加入MatrixOne社区</h3>
<p align="center">
<a href="https://26805113.s21i.faiusr.com/4/ABUIABAEGAAgv7rJjwYo977zvgQw1AY41AY!200x200.png.webp" target="blank"><img align="center" src="https://github.com/dengn/CASAUVSQ/blob/priority/wechat-logo.png" alt="matrixone16" height="30" width="30" /></a>
<a href="http://matrixoneworkspace.slack.com" target="blank"><img align="center" src="https://github.com/dengn/CASAUVSQ/blob/priority/slack_icon.png" alt="matrixone16" height="30" width="30" /></a>

</p>

<h5 align="center">如果你对MatrixOne项目感兴趣的话, 请帮忙给MatrixOne点击Star, Fork和Watch三连吧, 谢谢!</h5>


目录
========

* [MatrixOne是什么？](#what-is-matrixone)
* [核心特性](#key-features)
* [用户价值](#user-values)
* [架构](#architecture)
* [快速上手](#quick-start)
* [参与贡献](#contributing)
* [License](#license)

## <a id="what-is-matrixone">MatrixOne是什么？</a>

MatrixOne是一款面向未来的超融合异构云原生数据库，通过超融合数据引擎支持事务/分析/流处理等混合工作负载，通过异构云原生架构支持跨机房协同/多地协同/云边协同。简化开发运维，消简数据碎片，打破数据的系统、位置和创新边界。
<p align="center">
  <img alt="MatrixOne" height="500" src="https://github.com/matrixorigin/artwork/blob/main/docs/overview/all-in-one.png?raw=true">
</p>

##  🎯 <a id="key-features">核心特性</a>
### 💥 **超融合引擎**

<details>
  <summary><b><font size=4>超融合引擎</b></font></summary>
           融合数据引擎，单数据库即可支持TP、AP、时序、机器学习等混合工作负载。
</details>

<details>
  <summary><b><font size=4>内置流引擎</b></font></summary>
     利用独有的增量物化视图能力，无需跨数据库即可实现实时数据流处理。
</details>


### ☁️ **异构云原生**


<details>
  <summary><b><font size=4>异构统一</b></font></summary>
     支持跨机房协同/多地协同/云边协同，实现无感知扩缩容，提供高效统一的数据管理。
</details>

<details>
  <summary><b><font size=4>多地多活</b></font></summary>
     MatrixOne采用最优的一致性协议，实现业内最短网络延迟的多地多活。
</details>


### 🚀 **极致的性能**

<details>
  <summary><b><font size=4>高性能</b></font></summary>     特有的因子化计算和向量化执行引擎，支持极速的复杂查询。单表、星型和雪花查询都具备极速分析性能。</details>

<details>
  <summary><b><font size=4>强一致</b></font></summary>
     提供跨存储引擎的高性能全局分布式事务能力，在保证极速分析性能的同时支持更新、删除和实时点查询。
</details>

<details>
  <summary><b><font size=4>高可用</b></font></summary>
     存算分离，支持存储节点与计算节点独立扩缩容，高效应对负载变化。
</details>



## 💎 **<a id="user-values">用户价值</a>**
<details>
  <summary><b><font size=4>简化数据开发和运维</b></font></summary>
      随着业务发展，企业使用的数据引擎和中间件越来越多，而每一个数据引擎平均依赖5+个基础组件，存储3+个数据副本，每一个数据引擎都要各自安装、监控、补丁和升级。这些都导致数据引擎的选型、开发及运维成本高昂且不可控。在MatrixOne的一体化架构下，用户使用单个数据库即可服务多种数据应用，引入的数据组件和技术栈减少80%，大大简化了数据库管理和维护的成本。
</details>
<details>
  <summary><b><font size=4>消减数据碎片和不一致</b></font></summary>
    在既有复杂的系统架构内，存在多条数据管道多份数据存储冗余。数据依赖复杂，导致数据更新维护复杂，上下游数据不一致问题频发，人工校对难度增大。MatrixOne的高内聚架构和独有的增量物化视图能力，使得下游可以支持上游数据的实时更新，摆脱冗余的ETL流程，实现端到端实时数据处理。
</details>
<details>
  <summary><b><font size=4>无需绑定基础设施</b></font></summary>
    因为基础设施的碎片化，企业的私有化数据集群和公有云数据集群之间数据架构和建设方案割裂，数据迁移成本高。而数据上云一旦选型确定数据库厂商，后续的集群扩容、其他组件采购等都将被既有厂商绑定。MatrixOne提供统一的云边基础架构和高效统一的数据管理，企业数据架构不再被基础设施绑定，实现单数据集群跨云无感知扩缩容，提升性价比。
</details>
<details>
  <summary><b><font size=4>极速的分析性能</b></font></summary>  
    目前，由于缓慢的复杂查询性能以及冗余的中间表，数据仓库在业务敏捷性上的表现不尽人意，大量宽表的创建也严重影响迭代速度。MatrixOne通过特有的因子化计算和向量化执行引擎，支持极速的复杂查询，单表、星型和雪花查询都具备极速分析性能。
</details>
<details>
  <summary><b><font size=4>像TP一样可靠的AP体验</b></font></summary>   
    传统数据仓库数据更新代价非常高，很难做到数据更新即可见。在营销风控，无人驾驶，智能工厂等实时计算要求高的场景或者上游数据变化快的场景中，当前的大数据分析系统无法支持增量更新，往往需要做全量的更新，耗时耗力。MatrixOne通过提供跨存储引擎的高性能全局分布式事务能力，支持条级别的实时增量更新，在保证极速分析性能的同时支持更新、删除和实时点查询。
</details>
<details>
  <summary><b><font size=4>不停服自动扩缩容</b></font></summary>   
    传统数仓无法兼顾性能和灵活度，性价比无法做到最优。MatrixOne基于存算分离的技术架构，支持存储节点与计算节点独立扩缩容，高效应对负载变化。
</details>

## 🔎 <a id="architecture">架构一览</a>
MatrixOne的架构图如下图所示：   
<p align="center">
  <img alt="MatrixOne" height="500" src="https://github.com/matrixorigin/artwork/blob/main/docs/overview/matrixone_new_arch.png?raw=true">
</p>

关于更详细的MatrixOne技术架构，可以参考[MatrixOne架构](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Overview/matrixone-architecture/)。

## ⚡️ <a id="quick-start">快速上手</a>

### ⚙️ 安装MatrixOne

MatrixOne目前支持Linux及MacOS系统，您可以通过源码安装或者docker安装。其他安装方式请参见[MatrixOne安装指南](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/)

#### 使用源代码搭建

**步骤 1.** 搭建Go语言环境（至少需要1.18版本）。

**步骤 2.** 获取MatrixOne源码

根据您的需要，选择您所获取的代码永远保持最新，还是获得稳定版本的代码。

- *选项 1*：获取 MatrixOne(预览版本) 代码

**main** 分支是默认分支，主分支上的代码总是最新的，但不够稳定。

获取 MatrixOne(预览版本) 代码方法如下：

```
$ git clone https://github.com/matrixorigin/matrixone.git
$ cd matrixone
```

- *选项 2*：获取 MatrixOne(稳定版本) 代码

如果您想获得 MatrixOne 发布的最新稳定版本代码，请先从 **main** 切换选择至 **0.5.0** 版本分支。

```
$ git clone https://github.com/matrixorigin/matrixone.git
$ git checkout 0.5.0
$ cd matrixone
```

**3.** 运行编译文件

你可以运行`make debug`与`make clean`或者其他任何Makefile命令。

```
$ make config
$ make build
```

**4.** 启动MatrixOne服务

```
$ ./mo-server system_vars_config.toml
```

#### 使用docker

**步骤 1.** 确保docker已经安装

请检查Docker daemon是否正在后台运行，并确认docker版本：
```
$ docker --version
```
**步骤 2.** 创建并运行容器

使用以下命令将从 Docker Hub 中拉取 MatrixOne 镜像，你可以选择最新的镜像，或稳定版本的镜像。

- 最新版本的镜像：

```
$ docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:latest
```

- 0.5.0 稳定版本的镜像

```
$ docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:0.5.0
```

运行 Docker Hub 时需要输入用户名和密码，获取用户名和密码可以参考下一步骤 - 连接 MatrixOne 服务。

### 🌟 连接MatrixOne服务

1. 安装MySQL客户端

  MatrixOne支持MySQL连接协议，因此您可以使用各种语言通过MySQL客户机程序进行连接。  
  目前，MatrixOne只兼容Oracle MySQL客户端，因此一些特性可能无法在MariaDB、Percona客户端下正常工作。  

2. 连接MatrixOne服务

你可以使用MySQL命令行客户端来连接MatrixOne服务。  

```
$ mysql -h IP -P PORT -uUsername -p
```

连接符的格式与MySQL格式相同，您需要提供用户名和密码。  
此处以内置帐号作为示例：  

   - user: dump
   - password: 111

```
$ mysql -h 127.0.0.1 -P 6001 -udump -p
Enter password:
```

目前，MatrixOne只支持TCP监听。

## 🙌 <a id="contributing">参与贡献</a>

欢迎大家对MatrixOne的贡献。  
请查看[贡献指南](https://docs.matrixorigin.io/cn/0.5.0/MatrixOne/Contribution-Guide/make-your-first-contribution/)来了解有关提交补丁和完成整个贡献流程的详细信息。

### 👏贡献者们

<!-- readme: contributors -start -->
<!-- readme: contributors -end -->

## <a id="license">License</a>
[Apache License, Version 2.0](LICENSE)。
