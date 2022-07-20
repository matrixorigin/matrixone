# **安装单机版 MatrixOne**

作为一款开源数据库，MatrixOne 目前支持主流的 Linux 和 MacOS 系统。你可以通过以下三种方法安装单机版 MatrixOne：

- [方法 1：使用源代码搭建](#方法 1：使用源代码搭建)
- [方法 2：下载二进制包](#方法 2：下载二进制包)
- [方法 3：使用 Docker](方法 3：使用 Docker)

推荐硬件规格：x86 CPU；4核；32GB 内存，支持 CentOS 7+ 操作系统。

## **方法 1：使用源代码搭建**

#### 1. 安装部署 Go 语言环境

Go 语言需要升级到 1.18 版本。

#### 2. 获取 MatrixOne 源码

根据您的需要，选择您所获取的代码永远保持最新，还是获得稳定版本的代码。

##### 选项 1：获取 MatrixOne(预览版本) 代码

**main** 分支是默认分支，主分支上的代码总是最新的，但不够稳定。

获取 MatrixOne(预览版本) 代码方法如下：

```
$ git clone https://github.com/matrixorigin/matrixone.git
$ cd matrixone
```

##### 选项 2：获取 MatrixOne(稳定版本) 代码

如果您想获得 MatrixOne 发布的最新稳定版本代码，请先从 **main** 切换选择至 **0.5.0** 版本分支。

```
$ git clone https://github.com/matrixorigin/matrixone.git
$ git checkout 0.5.0
$ cd matrixone
```

#### 3. 运行编译文件

你可以运行`make debug`与`make clean`或者其他任何`Makefile`支持的命令。

```
$ make config
$ make build
```

#### 4. 启动 MatrixOne 服务

```
$ ./mo-server system_vars_config.toml
```

#### 5. 连接 MatrixOne 服务

当你完成安装 MatrixOne，你可以参考下面的章节，连接到 MatrixOne 服务器。

参见[连接 MatrixOne 服务](connect-to-matrixone-server.md)。

## **方法 2：下载二进制包**

从 0.3.0 版本开始，您可以直接下载二进制包，然后在 X86_64 Linux 环境或者 X86_64 的 MacOS 环境中运行 MatrixOne。

#### 1. 下载二进制包并解压

- **Linux 环境**

```bash
$ wget https://github.com/matrixorigin/matrixone/releases/download/v0.4.0/mo-server-v0.4.0-linux-amd64.zip
$ unzip mo-server-v0.4.0-linux-amd64.zip
```

- **MacOS 环境**

```bash
$ wget https://github.com/matrixorigin/matrixone/releases/download/v0.4.0/mo-server-v0.4.0-darwin-x86_64.zip
$ unzip mo-server-v0.4.0-darwin-x86_64.zip
```

#### 2. 启动 MatrixOne 服务

```
$./mo-server system_vars_config.toml
```

#### 3. 连接 MatrixOne 服务

当你完成安装 MatrixOne，你可以参考下面的章节，连接到 MatrixOne 服务器。

参见[连接 MatrixOne 服务](connect-to-matrixone-server.md)。

## **方法 3：使用 Docker**

#### 1. 安装 Docker

请检查 Docker daemon 是否正在后台运行，并确认 Docker 版本：

```
$ docker --version
```

#### 2. 创建并运行容器

使用以下命令将从 Docker Hub 中拉取 MatrixOne 镜像，你可以选择最新的镜像，或稳定版本的镜像。

- 最新版本的镜像：

```bash
$ docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:latest
```

- 0.5.0 稳定版本的镜像

```bash
$ docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:0.5.0
```

运行 Docker Hub 时需要输入用户名和密码，获取用户名和密码可以参考下一步骤 - 连接 MatrixOne 服务

#### 3. 连接 MatrixOne 服务

当你完成安装 MatrixOne，你可以参考下面的章节，连接到 MatrixOne 服务器。

参见[连接 MatrixOne 服务](connect-to-matrixone-server.md)。

## 参考文档

常见的安装和部署问题，参见[安装和部署常见问题](../FAQs/deployment-faqs.md)。
