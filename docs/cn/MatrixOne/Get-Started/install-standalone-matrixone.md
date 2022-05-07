# **安装单机版MatrixOne**

作为一款开源数据库，MatrixOne目前支持主流的Linux和MacOS系统。你可以直接从[源代码](#使用源代码搭建)搭建单机版本的MatrixOne，也可以使用[docker](#使用docker)安装部署。

## **从 AUR 安装**

ArchLinux 用户可以从 [AUR](https://aur.archlinux.org/packages/matrixone) 安装 MatrixOne。

```
$ git clone https://aur.archlinux.org/matrixone.git
$ cd matrixone
$ makepkg -rsi
```

## **使用源代码搭建**

#### 1. 安装部署Go语言环境

Go语言需要升级到1.18版本。

#### 2. 获取MatrixOne源码

```
$ git clone https://github.com/matrixorigin/matrixone.git
$ cd matrixone
```

#### 3. 运行编译文件
你可以运行`make debug`与`make clean`或者其他任何`Makefile`支持的命令。

```
$ make config
$ make build
```

#### 4. 启动MatrixOne服务

```
$ ./mo-server system_vars_config.toml
```
## **下载二进制包**
从0.3.0版本开始，您可以直接下载二进制包，然后在X86_64 Linux环境或者X86_64的MacOS环境中运行MatrixOne。
#### 1. 下载二进制包并解压

Linux环境
```bash
$ wget https://github.com/matrixorigin/matrixone/releases/download/v0.4.0/mo-server-v0.4.0-linux-amd64.zip
$ unzip mo-server-v0.4.0-linux-amd64.zip
```

MacOS环境
```bash
$ https://github.com/matrixorigin/matrixone/releases/download/v0.4.0/mo-server-v0.4.0-darwin-x86_64.zip
$ unzip mo-server-v0.4.0-darwin-x86_64.zip
```

#### 2. 启动MatrixOne服务
```
$./mo-server system_vars_config.toml
```

## **使用docker**

#### 1. 安装docker

请检查Docker daemon是否正在后台运行，并确认docker版本：
```
$ docker --version
```
#### 2. 创建并运行容器

使用以下命令将从Docker Hub中拉取最近的MatrixOne镜像：
```
$ docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:latest
```
