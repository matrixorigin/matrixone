# **安装单机版MatrixOne**

MatrixOne 目前支持Linux和MacOS系统，你可以直接从[源代码](#使用源代码搭建)搭建单机版本的MatrixOne，也可以使用[docker](#使用docker)。
## **使用源代码搭建**

#### 1. 安装部署Go语言环境

Go语言至少需要升级到1.17+版本。

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

#### 4. 启动MatrixOne服务器

```
$ ./mo-server system_vars_config.toml
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
