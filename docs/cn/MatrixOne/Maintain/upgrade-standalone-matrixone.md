# **升级单机版 MatrixOne**

MatrixOne 可以通过以下三种方法进行升级：

- <p><a href="#code_source_update">方法 1：使用源代码进行升级</a></p>
- <p><a href="#binary_packages_update">方法 2：使用二进制包进行升级</a></p>
- <p><a href="#use_docker_update">方法 3：使用 Docker 进行升级</a></p>

- 如果你是使用*源代码安装 MatrixOne* 或通过*二进制包安装 MatrixOne*，选择*使用源代码进行升级*或*使用二进制包进行升级*均可，二者方式互通。

- 如果你是使用 *Docke 安装 MatrixOne*，那么对应选择*使用 Docker 进行版本升级*。

!!! note
    本篇文档升级指导的内容仅适用于 v0.5.0 升级至更高，不适用于 0.4.0/0.3.0/0.2.0/0.1.0 版本升级至 0.5.0 版本。

## 前提条件

- 你已完成安装 v0.5.0 版本 MatrixOne。

## <h2><a name="code_source_update">方法 1：使用源代码进行升级</a></h2>

**场景示例**：

- 本地原 matrixone 目录下需要从版本 v0.5.0 升级到 v0.5.1
- 需要备份 *store* 目录

### 1. 备份 *store* 目录

```
cd matrixone
ls
cp -r store ${path_name}
```

`${path_name}` 为 *store* 所在的备份路径。

!!! note
    如果你从未启动过 MatrxiOne 服务，则不需要执行这一步。

### 2. 获取 MatrixOne v0.5.1 版本代码

```
git pull https://github.com/matrixorigin/matrixone.git
git checkout 0.5.1
```

### 3. 将已备份的 *store* 目录复制回 matrixone 路径下

```
cp -r ${path_name} store
```

### 4. 运行编译文件

你可以运行`make debug`与`make clean`或者其他任何`Makefile`支持的命令。

```
make config
make build
```

### 5. 启动 MatrixOne 服务

```
./mo-server system_vars_config.toml
```

### 6. 连接 MatrixOne 服务

当你完成升级 MatrixOne，你可以参考下面的章节，连接到 MatrixOne 服务器。

参见[连接 MatrixOne 服务](../Get-Started/connect-to-matrixone-server.md)。

## <h2><a name="binary_packages_update">方法 2：使用二进制包进行升级</a></h2>

**场景示例**：

- 本地原 matrixone 目录下需要从版本 v0.5.0 升级到 v0.5.1

- 需要备份 *store* 目录

### 1. 备份 *store* 目录

```
cd matrixone
ls
cp -r store ${path_name}
```

`${path_name}` 为 *store* 所在的备份路径。

!!! note
    如果你从未启动过 MatrxiOne 服务，则不需要执行这一步。

### 2. 下载二进制包并解压

- **Linux 环境**

```bash
wget https://github.com/matrixorigin/matrixone/releases/download/v0.5.0/mo-server-v0.5.1-linux-amd64.zip
unzip mo-server-v0.5.1-linux-amd64.zip
```

- **MacOS 环境**

```bash
wget https://github.com/matrixorigin/matrixone/releases/download/v0.5.0/mo-server-v0.5.1-darwin-x86_64.zip
unzip mo-server-v0.5.1-darwin-x86_64.zip
```

### 3. 覆盖 `mo-server`（选做）

- 如果你是将 v0.5.1 的二进制包下载到了新的路径下（例如，新的路径为 *0.5.1_path*），那么你只需要使用 v0.5.1 二进制包内的 `mo-server` 覆盖 v0.5.0 二进制包内的 `mo-server`。

  ```
  cp -rf 0.5.1_path/matrixone/mo-server matrixone/mo-server
  ```

- 如果你是将 v0.5.1 的二进制包下载到了原有的 matrxione 路径下，则  `mo-server` 将自动覆盖，你无需做其他操作。

### 4. 将已备份的 *store* 目录复制回 matrixone 路径下

```
cp -r ${path_name} store
```

### 5. 启动 MatrixOne 服务

```
./mo-server system_vars_config.toml
```

### 6. 连接 MatrixOne 服务

当你完成升级 MatrixOne，你可以参考下面的章节，连接到 MatrixOne 服务器。

参见[连接 MatrixOne 服务](../Get-Started/connect-to-matrixone-server.md)。

## <h2><a name="use_docker_update">方法 3：使用 Docker 进行升级</a></h2>

如果你无需保留历史数据，那么可以使用以下命令将从 Docker Hub 中下载 MatrixOne v0.5.1 版本的镜像。

```
docker ps
docker stop matrixone
docker rm matrixone
docker pull matrixorigin/matrixone:0.5.1
docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:0.5.1
```

如果你需要保留历史数据，请参考下面的场景示例：

**场景示例**：

- 已使用 Docker 运行了 v0.5.0版本 MatrixOne

- 未将容器路径挂载到本地磁盘，升级时需要保留历史数据

### 1. 将容器中的 *store* 目录备份到宿主机

① 查询当前正在运行的 Matrixone 的容器，检查 MatrixOne 是否正在运行，如果正在运行，将 MatrixOne 运行停止。

```
docker ps
docker stop matrixone
```

② 将当前 Matrixone 容器中的 *store* 目录拷贝到本地备份路径下。

```
docker cp matrixone:/store ${path_name}/
```

`${path_name}` 为 *store* 所在的备份路径。

### 2. 删除 v0.5.0 版本的 MatrixOne

```
docker rm matrixone
```

### 3. 下载 v0.5.1 版本 MatrixOne 镜像

使用以下命令将从 Docker Hub 中下载 MatrixOne v0.5.1 版本的镜像。

```
docker pull matrixorigin/matrixone:0.5.1
```

### 4. 启动容器，挂载 `/store` 目录

启动容器，将 `/store` 目录挂载到本地磁盘已备份好的 *store* 文件夹所在的位置：

```
docker run -d -p 6001:6001 -v ~/tmp/store:/store:rw --name matrixone matrixorigin/matrixone:0.5.1
```

如果需要自定义配置文件，也可以直接挂载存放在本地磁盘的自定义配置文件：

```
docker run -d -p 6001:6001 -v ${path_name}/system_vars_config.toml:/system_vars_config.toml:ro -v ${path_name}/store:/store:rw --name matrixone matrixorigin/matrixone:0.5.1
```

### 5. 连接 MatrixOne 服务

当你完成安装 MatrixOne，你可以参考下面的章节，连接到 MatrixOne 服务器。

参见[连接 MatrixOne 服务](../Get-Started/connect-to-matrixone-server.md)。

## 参考文档

常见的安装和部署问题，参见[安装和部署常见问题](../FAQs/deployment-faqs.md)。
