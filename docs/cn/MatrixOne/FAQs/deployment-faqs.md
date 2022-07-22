# 部署常见问题

## 操作系统要求

### 部署MatrixOne所需的操作系统版本是什么?

- 单机推荐配置： MatrixOne 0.5.0 支持下表中操作系统。

| Linux OS                 | 版本                   |
| :----------------------- | :------------------------ |
| Red Hat Enterprise Linux | 7.3 or later 7.x releases |
| CentOS                   | 7.3 or later 7.x releases |
| Oracle Enterprise Linux  | 7.3 or later 7.x releases |
| Ubuntu LTS               | 22.04 or later            |

- MatrixOne也支持 macOS 操作系统，当前仅建议在测试和开发环境运行。

| macOS | 版本                |
| :---- | :--------------------- |
| macOS | Monterey 12.3 or later |

## 硬件要求

### MatrixOne对部署硬件的配置要求如何？

单机安装情况下，MatrixOne 0.5.0 可以运行在 Intel x86-64 架构的 64 位通用硬件服务器平台上。

对于开发、测试和生产环境的服务器硬件配置要求和建议如下：

#### 开发和测试环境要求

| CPU     | 内存 | 本地存储   |
| :------ | :----- | :-------------- |
| 4 core+ | 16 GB+ | SSD/HDD 200 GB+ |

ARM 架构的 Macbook M1/M2 也适合开发环境。

#### 生产环境要求

| CPU      | 内存 | 本地存储   |
| :------- | :----- | :-------------- |
| 16 core+ | 64 GB+ | SSD/HDD 500 GB+ |

## 安装和部署

### 安装时需要更改什么设置吗？

通常情况下，安装时，你无需更改任何设置。`system_vars_config.toml` 默认设置完全可以直接运行 MatrixOne。但是如果你需要自定义监听端口、IP 地址、存储数据文件路径，你可以修改相应的 `system_vars_config.toml` 记录。

### 当我安装选择从源代码安装构建 MatrixOne时，产生了以下错误或构建失败提示，我该如何继续？

报错： `Get "https://proxy.golang.org/........": dial tcp 142.251.43.17:443: i/o timeout`

由于 MatrixOne 需要许多 GO 库作为依赖项，所以它在构建时，会同时下载 GO 库。上述所示的报错是下载超时的错误，主要原因是网络问题。

- 如果你使用的是中国大陆的网络，你需要设置你的 GO 环境到一个中国的镜像网站，以加快 GO 库的下载。

- 如果你通过 `go env` 检查你的 GO 环境，你可能会看到 `GOPROXY="https://proxy.golang.org,direct"`，那么你需要进行下面设置：

```
go env -w GOPROXY=https://goproxy.cn,direct
```

设置完成后，`make build` 应该很快就能完成。

### 当我通过 MO-Tester 对 MatrixOne 进行测试时，我如何解决产生的 `too many open files` 错误？

为了对 MatrixOne 进行测试，MO-Tester 会快速地打开和关闭许多 SQL 文件，于是很快就达到 Linux 和 MacOS 系统的最大打开文件限制，这就是导致 `too many open files` 错误的原因。

* 对于 MacOS 系统，你可以通过一个简单的命令设置打开文件的限制：

```
ulimit -n 65536
```

* 对于 Linux 系统，请参考详细的[指南](https://www.linuxtechi.com/set-ulimit-file-descriptors-limit-linux-servers/)，将 *ulimit* 设置为100000。

设置完成后，将不会出现 `too many open files` 错误。
