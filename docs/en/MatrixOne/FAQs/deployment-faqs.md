
# Deployment FAQs

### Operating system requirements

##### What are the required operating system versions for deploying MatrixOne?

For standalone installation, MatrixOne 0.5.0 supports the following operating system:

| Linux OS                 | Version                   |
| :----------------------- | :------------------------ |
| Red Hat Enterprise Linux | 7.3 or later 7.x releases |
| CentOS                   | 7.3 or later 7.x releases |
| Oracle Enterprise Linux  | 7.3 or later 7.x releases |
| Ubuntu LTS               | 22.04 or later            |

MatrixOne also supports macOS operating system, but it's only recommended to run as a test and development environment.

| macOS | Version                |
| :---- | :--------------------- |
| macOS | Monterey 12.3 or later |

### Hardware requirements

##### What are the required hardware for deploying MatrixOne?

For standalone installation, MatrixOne 0.5.0 can be running on the 64-bit generic hardware server platform in the Intel x86-64 architecture. The requirements and recommendations about server hardware configuration for development, testing and production environments are as follows:

##### Development and testing environments

| CPU     | Memory | Local Storage   |
| :------ | :----- | :-------------- |
| 4 core+ | 16 GB+ | SSD/HDD 200 GB+ |

The Macbook M1/M2 with ARM architecture is also a good fit for development environment.

##### Production environment

| CPU      | Memory | Local Storage   |
| :------- | :----- | :-------------- |
| 16 core+ | 64 GB+ | SSD/HDD 500 GB+ |

### Installation and deployment

##### What settings do I need to change for installation?

Normally you don't need to change anything for installation. A default setting of `system_vars_config.toml` is enough to run MatrixOne directly. But if you want to customize your listening port, ip address, stored data files path, you may modify the corresponding records of`system_vars_config.toml`.

##### When I install MatrixOne by building from source, I got an error of the following and the build failed, how can I proceed?

Error: `Get "https://proxy.golang.org/........": dial tcp 142.251.43.17:443: i/o timeout`

As MatrixOne needs many go libraries as dependency, it downloads them at the time of building it. This is an error of downloading timeout, it's mostly a networking issue. If you are using a Chinese mainland network, you need to set your go environment to a Chinese image site to accelerate the go library downloading. If you check your go environment by `go env`, you may see  `GOPROXY="https://proxy.golang.org,direct"`, you need to set it by

```
go env -w GOPROXY=https://goproxy.cn,direct
```

Then the `make build` should be fast enough to finish.

##### When I was testing MatrixOne with MO-tester, I got an error of `too many open files`?

MO-tester will open and close many SQL files in a high speed to test MatrixOne, this kind of usage will easily reach the maximum open file limit of Linux and MacOS, which is the reason for the `too many open files` error.

* For MacOS, you can just set the open file limit by a simple command:

```
ulimit -n 65536
```

* For Linux, please refer to this detailed [guide](https://www.linuxtechi.com/set-ulimit-file-descriptors-limit-linux-servers/) to set the ulimit to 100000.

After setting this value, there will be no more `too many open files` error.
