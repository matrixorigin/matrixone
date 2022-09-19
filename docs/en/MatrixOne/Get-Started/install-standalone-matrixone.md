# **Install standalone MatrixOne**

MatrixOne supports Linux and MacOS. You can install a standalone MatrixOne version by 3 methods:

- <p><a href="#code_source">Method 1: Building from source</a>.</p>
- <p><a href="#binary_packages">Method 2: Using binary package</a>.</p>
- <p><a href="#use_docker">Method 3: Using Docker</a>.</p>

Recommended hardware specification: x86 CPU with 4 cores and 32GB memory, with CentOS 7+ OS.

## <h2><a name="code_source">Method 1: Building from source</a></h2>

### 1. Install Go as necessary dependancy

Go version 1.19 is required.

### 2. Get the MatrixOne code to build MatrixOne

Depending on your needs, choose whether you want to keep your code up to date, or if you want to get the latest stable version of the code.

#### Option 1: Get the MatrixOne(Develop Version) code to build

The **main** branch is the default branch, the code on the main branch is always up-to-date but not stable enough.

1. Get the MatrixOne(Develop Version, also called Pre0.6 version) code:

    ```
    git clone https://github.com/matrixorigin/matrixone.git
    cd matrixone
    ```

2. You can run `make debug`, `make clean`, or anything else our Makefile offers.

    ```
    make build
    ```

3. Launch MatrixOne server：

    !!! note
         The startup-config file of MatrixOne(Develop Version) is different from the startup-config file of MatrixOne(Stable Version). The startup-config file code of MatrixOne(Develop Version) is as below:

    ```
    ./mo-service -cfg ./etc/cn-standalone-test.toml
    ```

#### Option 2: Get the MatrixOne(Stable Version) code to build

1. If you want to get the latest stable version code released by MatrixOne, please switch to the branch of version **0.5.1** first.

    ```
    git clone https://github.com/matrixorigin/matrixone.git
    git checkout 0.5.1
    cd matrixone
    ```

2. You can run `make debug`, `make clean`, or anything else our Makefile offers.

    ```
    make config
    make build
    ```

3. Launch MatrixOne server：

    !!! note
         The startup-config file of MatrixOne(Stable Version) is different from the startup-config file of MatrixOne(Develop Version). The startup-config file code of MatrixOne(Stable Version) is as below:

    ```
    ./mo-server system_vars_config.toml
    ```

### 3. Connect to MatrixOne Server

When you finish installing MatrixOne, you can refer to the section below to connect to the MatrixOne server.

See [Connect to MatrixOne server](connect-to-matrixone-server.md).

## <h2><a name="binary_packages">Method 2: Downloading binary packages</a></h2>

For each release, you can download binary packages directly to run MatrixOne in the X86_64 Linux or Mac X86_64 environment.

### 1. Download binary packages and decompress

Linux Environment

```bash
wget https://github.com/matrixorigin/matrixone/releases/download/v0.5.1/mo-server-v0.5.1-linux-amd64.zip
unzip mo-server-v0.5.1-linux-amd64.zip
```

MacOS Environment

```bash
wget https://github.com/matrixorigin/matrixone/releases/download/v0.5.1/mo-server-v0.5.1-darwin-x86_64.zip
unzip mo-server-v0.5.1-darwin-x86_64.zip
```

### 2.Launch MatrixOne server

```
./mo-server system_vars_config.toml
```

### 3. Connect to MatrixOne Server

When you finish installing MatrixOne, you can refer to the section below to connect to the MatrixOne server.

See [Connect to MatrixOne server](connect-to-matrixone-server.md).

## <h2><a name="use_docker">Method 3: Using docker</a></h2>

### 1. Install Docker

Please verify that Docker daemon is running in the background:

```
docker --version
```

### 2. Create and run the container of MatrixOne

It will pull the image from Docker Hub if not exists. You can choose to pull the stable version image or the develop version image.

- Stable Version Image(0.5.1 version)

```bash
docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:0.5.1
```

- If you want to pull the develop version image, see [Docker Hub](https://hub.docker.com/r/matrixorigin/matrixone/tags), get the image tag. An example as below:

    Develop Version Image(Pre0.6 version)

    ```bash
    docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:nightly-commitnumber
    ```
    
    !!! info
         The *nightly* version is updated once a day.
    
For the information on the user name and password, see the next step - Connect to MatrixOne Server.

### 3. Mount the data directory(Optional)

To customize the configuration file, you can mount the custom configuration file stored on the local disk.

```
docker run -d -p 6001:6001 -v ${path_name}/system_vars_config.toml:/system_vars_config.toml:ro -v ${path_name}/store:/store:rw --name matrixone matrixorigin/matrixone:0.5.1
```

### 4. Connect to MatrixOne Server

When you finish installing MatrixOne, you can refer to the section below to connect to the MatrixOne server.

See [Connect to MatrixOne server](connect-to-matrixone-server.md).

## Reference

- For more information on update, see [Update Standalone MatrixOne](update-standalone-matrixone.md).
- For more information on deployment，see [Deployment FAQs](../FAQs/deployment-faqs.md).
