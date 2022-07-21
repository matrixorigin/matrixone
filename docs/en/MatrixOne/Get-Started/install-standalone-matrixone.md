# **Install standalone MatrixOne**

MatrixOne supports Linux and MacOS. You can install a standalone MatrixOne version by 3 methods:

* Building from source.
* Using binary package.
* Using Docker.

Recommended hardware specification: x86 CPU with 4 cores and 32GB memory, with CentOS 7+ OS.

## **Method 1: Building from source**

#### 1. Install Go as necessary dependancy

Go version 1.18 is required.

#### 2. Get the MatrixOne code

Depending on your needs, choose whether you want to keep your code up to date, or if you want to get the latest stable version of the code.

##### Option 1: Get the MatrixOne(Preview Version) code

The **main** branch is the default branch, the code on the main branch is always up-to-date but not stable enough.

```
$ git clone https://github.com/matrixorigin/matrixone.git
$ cd matrixone
```

##### Option 2: Get the MatrixOne(Stable Version) code

If you want to get the latest stable version code released by MatrixOne, please switch to the branch of version **0.5.0** first.

```
$ git clone https://github.com/matrixorigin/matrixone.git
$ git checkout 0.5.0
$ cd matrixone
```

#### 3. Run make

You can run `make debug`, `make clean`, or anything else our Makefile offers.

```
$ make config
$ make build
```

#### 4. Boot MatrixOne server

```
$ ./mo-server system_vars_config.toml
```

#### 5. Connect to MatrixOne Server

When you finish installing MatrixOne, you can refer to the section below to connect to the MatrixOne server.

See [Connect to MatrixOne server](connect-to-matrixone-server.md).

## **Method 2: Downloading binary packages**

For each release, you can download binary packages directly to run MatrixOne in the X86_64 Linux or Mac X86_64 environment.

#### 1. Download binary packages and decompress

Linux Environment

```bash
$ wget https://github.com/matrixorigin/matrixone/releases/download/v0.5.0/mo-server-v0.5.0-linux-amd64.zip
$ unzip mo-server-v0.5.0-linux-amd64.zip
```

MacOS Environment

```bash
$ wget https://github.com/matrixorigin/matrixone/releases/download/v0.5.0/mo-server-v0.5.0-darwin-x86_64.zip
$ unzip mo-server-v0.5.0-darwin-x86_64.zip
```

#### 2.Launch MatrixOne server

```
$./mo-server system_vars_config.toml
```

#### 3. Connect to MatrixOne Server

When you finish installing MatrixOne, you can refer to the section below to connect to the MatrixOne server.

See [Connect to MatrixOne server](connect-to-matrixone-server.md).

## **Method 3: Using docker**

#### 1. Install Docker

Please verify that Docker daemon is running in the background:

```
$ docker --version
```

#### 2. Create and run the container of MatrixOne

It will pull the image from Docker Hub if not exists. You can choose to pull the latest image or a stable version.

Latest Image

```
$ docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:latest
```

0.5.0 Version Image

```
$ docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:0.5.0
```

For the information on the user name and password, see the next step - Connect to MatrixOne Server.

#### 3. Connect to MatrixOne Server

When you finish installing MatrixOne, you can refer to the section below to connect to the MatrixOne server.

See [Connect to MatrixOne server](connect-to-matrixone-server.md).
