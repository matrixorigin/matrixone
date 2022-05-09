# **Install standalone MatrixOne**

MatrixOne supports Linux and MacOS. You can install a standalone MatrixOne version either by [building from source](#building-from-source) or [using docker](#using-docker).

Recommended hardware specification: x86 CPU with 4 cores and 32GB memory, with CentOS 7+ OS.

## **Install From AUR**

ArchLinux User can install MatrixOne from [AUR](https://aur.archlinux.org/packages/matrixone).

```
$ git clone https://aur.archlinux.org/matrixone.git
$ cd matrixone
$ makepkg -rsi
```

## **Building from source**

#### 1. Install Go as necessary


Go version 1.18 is required.
  

#### 2. Get the MatrixOne code

```
$ git clone https://github.com/matrixorigin/matrixone.git
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

## **Downloading binary packages**

Starting with 0.3.0, you can download binary packages directly to run MatrixOne in the X86_64 Linux or Mac X86_64 environment.

#### 1. Download binary packages and decompress

Linux Environment
```bash
$ wget https://github.com/matrixorigin/matrixone/releases/download/v0.4.0/mo-server-v0.4.0-linux-amd64.zip
$ unzip mo-server-v0.4.0-linux-amd64.zip
```

MacOS Environment
```bash
$ https://github.com/matrixorigin/matrixone/releases/download/v0.4.0/mo-server-v0.4.0-darwin-x86_64.zip
$ unzip mo-server-v0.4.0-darwin-x86_64.zip
```

#### 2.Launch MatrixOne server

```
$./mo-server system_vars_config.toml
```


## **Using docker**

#### 1. Install Docker
Please verify that Docker daemon is running in the background:

```
$ docker --version
```
#### 2. Create and run the container for the latest release of MatrixOne
It will pull the image from Docker Hub if not exists.

```
$ docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:latest
```
