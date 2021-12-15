# **Install MatrixOne**

MatrixOne supports Linux and MacOS. You can install MatrixOne either by [building from source](#building-from-source) or [using docker](#using-docker).
 
## **Building from source**

#### 1. Install Go as necessary

Go version 1.17+ is required.
  
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
