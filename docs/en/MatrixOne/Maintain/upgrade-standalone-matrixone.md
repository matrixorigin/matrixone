# **Update Standalone MatrixOne**

You can update the standalone MatrixOne version by 3 methods:

- <p><a href="#code_source_update">Method 1: Updating from source</a></p>
- <p><a href="#binary_packages_update">Method 2: Using binary package to update</a></p>
- <p><a href="#use_docker_update">Method 3: Using Docker to update</a></p>

- If MatrixOne is installed by using source code or by using a binary package, you can use source or binary to update MatrixOne's version. The two methods are the same.

- If MatrixOne is installed with Docker, you can use Docker to update MatrixOne's version.

!!! note
    This document is only applicable to the v0.5.0 version updating. Not for v0.4.0/v0.3.0/v0.2.0/v0.1.0 update to v0.5.0 version.

## Before You Start

- MatrixOne v0.5.0 has been installed.

## <h2><a name="code_source_update">Method 1: Updating from source</a></h2>

**Scenario Example** :

- The local Matrixone directory needs to be upgraded from v0.5.0 to v0.5.1.

- The *store* directory needs to be backed up.

### 1. Backup *store*

```
cd matrixone
ls
cp -r store ${path_name}
```

`${path_name}` indicates the backup path of *store*.

!!! note
    If you've never started MatrixOne, you don't need to do this step.

### 2. Get the MatrixOne v0.5.1 code

```
git pull https://github.com/matrixorigin/matrixone.git
git checkout 0.5.1
```

### 3. Copy the backed up *store* directory back to the MatrixOne directory

```
cp -r ${path_name} store
```

### 4. Run make

You can run make debug, make clean, or anything else our Makefile offers.

```
make config
make build
```

### 5. Boot MatrixOne server

```
./mo-server system_vars_config.toml
```

### 6. Connect to MatrixOne Server

When you finish installing MatrixOne, you can refer to the section below to connect to the MatrixOne server.

See [Connect to MatrixOne server](../Get-Started/connect-to-matrixone-server.md).

## <h2><a name="binary_packages_update">Method 2: Using binary package to update</a></h2>

**Scenario Example** :

- The local Matrixone directory needs to be upgraded from v0.5.0 to v0.5.1.

- The *store* directory needs to be backed up.

### 1. Backup *store*

```
cd matrixone
ls
cp -r store ${path_name}
```

`${path_name}` indicates the backup path of *store*.

!!! note
    If you've never started MatrixOne, you don't need to do this step.

### 2. Download binary packages and decompress

- **Linux Environment**

```bash
wget https://github.com/matrixorigin/matrixone/releases/download/v0.5.0/mo-server-v0.5.1-linux-amd64.zip
unzip mo-server-v0.5.1-linux-amd64.zip
```

- **MacOS Environment**

```bash
wget https://github.com/matrixorigin/matrixone/releases/download/v0.5.0/mo-server-v0.5.1-darwin-x86_64.zip
unzip mo-server-v0.5.1-darwin-x86_64.zip
```

### 3. Cover `mo-server` (Optional)

- If you are download the v0.5.1 binary package to a new path (for example, the new path is *0.5.1_path*), then you only need to use the `mo-server` in the v0.5.1 binary package to overwrite the `mo-server` in the v0.5.0 binary package.

  ```
  cp -rf 0.5.1_path/matrixone/mo-server matrixone/mo-server
  ```

- If you download the V0.5.1 binary package to the current Matxione path, then the `mo-server` will be automatically covered, and you don't need to do anything else.

### 4. Copy the backed up *store* directory back to the MatrixOne directory

```
cp -r ${path_name} store
```

### 5. Launch MatrixOne server

```
./mo-server system_vars_config.toml
```

### 6. Connect to MatrixOne Server

When you finish installing MatrixOne, you can refer to the section below to connect to the MatrixOne server.

See [Connect to MatrixOne server](../Get-Started/connect-to-matrixone-server.md).

## <h2><a name="use_docker_update">Method 3: Using Docker to update</a></h2>

If you do not need to keep historical data, you can use the following command to download an image of MatrixOne V0.5.1 from Docker Hub.

```
docker ps
docker stop matrixone
docker rm matrixone
docker pull matrixorigin/matrixone:0.5.1
docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:0.5.1
```

If you need to keep historical data, please refer to the following scenario example:

**Scenario Example** :

- The v0.5.0 version of MatrixOne is runing by Docker.

- The container path is not attached to the local disk, and historical data needs to be preserved before the upgrade.

### 1.Back up the *store* directory in the container to the host

① Check the containers of the currently running Matrixone, and check whether the Matrixone is running. If the Matrixone is running, stop the Matrixone.

```
docker ps
docker stop matrixone
```

② Copy the *store* directory from the current Matrixone container to the local backup path.

```
docker cp matrixone:/store ${path_name}/
```

`${path_name}` indicates the backup path of *store*.

### 2. Remove v0.5.0 version of MatrixOne

```
docker rm matrixone
```

### 3. Download the MatrixOne v0.5.1 image

Use the following command to download the MatrixOne v0.5.1 image from Docker Hub.

```
docker pull matrixorigin/matrixone:0.5.1
```

### 4. Run the container and mount the */store* directory

Run the container and mount the '/store' directory to the location of the backed-up store folder on your local disk:

```
docker run -d -p 6001:6001 -v ~/tmp/store:/store:rw --name matrixone matrixorigin/matrixone:0.5.1
```

To customize the configuration file, you can mount the custom configuration file stored on the local disk.

```
docker run -d -p 6001:6001 -v ${path_name}/system_vars_config.toml:/system_vars_config.toml:ro -v ${path_name}/store:/store:rw --name matrixone matrixorigin/matrixone:0.5.1
```

For the information on the user name and password, see the next step - Connect to MatrixOne Server.

### 5. Connect to MatrixOne Server

When you finish installing MatrixOne, you can refer to the section below to connect to the MatrixOne server.

See [Connect to MatrixOne server](../Get-Started/connect-to-matrixone-server.md).

## Reference

For more information on deployment，see[Deployment FAQs](../FAQs/deployment-faqs.md).
