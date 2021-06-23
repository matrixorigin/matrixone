MatrixBase is an open-source distributed OLAP database.

# Build & Run & Shutdown
## Build
**1. change the root directory of the matrixone**
```
% cd matrixone
% pwd
/pathto/matrixone
```

**2. generate the configuration file**

```
make config
```
It does things following:

Generate the configuration generator bin `gen_config`.

`gen_config` reads the configuration definition file `cmd/generate-config/system_vars_def.toml` 

and generates the configuration file `cmd/generate-config/system_vars_config.toml `.

Then, it moves the configuration file `cmd/generate-config/system_vars_config.toml ` to the 
matrixone root directory.

It moves `cmd/generate-config/system_vars.go ` to `pkg/config`.

It moves `cmd/generate-config/system_vars_test.go ` to `pkg/config`.

**3. generate the `main` bin**

```
make build
```

It makes an execution bin - `main` for the `matrixone`.

## Run
**1. Boot the server**

The `ip` and `port` in the `system_vars_config.toml` can be changed.

Change to the root directory of the `matrixone`.

```
% ./main system_vars_config.toml
```

**2. Connect the server**
We need to install the mysql client first.

Test Account:

user : `dump`

password : `111`

```
% mysql -h IP -P PORT -udump -p
```


For example:
```
% mysql -h 127.0.0.1 -P 6001 -udump -p
```

## Shutdown

Use shell command to send a close signal to the server.

```
Ctr+C or Ctr+\.
```