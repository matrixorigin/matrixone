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

# The Configuration Specification

In this system, the configuration logic starts from a definition file.
In the definition file, many system parameters can be well-defined.

## Define the parameter

The definition of the parameter has the form following:

```
[[parameter]]
name = "xxxx"
scope = ["xxxx"]
access = ["xxxx"]
type = "xxxx"
domain-type = "xxxx"
values = ["xxxx"]
comment = "xxxx"
update-mode = "xxxx"
```

1. `name` denotes the unique name of the parameter in the definition file. It must be a **valid Go identifier**. And, the first letter of it must be the **low case Ascii char or '_'**. The rest letters can be the Ascii char, digit or '_'.

That is :
```
identifier = low-case-letter { letter | digit } *
low-case-letter = "a" ... "z" | "_"
letter     = "a" ... "z" | "A" ... "Z" | "_"
digit      = "0" ... "9"
```

2. `scope` denotes `session` parameter or `global` parameter. The `session` parameter only affects the activity in only one session. The `global` parameter effects activity in all sessions. The parameter belongs to the **only one** of the two. 

3. `access` denotes where you can change the value of the parameter. `cmd` means the update can happen in the command line option. `file` means the update can happen in the configuration file. `env` means the update can happen in the environment variable. Now, only `file` really works.

4. `type` denotes the data type of the parameter includes `string`,`int64`,`float64`,`bool`. 

5. `domain-type` denotes the parameter can be the set or the range. `set` means the parameter must be the one element in the set. It is a enum type. `range` means the parameter can be any value in a range.

6. `values` denotes the initial value for the parameter. 
    When the `domain-type` is `set`, `values` contains all alternatives of the parameter.
    When the `domain-type` is `range`, `values` only contains three values. the first one is the initial value of the parameter.
   The second one and the third one forms a range. So,in math, the non-equation `the second one ` <= `the first one` <= `the third one` holds.
   
7. `comment` denotes the comment for clarification.

8. `update-mode` denotes the parameter can be changed or not. `dynamic` means it can be changed. `fix` means it can not be changed. it only holds the initial value. `hotload` means it can be changed during the system running.
