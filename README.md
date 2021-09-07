# What is MatrixOne?

# Features

# Architecture

# Quick Start
## Building

**Get the MatrxiOne code:**

```
git clone https://github.com/matrixorigin/matrixone.git matrixone
cd matrixone
```

**Run make:**

Run `make debug`, `make clean`, or anything else our Makefile offers. You can just run the following command to build quickly.

```
make config
make build
```

## Starting

**Prerequisites**

- MySQL client

  MatrxiOne supports the MySQL wire protocol, so you can use MySQL client drivers to connect from various languages.

**Boot MatrxiOne server:**

```
./mo-server system_vars_config.toml
```

**Connect MatrxiOne server:**

```
mysql -h IP -P PORT -uUsername -p
```

**For example:**

Test Account:

- user: dump
- password: 111

```
mysql -h 127.0.0.1 -P 6001 -udump -p
```

# Contributing
See [Contributing Guide](CONTRIBUTING.md) for details on contribution workflows.

## Roadmap

# License
MatrixOne is licensed under the [Apache License, Version 2.0](LICENSE)
