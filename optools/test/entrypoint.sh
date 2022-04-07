#!/usr/bin/env bash

sed -i "s/HOST/${HOST}/g" system_vars_config.toml
sed -i "s/NAME/${NAME}/g" system_vars_config.toml

./mo-server ./system_vars_config.toml
