#!/bin/bash

sed -i "s/HOST/${HOST}/g" system_vars_config.toml
sed -i "s/NAME/${NAME}/g" system_vars_config.toml

/usr/local/bin/dumb-init ./mo-server ./system_vars_config.toml