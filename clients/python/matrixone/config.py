# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
MatrixOne Configuration Management

This module provides comprehensive configuration management for MatrixOne connections,
allowing users to override default connection parameters via environment variables,
configuration files, or direct parameter overrides.

Features:

- Default connection parameters with sensible defaults
- Environment variable overrides for all connection parameters
- Support for connection hooks via on_connect parameter
- Multiple configuration retrieval methods (tuple, dict, kwargs)
- Configuration debugging and validation

Usage Examples::

    from matrixone.config import get_connection_kwargs, print_config
    from matrixone.connection_hooks import ConnectionAction
    from matrixone import Client

    # Basic Configuration
    config = get_connection_kwargs()
    print_config()

    # Parameter Overrides
    config = get_connection_kwargs(
        host="production-server",
        port=6002,
        on_connect=[ConnectionAction.ENABLE_FULLTEXT]
    )

    # Using with Client
    client = Client()
    client.connect(**config)

Environment Variables:

- MATRIXONE_HOST: Database host (default: 127.0.0.1)
- MATRIXONE_PORT: Database port (default: 6001)
- MATRIXONE_USER: Database user (default: root)
- MATRIXONE_PASSWORD: Database password (default: 111)
- MATRIXONE_DATABASE: Database name (default: test)
- MATRIXONE_CHARSET: Character set (default: utf8mb4)
- MATRIXONE_CONNECT_TIMEOUT: Connection timeout in seconds (default: 30)
- MATRIXONE_AUTOCOMMIT: Enable autocommit (default: True)
- MATRIXONE_ON_CONNECT: Connection hooks (comma-separated list)

Connection Hooks:

The on_connect parameter supports:

- Predefined actions: enable_all, enable_ivf, enable_hnsw, enable_fulltext, enable_vector
- Custom callback functions
- Mixed actions and callbacks

Example on_connect values: "enable_all", "enable_ivf,enable_hnsw", or custom function
"""

import os
from typing import Any, Dict


class MatrixOneConfig:
    """
    MatrixOne connection configuration manager.

    This class provides centralized configuration management for MatrixOne connections,
    supporting environment variable overrides, parameter validation, and multiple
    output formats for different use cases.

    Attributes::

        DEFAULT_CONFIG (Dict[str, Any]): Default connection parameters
        ENV_MAPPING (Dict[str, str]): Mapping of parameters to environment variables

    Example::

        # Get configuration with environment overrides
        config = MatrixOneConfig.get_config()

        # Get connection parameters as tuple
        host, port, user, password, database = MatrixOneConfig.get_connection_params()

        # Get connection parameters as keyword arguments
        kwargs = MatrixOneConfig.get_connection_kwargs()

        # Print current configuration
        MatrixOneConfig.print_config()

    """

    # Default connection parameters
    DEFAULT_CONFIG = {
        "host": "127.0.0.1",
        "port": 6001,
        "user": "root",
        "password": "111",
        "database": "test",
        "charset": "utf8mb4",
        "connect_timeout": 30,
        "autocommit": True,
        "on_connect": None,
    }

    # Environment variable mapping
    ENV_MAPPING = {
        "host": "MATRIXONE_HOST",
        "port": "MATRIXONE_PORT",
        "user": "MATRIXONE_USER",
        "password": "MATRIXONE_PASSWORD",
        "database": "MATRIXONE_DATABASE",
        "charset": "MATRIXONE_CHARSET",
        "connect_timeout": "MATRIXONE_CONNECT_TIMEOUT",
        "autocommit": "MATRIXONE_AUTOCOMMIT",
        "on_connect": "MATRIXONE_ON_CONNECT",
    }

    @classmethod
    def get_config(cls, **overrides) -> Dict[str, Any]:
        """
        Get connection configuration with environment variable overrides.

        This method retrieves the complete configuration dictionary, applying
        environment variable overrides and direct parameter overrides in order:
        1. Default configuration
        2. Environment variable overrides
        3. Direct parameter overrides

        Args::

            **overrides: Additional parameter overrides. These take precedence
                        over environment variables and defaults.

        Returns::

            Dict[str, Any]: Complete configuration dictionary containing:
                - host (str): Database host
                - port (int): Database port
                - user (str): Database user
                - password (str): Database password
                - database (str): Database name
                - charset (str): Character set
                - connect_timeout (int): Connection timeout in seconds
                - autocommit (bool): Enable autocommit
                - on_connect (Any): Connection hooks configuration

        Example::

            # Get default configuration
            config = MatrixOneConfig.get_config()

            # Override specific parameters
            config = MatrixOneConfig.get_config(
                host="production-server",
                port=6002,
                on_connect=[ConnectionAction.ENABLE_ALL]
            )
        """
        config = cls.DEFAULT_CONFIG.copy()

        # Apply environment variable overrides
        for param, env_var in cls.ENV_MAPPING.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Convert types based on parameter
                if param == "port":
                    config[param] = int(env_value)
                elif param == "connect_timeout":
                    config[param] = int(env_value)
                elif param == "autocommit":
                    config[param] = env_value.lower() in ("true", "1", "yes", "on")
                else:
                    config[param] = env_value

        # Apply direct overrides
        config.update(overrides)

        return config

    @classmethod
    def get_connection_params(cls, **overrides) -> tuple:
        """
        Get connection parameters as tuple for easy unpacking.

        This method returns the core connection parameters as a tuple,
        which is useful for direct unpacking into function calls.

        Args::

            **overrides: Additional parameter overrides

        Returns::

            Tuple[str, int, str, str, str]: Tuple containing (host, port, user, password, database)

        Example::

            # Unpack parameters directly
            host, port, user, password, database = MatrixOneConfig.get_connection_params()

            # Use with legacy connection methods
            connection = create_connection(*MatrixOneConfig.get_connection_params())
        """
        config = cls.get_config(**overrides)
        return (
            config["host"],
            config["port"],
            config["user"],
            config["password"],
            config["database"],
        )

    @classmethod
    def get_connection_kwargs(cls, **overrides) -> Dict[str, Any]:
        """
        Get connection parameters as keyword arguments.

        This method returns all connection parameters as a dictionary,
        including extended parameters like charset, timeout, and connection hooks.
        This is the most comprehensive configuration method.

        Args::

            **overrides: Additional parameter overrides

        Returns::

            Dict[str, Any]: Complete connection parameters dictionary

        Example::

            from matrixone import Client
            from matrixone.connection_hooks import ConnectionAction

            # Get complete configuration and use directly with Client constructor
            client = Client(**MatrixOneConfig.get_connection_kwargs(
                on_connect=[ConnectionAction.ENABLE_ALL]
            ))

            # Or use with connect method
            config = MatrixOneConfig.get_connection_kwargs()
            client = Client()
            client.connect(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['password'],
                database=config['database'],
                charset=config['charset'],
                on_connect=config['on_connect']
            )
        """
        config = cls.get_config(**overrides)
        return {
            "host": config["host"],
            "port": config["port"],
            "user": config["user"],
            "password": config["password"],
            "database": config["database"],
            "charset": config["charset"],
            "connection_timeout": config["connect_timeout"],
            "auto_commit": config["autocommit"],
            "on_connect": config["on_connect"],
        }

    @classmethod
    def print_config(cls, **overrides):
        """
        Print current configuration for debugging and validation.

        This method displays the current configuration in a formatted way,
        showing all parameters with their current values. Sensitive information
        like passwords are masked for security.

        Args::

            **overrides: Additional parameter overrides to apply before printing

        Example::

            # Print default configuration
            MatrixOneConfig.print_config()

            # Print configuration with overrides
            MatrixOneConfig.print_config(host="production-server", port=6002)

        Note: Prints formatted configuration to stdout.
        """
        config = cls.get_config(**overrides)
        print("MatrixOne Connection Configuration:")
        print("=" * 40)
        for key, value in config.items():
            if key == "password":
                print(f"  {key}: {'*' * len(str(value))}")
            elif key == "on_connect":
                if value is None:
                    print(f"  {key}: None")
                else:
                    print(f"  {key}: {type(value).__name__}")
            else:
                print(f"  {key}: {value}")
        print("=" * 40)


# Convenience functions
def get_config(**overrides) -> Dict[str, Any]:
    """
    Get MatrixOne connection configuration.

    Convenience function for MatrixOneConfig.get_config().

    Args::

        **overrides: Additional parameter overrides

    Returns::

        Dict[str, Any]: Complete configuration dictionary

    Example::

        from matrixone.config import get_config

        config = get_config(host="production-server")

    """
    return MatrixOneConfig.get_config(**overrides)


def get_connection_params(**overrides) -> tuple:
    """
    Get MatrixOne connection parameters as tuple.

    Convenience function for MatrixOneConfig.get_connection_params().

    Args::

        **overrides: Additional parameter overrides

    Returns::

        Tuple[str, int, str, str, str]: Connection parameters tuple

    Example::

        from matrixone.config import get_connection_params

        host, port, user, password, database = get_connection_params()

    """
    return MatrixOneConfig.get_connection_params(**overrides)


def get_connection_kwargs(**overrides) -> Dict[str, Any]:
    """
    Get MatrixOne connection parameters as keyword arguments.

    Convenience function for MatrixOneConfig.get_connection_kwargs().
    This is the most commonly used function for getting complete configuration.

    Args::

        **overrides: Additional parameter overrides

    Returns::

        Dict[str, Any]: Complete connection parameters dictionary

    Example::

        from matrixone.config import get_connection_kwargs
        from matrixone.connection_hooks import ConnectionAction

        config = get_connection_kwargs(on_connect=[ConnectionAction.ENABLE_ALL])

    """
    return MatrixOneConfig.get_connection_kwargs(**overrides)


def get_client_kwargs(**overrides) -> Dict[str, Any]:
    """
    Get connection parameters suitable for Client constructor.

    This method returns connection parameters that can be directly passed
    to the Client() constructor, excluding parameters that are only used
    by the connect() method.

    Args::

        **overrides: Additional parameter overrides

    Returns::

        Dict[str, Any]: Client constructor parameters dictionary

    Example::

        from matrixone import Client
        from matrixone.config import get_client_kwargs

        # Create client with all configuration in one line
        client = Client(**get_client_kwargs())

        # Or with overrides
        client = Client(**get_client_kwargs(host="production-server"))
    """
    config = MatrixOneConfig.get_config(**overrides)
    return {
        "host": config["host"],
        "port": config["port"],
        "user": config["user"],
        "password": config["password"],
        "database": config["database"],
        "charset": config["charset"],
        "connection_timeout": config["connect_timeout"],
        "auto_commit": config["autocommit"],
    }


def print_config(**overrides):
    """
    Print current MatrixOne configuration.

    Convenience function for MatrixOneConfig.print_config().

    Args::

        **overrides: Additional parameter overrides to apply before printing

    Example::

        from matrixone.config import print_config

        print_config()  # Print current configuration

    """
    MatrixOneConfig.print_config(**overrides)
