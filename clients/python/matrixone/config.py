"""
MatrixOne Configuration Management

This module provides configuration management for MatrixOne connections,
allowing users to override default connection parameters via environment variables.
"""

import os
from typing import Any, Dict


class MatrixOneConfig:
    """MatrixOne connection configuration manager"""

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
    }

    @classmethod
    def get_config(cls, **overrides) -> Dict[str, Any]:
        """
        Get connection configuration with environment variable overrides.

        Args:
            **overrides: Additional parameter overrides

        Returns:
            Dict containing connection parameters
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

        Args:
            **overrides: Additional parameter overrides

        Returns:
            Tuple of (host, port, user, password, database)
        """
        config = cls.get_config(**overrides)
        return (config["host"], config["port"], config["user"], config["password"], config["database"])

    @classmethod
    def get_connection_kwargs(cls, **overrides) -> Dict[str, Any]:
        """
        Get connection parameters as keyword arguments.

        Args:
            **overrides: Additional parameter overrides

        Returns:
            Dict of connection parameters
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
        }

    @classmethod
    def print_config(cls, **overrides):
        """Print current configuration for debugging"""
        config = cls.get_config(**overrides)
        print("MatrixOne Connection Configuration:")
        print("=" * 40)
        for key, value in config.items():
            if key == "password":
                print(f"  {key}: {'*' * len(str(value))}")
            else:
                print(f"  {key}: {value}")
        print("=" * 40)


# Convenience functions
def get_config(**overrides) -> Dict[str, Any]:
    """Get MatrixOne connection configuration"""
    return MatrixOneConfig.get_config(**overrides)


def get_connection_params(**overrides) -> tuple:
    """Get MatrixOne connection parameters as tuple"""
    return MatrixOneConfig.get_connection_params(**overrides)


def get_connection_kwargs(**overrides) -> Dict[str, Any]:
    """Get MatrixOne connection parameters as keyword arguments"""
    return MatrixOneConfig.get_connection_kwargs(**overrides)


def print_config(**overrides):
    """Print current MatrixOne configuration"""
    MatrixOneConfig.print_config(**overrides)
