Configuration Management Guide
==============================

This guide explains how to use MatrixOne's comprehensive configuration management system
to manage connection parameters, environment variables, and connection hooks.

Overview
--------

MatrixOne provides a flexible configuration management system that allows you to:

- Set default connection parameters
- Override parameters via environment variables
- Configure connection hooks for automatic feature enablement
- Use different configuration formats for different use cases
- Debug and validate configurations

Basic Usage
-----------

Import the configuration functions:

.. code-block:: python

    from matrixone.config import get_connection_kwargs, print_config

Get default configuration:

.. code-block:: python

    # Get complete configuration
    config = get_connection_kwargs()
    
    # Print current configuration
    print_config()

Environment Variables
--------------------

You can override any configuration parameter using environment variables:

.. code-block:: bash

    export MATRIXONE_HOST=localhost
    export MATRIXONE_PORT=6001
    export MATRIXONE_USER=admin
    export MATRIXONE_PASSWORD=mypassword
    export MATRIXONE_DATABASE=mydb
    export MATRIXONE_ON_CONNECT=enable_all,enable_vector

Supported Environment Variables:

- ``MATRIXONE_HOST``: Database host (default: 127.0.0.1)
- ``MATRIXONE_PORT``: Database port (default: 6001)
- ``MATRIXONE_USER``: Database user (default: root)
- ``MATRIXONE_PASSWORD``: Database password (default: 111)
- ``MATRIXONE_DATABASE``: Database name (default: test)
- ``MATRIXONE_CHARSET``: Character set (default: utf8mb4)
- ``MATRIXONE_CONNECT_TIMEOUT``: Connection timeout in seconds (default: 30)
- ``MATRIXONE_AUTOCOMMIT``: Enable autocommit (default: True)
- ``MATRIXONE_ON_CONNECT``: Connection hooks (comma-separated list)

Parameter Overrides
-------------------

Override specific parameters in code:

.. code-block:: python

    from matrixone.config import get_connection_kwargs
    from matrixone.connection_hooks import ConnectionAction
    
    # Override specific parameters
    config = get_connection_kwargs(
        host="production-server",
        port=6002,
        on_connect=[ConnectionAction.ENABLE_FULLTEXT]
    )

Configuration Methods
---------------------

MatrixOne provides three methods for getting configuration:

1. **get_config()** - Returns complete configuration dictionary
2. **get_connection_params()** - Returns core parameters as tuple
3. **get_connection_kwargs()** - Returns all parameters as dictionary

.. code-block:: python

    from matrixone.config import get_config, get_connection_params, get_connection_kwargs
    
    # Get complete configuration
    config = get_config()
    
    # Get core parameters as tuple
    host, port, user, password, database = get_connection_params()
    
    # Get all parameters as dictionary (most commonly used)
    kwargs = get_connection_kwargs()

Using with Client
-----------------

Connect using configuration:

.. code-block:: python

    from matrixone import Client
    from matrixone.config import get_connection_kwargs
    
    # Get configuration
    config = get_connection_kwargs()
    
    # Filter to only supported parameters for Client.connect()
    client = Client()
    client.connect(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        on_connect=config['on_connect']
    )

Connection Hooks Configuration
-----------------------------

Configure connection hooks for automatic feature enablement:

.. code-block:: python

    from matrixone.config import get_connection_kwargs
    from matrixone.connection_hooks import ConnectionAction, create_connection_hook
    
    # Using predefined actions
    config = get_connection_kwargs(
        on_connect=[ConnectionAction.ENABLE_ALL]
    )
    
    # Using string names
    config = get_connection_kwargs(
        on_connect=["enable_fulltext", "enable_vector"]
    )
    
    # Using custom callback
    def my_callback(client):
        print(f"Connected to {client._connection_params['host']}")
    
    config = get_connection_kwargs(on_connect=my_callback)
    
    # Using mixed actions and callbacks
    config = get_connection_kwargs(
        on_connect=create_connection_hook(
            actions=[ConnectionAction.ENABLE_FULLTEXT],
            custom_hook=my_callback
        )
    )

Environment Variable for Connection Hooks
-----------------------------------------

Set connection hooks via environment variable:

.. code-block:: bash

    # Enable all features
    export MATRIXONE_ON_CONNECT=enable_all
    
    # Enable specific features
    export MATRIXONE_ON_CONNECT=enable_ivf,enable_hnsw,enable_fulltext
    
    # Multiple actions
    export MATRIXONE_ON_CONNECT=enable_vector,enable_fulltext

Configuration Debugging
-----------------------

Debug and validate configurations:

.. code-block:: python

    from matrixone.config import print_config
    
    # Print current configuration
    print_config()
    
    # Print configuration with overrides
    print_config(host="test-server", port=6003)

Output example:

.. code-block:: text

    MatrixOne Connection Configuration:
    ========================================
      host: 127.0.0.1
      port: 6001
      user: root
      password: ***
      database: test
      charset: utf8mb4
      connect_timeout: 30
      autocommit: True
      on_connect: None
    ========================================

Advanced Usage
--------------

Class-based Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~

Use the MatrixOneConfig class directly:

.. code-block:: python

    from matrixone.config import MatrixOneConfig
    
    # Get configuration with environment overrides
    config = MatrixOneConfig.get_config()
    
    # Get connection parameters as tuple
    host, port, user, password, database = MatrixOneConfig.get_connection_params()
    
    # Get connection parameters as keyword arguments
    kwargs = MatrixOneConfig.get_connection_kwargs()
    
    # Print current configuration
    MatrixOneConfig.print_config()

Configuration Precedence
~~~~~~~~~~~~~~~~~~~~~~~~

Configuration parameters are applied in the following order:

1. Default configuration
2. Environment variable overrides
3. Direct parameter overrides

Later values override earlier ones.

Best Practices
--------------

1. **Use Environment Variables for Deployment**
   Set environment variables in your deployment environment rather than hardcoding values.

2. **Use get_connection_kwargs() for Most Cases**
   This method provides the most comprehensive configuration.

3. **Filter Parameters for Client.connect()**
   Only pass supported parameters to avoid errors.

4. **Use Connection Hooks for Feature Enablement**
   Configure connection hooks to automatically enable features like vector operations and fulltext search.

5. **Debug with print_config()**
   Use print_config() to verify your configuration before connecting.

Example Applications
--------------------

Production Setup
~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client
    from matrixone.config import get_connection_kwargs
    
    # Production configuration via environment variables
    config = get_connection_kwargs()
    
    # Filter to supported parameters
    client = Client()
    client.connect(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        on_connect=config['on_connect']
    )

Development Setup
~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client
    from matrixone.config import get_connection_kwargs
    from matrixone.connection_hooks import ConnectionAction
    
    # Development configuration with overrides
    config = get_connection_kwargs(
        host="localhost",
        database="dev_db",
        on_connect=[ConnectionAction.ENABLE_ALL]
    )
    
    client = Client()
    client.connect(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        on_connect=config['on_connect']
    )

Testing Setup
~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client
    from matrixone.config import get_connection_kwargs
    
    # Test configuration
    config = get_connection_kwargs(
        database="test_db",
        on_connect=None  # No hooks for testing
    )
    
    client = Client()
    client.connect(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )

API Reference
-------------

.. automodule:: matrixone.config
   :members:
   :undoc-members:
   :show-inheritance:

Related Topics
--------------

- :doc:`connection_hooks_guide` - Learn about connection hooks
- :doc:`quickstart` - Get started with MatrixOne
- :doc:`best_practices` - Best practices for MatrixOne usage
