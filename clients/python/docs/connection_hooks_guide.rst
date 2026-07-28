Connection Hooks Guide
======================

The MatrixOne Python Client now supports Connection Hooks functionality, allowing you to automatically execute predefined actions every time a new connection is established.

Overview
--------

Connection hooks allow you to:

- Automatically enable specific features (such as IVF, HNSW, fulltext search) on each new connection
- Execute custom callback functions
- Avoid repetitive manual configuration

Predefined Actions
------------------

The following predefined actions can be automatically executed on connection:

- ``ConnectionAction.ENABLE_IVF``: Enable IVF vector indexing
- ``ConnectionAction.ENABLE_HNSW``: Enable HNSW vector indexing  
- ``ConnectionAction.ENABLE_FULLTEXT``: Enable fulltext search
- ``ConnectionAction.ENABLE_VECTOR``: Enable all vector operations (IVF + HNSW)
- ``ConnectionAction.ENABLE_ALL``: Enable all features

Configuration Management Integration
------------------------------------

Connection hooks can be configured using the configuration management system.
See :doc:`configuration_guide` for detailed information on using environment variables
and configuration parameters.

.. code-block:: python

    from matrixone.config import get_connection_kwargs
    from matrixone.connection_hooks import ConnectionAction
    
    # Configure connection hooks via configuration management
    config = get_connection_kwargs(
        on_connect=[ConnectionAction.ENABLE_ALL]
    )
    
    # Use with client
    client = Client()
    client.connect(**config)

Environment Variable Support
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also set connection hooks via environment variables:

.. code-block:: bash

    export MATRIXONE_ON_CONNECT=enable_all,enable_vector

Basic Usage
-----------

Synchronous Client
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client
    from matrixone.connection_hooks import ConnectionAction

    # Create client
    client = Client()

    # Enable all features on connection
    client.connect(
        host="127.0.0.1",
        port=6001,
        user="root",
        password="111",
        database="test",
        on_connect=[ConnectionAction.ENABLE_ALL]
    )

    # Now you can directly use vector search and fulltext search features
    # No need to manually call enable_ivf(), enable_hnsw(), enable_fulltext()

Asynchronous Client
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import asyncio
    from matrixone import AsyncClient
    from matrixone.connection_hooks import ConnectionAction

    async def main():
        # Create async client
        client = AsyncClient()

        # Enable vector operations on connection
        await client.connect(
            host="127.0.0.1",
            port=6001,
            user="root",
            password="111",
            database="test",
            on_connect=[ConnectionAction.ENABLE_VECTOR]
        )

        # Now you can directly use vector search features

    asyncio.run(main())

Custom Callbacks
----------------

You can also provide custom callback functions:

.. code-block:: python

    from matrixone import Client

    def my_callback(client):
        print(f"Connected to {client._connection_params['host']}")
        # Execute custom setup

    client = Client()
    client.connect(
        host="127.0.0.1",
        port=6001,
        user="root",
        password="111",
        database="test",
        on_connect=my_callback
    )

Mixed Usage
-----------

You can use both predefined actions and custom callbacks:

.. code-block:: python

    from matrixone import Client
    from matrixone.connection_hooks import ConnectionAction, create_connection_hook

    def setup_callback(client):
        print("Setting up client for analytics workload")

    # Create mixed hook
    hook = create_connection_hook(
        actions=[ConnectionAction.ENABLE_FULLTEXT, ConnectionAction.ENABLE_IVF],
        custom_hook=setup_callback
    )

    client = Client()
    client.connect(
        host="127.0.0.1",
        port=6001,
        user="root",
        password="111",
        database="test",
        on_connect=hook
    )

String Actions
--------------

You can also use string-formatted action names:

.. code-block:: python

    from matrixone import Client

    client = Client()
    client.connect(
        host="127.0.0.1",
        port=6001,
        user="root",
        password="111",
        database="test",
        on_connect=["enable_fulltext", "enable_ivf"]
    )

Real-world Use Cases
--------------------

Vector Search Application
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client
    from matrixone.connection_hooks import ConnectionAction

    # Client optimized for vector search
    def setup_vector_client():
        client = Client()
        client.connect(
            host="127.0.0.1",
            port=6001,
            user="root",
            password="111",
            database="vector_search_db",
            on_connect=[ConnectionAction.ENABLE_VECTOR]
        )
        return client

    # Use client for vector search
    client = setup_vector_client()
    # Now you can directly use vector indexing features

Fulltext Search Application
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client
    from matrixone.connection_hooks import ConnectionAction

    # Client optimized for fulltext search
    def setup_search_client():
        client = Client()
        client.connect(
            host="127.0.0.1",
            port=6001,
            user="root",
            password="111",
            database="search_db",
            on_connect=[ConnectionAction.ENABLE_FULLTEXT]
        )
        return client

    # Use client for fulltext search
    client = setup_search_client()
    # Now you can directly use fulltext search features

Analytics Application
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client
    from matrixone.connection_hooks import ConnectionAction, create_connection_hook

    def analytics_setup(client):
        print("Configuring client for analytics workload")
        # Can set session variables, create temporary tables, etc.

    # Client optimized for analytics
    def setup_analytics_client():
        client = Client()
        client.connect(
            host="127.0.0.1",
            port=6001,
            user="root",
            password="111",
            database="analytics_db",
            on_connect=create_connection_hook(
                actions=[ConnectionAction.ENABLE_ALL],
                custom_hook=analytics_setup
            )
        )
        return client

Technical Details
-----------------

How connection hooks work:

1. **Event Listening**: Uses SQLAlchemy's event system to listen for connection events
2. **Connection Tracking**: Tracks which connections have already executed the hook to avoid duplicate execution
3. **Direct Execution**: Executes SQL directly on existing connections to avoid creating new connections
4. **Error Handling**: Hook execution failures do not affect connection establishment

Important Notes
---------------

- Connection hooks execute every time a new connection is established
- Hook execution failures do not prevent connection establishment
- Predefined actions set corresponding session variables
- Custom callback functions execute after predefined actions

API Reference
-------------

ConnectionAction
~~~~~~~~~~~~~~~~

.. code-block:: python

    class ConnectionAction(Enum):
        ENABLE_IVF = "enable_ivf"
        ENABLE_HNSW = "enable_hnsw"
        ENABLE_FULLTEXT = "enable_fulltext"
        ENABLE_VECTOR = "enable_vector"
        ENABLE_ALL = "enable_all"

ConnectionHook
~~~~~~~~~~~~~~

.. code-block:: python

    class ConnectionHook:
        def __init__(self, actions=None, custom_hook=None):
            """
            Args:
                actions: List of ConnectionAction or string action names
                custom_hook: Custom callback function
            """

create_connection_hook
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def create_connection_hook(actions=None, custom_hook=None):
        """
        Create a connection hook with predefined actions and/or custom callback
        
        Args:
            actions: List of ConnectionAction or string action names
            custom_hook: Custom callback function
            
        Returns:
            ConnectionHook: Configured connection hook
        """

Related Topics
--------------

- :doc:`configuration_guide` - Learn about configuration management
- :doc:`quickstart` - Get started with MatrixOne
- :doc:`vector_guide` - Vector operations and indexing
- :doc:`fulltext_guide` - Fulltext search capabilities
- :doc:`best_practices` - Best practices for MatrixOne usage
