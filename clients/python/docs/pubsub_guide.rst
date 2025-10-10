Pub/Sub Operations Guide
========================

This guide covers publish/subscribe operations in the MatrixOne Python SDK, enabling real-time messaging and event-driven architectures.

Overview
--------

MatrixOne's Pub/Sub system provides:

* **Real-time Messaging**: Publish and subscribe to topics for real-time communication
* **Event-driven Architecture**: Build reactive applications with event-based patterns
* **Topic Management**: Create, list, and manage topics
* **Message Filtering**: Subscribe to specific message patterns
* **Async Support**: Full async/await support for high-performance applications
* **Durability**: Reliable message delivery with persistence options

Getting Started
---------------

Basic Setup
~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   # Connect to MatrixOne
   connection_params = get_connection_params()
   client = Client(*connection_params)
   client.connect(*connection_params)

   # Get Pub/Sub manager
   pubsub = client.pubsub

Topic Management
----------------

Creating Topics
~~~~~~~~~~~~~~~

.. code-block:: python

   # Create a topic
   topic_name = "user_events"
   topic = pubsub.create_topic(topic_name)
   print(f"Created topic: {topic.name}")

   # Create topic with configuration
   topic = pubsub.create_topic(
       name="system_logs",
       description="System logging events",
       retention_hours=24
   )

Listing Topics
~~~~~~~~~~~~~~

.. code-block:: python

   # List all topics
   topics = pubsub.list_topics()
   for topic in topics:
       print(f"Topic: {topic.name}, Subscribers: {topic.subscriber_count}")

   # Get specific topic
   topic = pubsub.get_topic("user_events")
   if topic:
       print(f"Topic exists: {topic.name}")

Deleting Topics
~~~~~~~~~~~~~~~

.. code-block:: python

   # Delete a topic
   pubsub.delete_topic("user_events")
   print("Topic deleted")

Publishing Messages
-------------------

Basic Publishing
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Publish a simple message
   message = "User logged in: john_doe"
   pubsub.publish("user_events", message)
   print("Message published")

   # Publish with metadata
   message_data = {
       "event": "user_login",
       "user_id": "john_doe",
       "timestamp": "2024-01-15T10:30:00Z",
       "ip_address": "192.168.1.100"
   }
   pubsub.publish("user_events", message_data)
   print("Structured message published")

Batch Publishing
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Publish multiple messages
   messages = [
       {"event": "user_login", "user_id": "alice"},
       {"event": "user_logout", "user_id": "bob"},
       {"event": "user_register", "user_id": "charlie"}
   ]
   
   for message in messages:
       pubsub.publish("user_events", message)
   
   print(f"Published {len(messages)} messages")

Subscribing to Messages
-----------------------

Basic Subscription
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Subscribe to a topic
   def message_handler(message):
       print(f"Received: {message.data}")
       print(f"From topic: {message.topic}")
       print(f"Timestamp: {message.timestamp}")

   subscription = pubsub.subscribe("user_events", message_handler)
   print("Subscribed to user_events")

   # Keep subscription active
   import time
   time.sleep(10)  # Listen for 10 seconds
   
   # Unsubscribe
   subscription.unsubscribe()

Filtered Subscription
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Subscribe with message filtering
   def login_handler(message):
       if message.data.get("event") == "user_login":
           print(f"User login: {message.data.get('user_id')}")

   subscription = pubsub.subscribe(
       "user_events", 
       login_handler,
       filter={"event": "user_login"}
   )

   # Subscribe to multiple event types
   def user_activity_handler(message):
       event = message.data.get("event")
       user_id = message.data.get("user_id")
       print(f"User activity: {user_id} - {event}")

   subscription = pubsub.subscribe(
       "user_events",
       user_activity_handler,
       filter={
           "event": {"$in": ["user_login", "user_logout", "user_register"]}
       }
   )

Async Operations
----------------

Async Publishing
~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient

   async def async_publishing():
       # Connect asynchronously
       connection_params = get_connection_params()
       async_client = AsyncClient(*connection_params)
       await async_client.connect(*connection_params)

       # Get async Pub/Sub manager
       pubsub = async_client.pubsub

       # Async publish
       await pubsub.publish_async("user_events", {
           "event": "async_user_login",
           "user_id": "async_user"
       })

       await async_client.disconnect()

   # Run async publishing
   asyncio.run(async_publishing())

Async Subscription
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   async def async_subscription():
       connection_params = get_connection_params()
       async_client = AsyncClient(*connection_params)
       await async_client.connect(*connection_params)

       pubsub = async_client.pubsub

       async def async_message_handler(message):
           print(f"Async received: {message.data}")

       # Async subscribe
       subscription = await pubsub.subscribe_async(
           "user_events",
           async_message_handler
       )

       # Keep subscription active
       await asyncio.sleep(10)
       
       # Unsubscribe
       await subscription.unsubscribe_async()
       await async_client.disconnect()

   asyncio.run(async_subscription())

Real-world Examples
-------------------

Event-driven User Management
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class UserEventSystem:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.pubsub = self.client.pubsub
           self.setup_subscriptions()

       def setup_subscriptions(self):
           # Subscribe to user events
           self.pubsub.subscribe("user_events", self.handle_user_event)
           
           # Subscribe to system events
           self.pubsub.subscribe("system_events", self.handle_system_event)

       def handle_user_event(self, message):
           event_data = message.data
           event_type = event_data.get("event")
           
           if event_type == "user_login":
               self.on_user_login(event_data)
           elif event_type == "user_logout":
               self.on_user_logout(event_data)
           elif event_type == "user_register":
               self.on_user_register(event_data)

       def on_user_login(self, data):
           user_id = data.get("user_id")
           print(f"User {user_id} logged in")
           # Update user status, send notifications, etc.

       def on_user_logout(self, data):
           user_id = data.get("user_id")
           print(f"User {user_id} logged out")
           # Clean up sessions, update statistics, etc.

       def on_user_register(self, data):
           user_id = data.get("user_id")
           print(f"New user registered: {user_id}")
           # Send welcome email, create user profile, etc.

       def publish_user_event(self, event_type, user_id, metadata=None):
           event_data = {
               "event": event_type,
               "user_id": user_id,
               "timestamp": datetime.now().isoformat(),
               **(metadata or {})
           }
           self.pubsub.publish("user_events", event_data)

       def handle_system_event(self, message):
           # Handle system-level events
           print(f"System event: {message.data}")

   # Usage
   user_system = UserEventSystem()
   user_system.publish_user_event("user_login", "john_doe", {
       "ip_address": "192.168.1.100",
       "user_agent": "Mozilla/5.0..."
   })

Microservices Communication
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class MicroserviceA:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.pubsub = self.client.pubsub
           self.setup_communication()

       def setup_communication(self):
           # Subscribe to requests from other services
           self.pubsub.subscribe("service_a_requests", self.handle_request)
           
           # Subscribe to responses
           self.pubsub.subscribe("service_a_responses", self.handle_response)

       def handle_request(self, message):
           request_data = message.data
           request_id = request_data.get("request_id")
           
           # Process request
           result = self.process_request(request_data)
           
           # Publish response
           self.pubsub.publish("service_b_responses", {
               "request_id": request_id,
               "result": result,
               "status": "success"
           })

       def process_request(self, data):
           # Business logic here
           return {"processed": True, "data": data}

       def handle_response(self, message):
           # Handle responses from other services
           print(f"Received response: {message.data}")

   class MicroserviceB:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.pubsub = self.client.pubsub

       def send_request(self, request_data):
           request_id = f"req_{int(time.time())}"
           
           # Publish request
           self.pubsub.publish("service_a_requests", {
               "request_id": request_id,
               **request_data
           })
           
           # Subscribe to response
           response_received = False
           response_data = None
           
           def response_handler(message):
               nonlocal response_received, response_data
               if message.data.get("request_id") == request_id:
                   response_data = message.data
                   response_received = True
           
           self.pubsub.subscribe("service_b_responses", response_handler)
           
           # Wait for response (with timeout)
           timeout = 10  # seconds
           start_time = time.time()
           while not response_received and (time.time() - start_time) < timeout:
               time.sleep(0.1)
           
           return response_data

Real-time Analytics
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class RealTimeAnalytics:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.pubsub = self.client.pubsub
           self.metrics = {}
           self.setup_analytics()

       def setup_analytics(self):
           # Subscribe to various event streams
           self.pubsub.subscribe("user_events", self.track_user_metrics)
           self.pubsub.subscribe("system_events", self.track_system_metrics)
           self.pubsub.subscribe("business_events", self.track_business_metrics)

       def track_user_metrics(self, message):
           event_data = message.data
           event_type = event_data.get("event")
           
           # Update user metrics
           if event_type not in self.metrics:
               self.metrics[event_type] = 0
           self.metrics[event_type] += 1
           
           # Real-time dashboard updates
           self.update_dashboard()

       def track_system_metrics(self, message):
           # Track system performance metrics
           print(f"System metric: {message.data}")

       def track_business_metrics(self, message):
           # Track business KPIs
           print(f"Business metric: {message.data}")

       def update_dashboard(self):
           # Send metrics to dashboard
           self.pubsub.publish("dashboard_updates", {
               "metrics": self.metrics,
               "timestamp": datetime.now().isoformat()
           })

       def get_metrics(self):
           return self.metrics

Error Handling
--------------

Robust error handling for production applications:

.. code-block:: python

   from matrixone.exceptions import PubSubError, ConnectionError

   try:
       # Pub/Sub operations
       pubsub.publish("user_events", {"event": "test"})
   except PubSubError as e:
       print(f"Pub/Sub error: {e}")
   except ConnectionError as e:
       print(f"Connection error: {e}")
   except Exception as e:
       print(f"Unexpected error: {e}")

   # Retry mechanism for failed publishes
   def publish_with_retry(pubsub, topic, message, max_retries=3):
       for attempt in range(max_retries):
           try:
               pubsub.publish(topic, message)
               return True
           except Exception as e:
               print(f"Publish attempt {attempt + 1} failed: {e}")
               if attempt == max_retries - 1:
                   raise
               time.sleep(2 ** attempt)  # Exponential backoff
       return False

Performance Optimization
------------------------

Best practices for optimal performance:

.. code-block:: python

   # Batch message publishing
   def batch_publish(pubsub, topic, messages, batch_size=100):
       for i in range(0, len(messages), batch_size):
           batch = messages[i:i + batch_size]
           for message in batch:
               pubsub.publish(topic, message)

   # Efficient message filtering
   def efficient_subscription(pubsub, topic, handler, filters=None):
       # Use specific filters to reduce message processing
       return pubsub.subscribe(topic, handler, filter=filters)

   # Connection pooling for high-throughput applications
   class PubSubService:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.pubsub = self.client.pubsub
           self.lock = threading.Lock()

       def thread_safe_publish(self, topic, message):
           with self.lock:
               return self.pubsub.publish(topic, message)

Troubleshooting
---------------

Common issues and solutions:

**Message not received**
   - Verify topic name and subscription setup
   - Check message filters and format
   - Ensure subscription is active

**Performance issues**
   - Use batch operations for large message volumes
   - Optimize message filtering
   - Consider async operations for high-throughput scenarios

**Connection issues**
   - Verify MatrixOne server is running
   - Check connection parameters
   - Ensure proper network connectivity

**Message ordering**
   - Messages may not arrive in exact publish order
   - Use timestamps for ordering if needed
   - Consider message sequencing for critical applications

For more information, see the :doc:`api/client` and :doc:`best_practices`.
