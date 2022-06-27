# morpc
The morpc is a [Goetty](https://github.com/fagongzi/goetty) wrapper based message communication framework. Based on morpc, you can implement `Request-Response`, `Stream` two types of communication.

## Components
morpc consists of `RPCClient` and `RPCServer`. RPCClient is used to send messages and RPCServer is used to process and respond to request messages.

### RPCClient
An RPCClient can manage multiple underlying tcp connections, which we call `Backend`. Each `Backend` will start two gortoutines to handle the IO reads and writes. A server address can correspond to more than one Backend, and load balancing by way of RoundRobin.

### RPCServer
RPCServer can listen to a TCP address or a UnixSocket.After a client connects, the RPCServer allocates two co-processes to handle the IO reads and writes.When RPCServer is started, it will set a message processing `Handler`, which will be invoked whenever a message is received from a client, and the specific logic of message processing needs to be implemented in the `Handler`.

## Examples
* [Request-Response](./examples/pingpong/main.go)
* [Stream](./examples/stream/main.go)


