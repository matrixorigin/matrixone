# **Install distributed MatrixOne**

MatrixOne supports distributed deloyment. At least 3 nodes are required for the distributed setting. 

### Two Types of Nodes
There are two types of nodes in a MatrixOne cluster: prophet nodes and pure storage nodes. The first three nodes are prophet nodes. The others are pure storage nodes. 

For example, if there're three nodes in the cluster, all the nodes are prophet nodes.
If there're four nodes in the cluster, three are prophet nodes, one is a pure storage node.


### Installation
Each node must install a MatrixOne as standalone version. Installation steps are the same as [the standalone](install-standalone-matrixone.md). 

### Generate Default Configuration
Run following command in directory `matrixone` to generate default Configuration.
```
$ make config
```
The default Configuration file is `system_vars_config.toml`.

### Configuration Settings

To configurate a distributed setting, following parameters need to be modified in the default Configuration file 'system_vars_config.toml'.

* **nodeID**

    `nodeID` is the Node ID of the cube. In a cluster, each node should have a different `nodeID`.

* **addr-raft and addr-advertise-raft**

    `addr-raft` is the address for raft-group rpc communication. 

    It is the 10000 port of the node.

    ```
    addr-raft = "localhost:10000"
    ```

    Only docker deployment need adjust `addr-advertise-raft`

    For docker deployment, the ip is `0.0.0.0`

    ```
    addr-raft = "0.0.0.0:10000"
    ```

    In the case that some clients cannot access the raft-group, `addr-advertise-raft` must be manually set.

    ```
    addr-advertise-raft = "${HOST}:10000"
    ```


* **addr-client and addr-advertise-client**

    `addr-client` is the address for cube service.
    It is the 20000 port of the node.

    ```
    addr-client = "localhost:20000"  
    ```

    Only docker deployment need adjust `addr-advertise-client`

    For docker deployment, the ip is `0.0.0.0`

    ```
    addr-client = "0.0.0.0:20000"  
    ```

    In the case that a client cannot access Cube through the default client URLs listened to by Cube, `addr-advertise-client` must be manually set.

    ```
    addr-advertise-client = "${HOST}:20000"
    ```


* **dir-data**

    `dir-data` is the directory for cube data. In a cluster, each node should have a different `dir-data`.

* **store-heartbeat-duration**

    `store-heartbeat-duration` is the period for this node to report information to scheduler. It should be less than 10s.

* **prophet name**

    `name` in `Prophet Configs` is the name of the node. In a cluster, each node should have a different prophet name.

    ```
    name = "node0"
    ```

* **rpc-addr and rpc-advertise-addr**

    `rpc-addr` is the address for other clients to access prophet. 
    It is the 30000 port of the node.

    ```
    rpc-addr = "localhost:30000"
    ```

    Only docker deployment need adjust `rpc-advertise-addr`

    For docker deployment, the ip is `0.0.0.0`

    ```
    rpc-addr = "0.0.0.0:30000"  
    ```

    In the case that a client cannot access prophet, `rpc-advertise-addr` must be manually set.

    ```
    rpc-advertise-addr = "${HOST}:30000"
    ```

* **storage-node**

    `storage-node` is whether the node is a pure storage node.
    For the three prophet nodes, `storage-node` is `true`.

    ```
    storage-node = true
    ```

    For pure storage nodes, `storage-node` is `false`

    ```
    storage-node = false
    ```

* **external-etcd**

    For the three prophet nodes, `external-etcd` is empty.

    ```
    external-etcd = ["", "", ""]
    ```

    For pure storage nodes, `external-etcd` is the list of the `client-urls` of the three prophet nodes, i.e. the 40000 port of the prophet nodes.

    ```
    external-etcd = ["http://ip_of_prophet_node1:40000", "http://ip_of_prophet_node2:40000", "http://ip_of_prophet_node3:40000"]
    ```

* **join**

    `join` is for the second and third prophet nodes to join the raft group.
    For the first prophet node, `join` is a empty string.

    ```
    join = ""
    ```

    For the second and third prophet node, `join` is the `peer-urls` of the first prophet node.

    ```
    join = "http://ip_of_the_first_prophet_node:40000"
    ```

    For pure storage nodes, there's no need to adjust `join`.

* **client-urls and advertise-client-urls**

    `client-urls` is exposed to other nodes in the cluster.
    It is 40000 port of the node

    ```
    client-urls = "http://localhost:40000"
    ``` 

    Only docker deployment need adjust `advertise-client-urls`

    For docker deployment, the ip is `0.0.0.0`

    ```
    client-urls = "http://0.0.0.0:40000"
    ```

    In the case that some clients ccannot access prophet through the default client URLs listened to by prophet, `advertise-client-urls` must be manually set.

    ```
    advertise-client-urls = "http://${HOST}:40000
    ```

* **peer-urls and advertise-peer-urls**

    `peer-urls` is the list of peer URLs to be listened to by a prophet node. It is the 40000 port of the node.

    ```
    peer-urls = "http://localhost:50000"
    ```

    Only docker deployment need adjust `advertise-peer-urls`

    For docker deployment, the ip is `0.0.0.0`

    ```
    peer-urls = "http://0.0.0.0:50000"
    ```

    In the case that a node cannot be listened to by a prophet node, `advertise-peer-urls` should be manually set.

    ```
    advertise-peer-urls = "http://${HOST}:50000"
    ```

* **max-replicas**

    `max-replicas` is the max number of replica in a prophet group. It should be 3.
    
    ```
    max-replicas = 3
    ```

