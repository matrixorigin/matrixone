
对于分布式系统，应该注意以下参数。


### Configuration Settings
### 配置设置
要修改分布式配置，需要在默认配置文件`system_vars_config.toml`中修改以下参数：

* **nodeID**

    `nodeID` 是Cube集群中节点的编号。在集群中，每个节点都有一个不同的编号。

* **addr-raft 与 addr-advertise-raft**

   （ `addr-raft` is the address for raft-group rpc communication. ）
    `addr-raft`是raft组rpc通信时的地址：

   （It is the 10000 port of the node.）
    它是节点的10000号端口。

    ```
    addr-raft = "localhost:10000"
    ```

   （Only docker deployment need adjust `addr-advertise-raft`）
    只有使用 docker 部署时才需要调整`addr-advertise-raft`参数。

   （For docker deployment, the ip is `0.0.0.0`）
    对于 docker部署来说，Ip是`0.0.0.0`
    ```
    addr-raft = "0.0.0.0:10000"
    ```

    当部分客户端无法访问raft-group时，需要手动设置`addr-advertise-raft`。
    ```
    addr-advertise-raft = "${HOST}:10000"
    ```


* **addr-client 与 addr-advertise-client**

    （`addr-client` is the address for cube service.）
     `addr-client`是集群服务的地址，是节点的20000号端口。

    ```
    addr-client = "localhost:20000"  
    ```

    Only docker deployment need adjust `addr-advertise-client`
    只有使用 docker 部署时才需要调整`addr-advertise-client`参数。

    For docker deployment, the ip is `0.0.0.0`
    对于 docker部署来说，Ip是`0.0.0.0`
    ```
    addr-client = "0.0.0.0:20000"  
    ```

   （ In the case that a client cannot access Cube through the default client URLs listened to by Cube, `addr-advertise-client` must be manually set.）
    当客户端无法通过Cube监听的默认客户端链接访问Cube时，必须手动设置``addr-advertise-client`。
    ```
    addr-advertise-client = "${HOST}:20000"
    ```


* **dir-data**

    `dir-data` is the directory for cube data. In a cluster, each node should have a different `dir-data`.
    `dir-data`是Cube数据的目录，在集群中，每个节点应该有不同的`dir-data`。
* **store-heartbeat-duration**

    `store-heartbeat-duration` is the period for this node to report information to scheduler. It should be less than 10s.
    `store-heartbeat-duration`是当下节点向调度器报告信息的时间，不大于10s。

* **prophet name**

    `name` in `Prophet Configs` is the name of the node. In a cluster, each node should have a different prophet name.
    在`Prophet Configs` 中的`name`是节点名，在集群中，每个节点都有不同的`prophet name`。
    ```
    name = "node0"
    ```

* **rpc-addr 和 rpc-advertise-addr**

    `rpc-addr` is the address for other clients to access prophet. 
    `rpc-addr`是其他客户端访问prophet的地址，是节点的第30000号端口。

    ```
    rpc-addr = "localhost:30000"
    ```

    Only docker deployment need adjust `rpc-advertise-addr`
    只有使用 docker 部署时才需要调整`rpc-advertise-addr`参数。
    For docker deployment, the ip is `0.0.0.0`
    对于 docker部署来说，Ip是`0.0.0.0`。
    ```
    rpc-addr = "0.0.0.0:30000"  
    ```
    当客户端无法访问prophet时，必须手动设置`rpc-advertise-addr`。

    ```
    rpc-advertise-addr = "${HOST}:30000"
    ```

* **storage-node**

    `storage-node` is whether the node is a pure storage node.
    For the three prophet nodes, `storage-node` is `true`.
    `storage-node`表示节点是否是一个纯存储节点，对于集群中的三个prophet节点来说，该参数为`true`。
    ```
    storage-node = true
    ```
    对于纯存储节点来说，该参数为`false`。
    For pure storage nodes, `storage-node` is `false`

    ```
    storage-node = false
    ```

* **external-etcd**

    For the three prophet nodes, `external-etcd` is empty.
    对于集群中的三个prophet节点来说，`external-etcd`参数为空。
    ```
    external-etcd = ["", "", ""]
    ```

    For pure storage nodes, `external-etcd` is the list of the `client-urls` of the three prophet nodes, i.e. the 40000 port of the prophet nodes.
    对于纯存储节点，`external-etcd`参数为三个prophet节点的`client-urls`的列表，如：当prophet节点的端口号为40000时：
    ```
    external-etcd = ["http://ip_of_prophet_node1:40000", "http://ip_of_prophet_node2:40000", "http://ip_of_prophet_node3:40000"]
    ```

* **join**

    `join` is for the second and third prophet nodes to join the raft group.
    `join`是为了让第二个和第三个prophet节点加入raft组。
    For the first prophet node, `join` is a empty string.
    对于首个prophet节点，`join`取空值。
    ```
    join = ""
    ```

    For the second and third prophet node, `join` is the `peer-urls` of the first prophet node.
    对于第二、三个prophet节点，`join`取首个节点的`peer-urls`值，如：
    ```
    join = "http://ip_of_the_first_prophet_node:40000"
    ```

    For pure storage nodes, there's no need to adjust `join`.
    对于纯存储节点，不需要调整`join`参数。 

* **client-urls 和 advertise-client-urls**

    `client-urls` is exposed to other nodes in the cluster.
    It is 40000 port of the node
    `client-urls` 公开给集群中的其他节点，是节点的40000号端口：
    ```
    client-urls = "http://localhost:40000"
    ``` 

    Only docker deployment need adjust `advertise-client-urls`

    For docker deployment, the ip is `0.0.0.0`
    只有使用 docker 部署时才需要调整`advertise-client-urls`参数。
    对于docker部署，Ip为`0.0.0.0`：

    ```
    client-urls = "http://0.0.0.0:40000"
    ```

    In the case that some clients cannot access prophet through the default client URLs listened to by prophet, `advertise-client-urls` must be manually set.
    当客户端无法通过prophet监听的默认客户端链接访问prophet时，需要手动设置`advertise-client-urls`：
    ```
    advertise-client-urls = "http://${HOST}:40000
    ```

* **peer-urls 和 advertise-peer-urls**

    `peer-urls` is the list of peer URLs to be listened to by a prophet node. It is the 40000 port of the node.
    `peer-urls`是prophet节点监听的成员地址的列表，是节点的40000号端口：

    ```
    peer-urls = "http://localhost:50000"
    ```

    Only docker deployment need adjust `advertise-peer-urls`

    For docker deployment, the ip is `0.0.0.0`
    只有使用 docker 部署时才需要调整`advertise-peer-urls`参数。
    对于docker部署，Ip为`0.0.0.0`：

    ```
    peer-urls = "http://0.0.0.0:50000"
    ```

    In the case that a node cannot be listened to by a prophet node, `advertise-peer-urls` should be manually set.
    当节点无法被prophet节点监听到时，需要手动设置`advertise-peer-urls`：
    ```
    advertise-peer-urls = "http://${HOST}:50000"
    ```

* **max-replicas**

    `max-replicas` is the max number of replica in a prophet group. It should be 3.
    `max-replicas`是prophet组中最大副本数，应该设置为3。
    ```
    max-replicas = 3
    ```

