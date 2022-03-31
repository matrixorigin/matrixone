# **Install distributed MatrixOne**

MatrixOne supports distributed deloyment. At least 3 stores (same as Nodes) are required for the distributed setting. 

### Two Types of Stores
There are two types of stores in a MatrixOne cluster: prophet stores and pure storage stores. The first three stores are prophet stores. The others are pure storage stores. 

For example, if there're three stores in the cluster, all the stores are prophet stores.
If there're five nodes in the cluster, three are prophet stores, two are pure storage stores.


### Installation

This tutorial is an example of 5 stores installation. 

#### *Step1*: 

Prepare 3 servers as prophet stores (recommended specification: x86 CPU with 16 cores and 64GB memory, with CentOS 7+ OS).

Each store must install a MatrixOne as standalone version. Installation steps are the same as [the standalone](install-standalone-matrixone.md). 

#### *Step2*:

Set up store1, which is also called the `Prophet Genesis Store`. 

1. Set `id` of the Prophet Genesis Store: 
```
nodeID = 1
```
2. Set  `addr-raft` and `addr-client` to receive Raft protocol and `Shard Proxy` messages from other MatrixCube stores: 
```
addr-raft = "your_ip_address:10000"
addr-client = "your_ip_address:20000"
```

3. Set the size limit of a `Shard`:
```
shard-capacity-bytes = "4096MB"
```

4. Set `rpc-addr` to receive heartbeats from the whole cluster:
```
rpc-addr = "your_ip_address:30000"
```

5. Set `join`, `client-urls` and `peer-urls` to group into a Etcd-server cluster. As the Prophet Genesis Store is the first store, the `join` parameter can remain empty:

```
join = ""
client-urls = "your_ip_address:40000"
peer-urls = "your_ip_address:50000"
```
6. Set `prophet-node` to TRUE, as the prophet store will hold the cluster metadata:
```
prophet-node = true
```
7. Set `max-replicas` number to the replica numbers that you want, it can only be `2*N+1` numbers:
```
max-replicas = 3
```

#### *Step3*: 

Set up store2 and store3 as prophet stores. Repeat the same procedure of step1 and step2 for each store. The only two differences are setting a unique `nodeID` and a `join` ip address. 
``` 
nodeID = 2 or 3; 
join = "ip_address_of_store1"
```

#### *Step4*:

Set up the other two stores as pure storage store. Repeat the same procedure of step1 and step2 for each store. There are three more parameters to be set:
* Set a unique number for nodeID. 
* Set `prophet-node` to TRUE.
* Fill the three empty string with the three client-urls of the three prophet node for the `external-etcd` parameter.
```
nodeID = 4/5;
prophet-node = false;
prophet-node = ["ip_address_of_prophet_store1","ip_address_of_prophet_store2","ip_address_of_prophet_store3"]
```

To learn more about our distribute system parameters, please refer to [distributed settings](../Reference/System-Parameters/distributed-settings.md). 
