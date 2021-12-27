# **Set Up MatrixOne**
    Matrixone supports both single node and distributed deployment, for single node deployment, you can start your journey with MatrixOne out of box.
    For distributed deployment, change each node's config file(system_vars_config.toml) according to the following instructions.
### **Deploy MatrixOne on a distributed bare-metal cluster**
```
    1. Requirements: at least three nodes

    2. set up the prophet genesis node
        2.1. make sure the nodeID is unique
        2.2. change the addr-raft ip to the machine ip
        2.3. change the addr-client ip to the machine ip
        2.4. make sure the prophet name is different from the names of other two prophet node
        2.5. change the rpc-addr ip to the machine ip
        2.6. change the client-urls ip to the machine ip
        2.7. change the peer-urls ip to the machine ip
        2.8. change the shard-groups value to 2
        2.9. make sure the dir-data is different from the other nodes in the cluster

    3. set up the other two prophet nodes
        3.1. apply the above 9 steps of prophet genesis node setting
        3.2. change the prophet join address from empty string to the prophet genesis node's peer-urls
   
    4. set up pure storage node
        4.1. make sure the nodeID is unique
        4.2. change storage-node to false
        4.3. change the addr-raft ip to the machine ip
        4.4. change the addr-client ip to the machine ip
        4.5. In the external-etcd attribute, fill the three empty string with the three peer-urls of the three prophet node
        4.6. make sure the dir-data is different from the other nodes in the cluster
        4.7. change the shard-groups value to 2
```

### **Deploy MatrixOne on a docker cluster**
Start MatrixOne cluster on docker or kubernetes, please refer to this repo [matrixorigin/matrixone-operator](https://github.com/matrixorigin/matrixone-operator)                                  