# Bootstrap Example

Assuming you are in MO's source directory.

Build mo-service -
```
make service
```
Start 3 mo-service instances configured as Log Service instances in 3 different terminals on the same machine. 

```
./mo-service -cfg etc/bootstrap-example/log-node-1.toml
```
```
./mo-service -cfg etc/bootstrap-example/log-node-2.toml
```
```
./mo-service -cfg etc/bootstrap-example/log-node-3.toml
```

It takes several seconds to start each process on mac as the SSD is slow when invoking fsync(). After about 10-20 seconds, you should be able to see a minimal MO cluster with just 3 Log Service instances. On each instance, there is a HAKeeper replica and a Log Shard replica. 

You should be able to see lots of logs on your terminals. "[00001:62146]" means Shard 1 ReplicaID 62146. You should be able to see log messages relate to replicas from both Shard 0 (HAKeeper) and Shard 1 (regular Log shard) in each one of your terminals.

To see how replicas are repaired in action, start another standby Log Service instance. 

```
./mo-service -cfg etc/bootstrap-example/log-node-4.toml
```

then stop on of the first 3 Log Service instances by using CTRL+C or a kill signal. Wait for about 30 seconds and you should be able to see two replicas launched on the fourth Log Service instance launched above. 
