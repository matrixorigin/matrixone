# Bootstrap Example 2

Assuming you are in MO's source directory.

Build mo-service -

```
make service
```
Start 3 mo-service instances configured as Log Store instances and 1 as Data Node instance in 4 different terminals on the same machine.
(**Note**: Currently, you need to start DN Store **first**.)

```
./mo-service -cfg etc/distributed-mem-engine-exampl/dn-node-1.toml
```

```
./mo-service -cfg etc/distributed-mem-engine-exampl/log-node-1.toml
```

```
./mo-service -cfg etc/distributed-mem-engine-exampl/log-node-2.toml
```

```
./mo-service -cfg etc/distributed-mem-engine-exampl/log-node-3.toml
```

```
./mo-service -cfg etc/distributed-mem-engine-exampl/cn-node-1.toml
```

```
./mo-service -cfg etc/distributed-mem-engine-exampl/cn-node-2.toml
```

It takes several seconds to start each process on mac as the SSD is slow when invoking fsync(). After about 10-20 seconds, you should be able to see a minimal MO cluster with just 1 Data Service and 3 Log Service instances. On each Log Service instance, there is a HAKeeper replica and a Log Shard replica. 

