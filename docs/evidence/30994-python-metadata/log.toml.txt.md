# `log.toml.txt`

Artifact: {"name":"log.toml.txt","bytes":1140,"sha256":"461882f029b7a9a465d1e0662efdd0518b632f49c9a51513839e80be6819c535","encoding":"utf-8","stored_bytes":1140}

````toml
service-type = "LOG"
data-dir = "/Users/xupeng/.codex/issue-to-pr/matrixone/30994/runtime/data"
[log]
level = "info"
[limit]
memory = "1TB"
[hakeeper-client]
service-addresses = ["127.0.0.1:56301"]
[[fileservice]]
name = "LOCAL"
backend = "DISK-V2"
data-dir = "/Users/xupeng/.codex/issue-to-pr/matrixone/30994/runtime/data/local"
[[fileservice]]
name = "SHARED"
backend = "DISK-V2"
data-dir = "/Users/xupeng/.codex/issue-to-pr/matrixone/30994/runtime/data/shared"
[fileservice.cache]
memory-capacity = "128MB"
disk-capacity = "1GB"
disk-path = "/Users/xupeng/.codex/issue-to-pr/matrixone/30994/runtime/data/cache"
[[fileservice]]
name = "ETL"
backend = "DISK-ETL"
[observability]
disableTrace = true
disableMetric = true
[logservice]
uuid = "61f711b7-a2e9-44b6-b81f-0aa21e24f2d8"
deployment-id = 30994
logservice-port = 56301
raft-port = 56300
gossip-port = 56302
gossip-seed-addresses = ["127.0.0.1:56302"]
gossip-allow-self-as-seed = true
[logservice.BootstrapConfig]
bootstrap-cluster = true
num-of-log-shards = 1
num-of-tn-shards = 1
num-of-log-shard-replicas = 1
init-hakeeper-members = ["131072:61f711b7-a2e9-44b6-b81f-0aa21e24f2d8"]

````
