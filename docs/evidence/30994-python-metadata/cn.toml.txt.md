# `cn.toml.txt`

Artifact: {"name":"cn.toml.txt","bytes":975,"sha256":"7d6d6268d367b8ad382d18786359c8ce875d361b54271dfba9d1852fa456a7e8","encoding":"utf-8","stored_bytes":975}

````toml
service-type = "CN"
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
[cn]
uuid = "b21220b2-6753-437a-8b18-d33281276a22"
port-base = 56200
[cn.frontend]
processLimitationSize = 4294967296
processLimitationSpillSize = 4294967296
port = 56411
unix-socket = "/Users/xupeng/.codex/issue-to-pr/matrixone/30994/runtime/mysql.sock"

````
