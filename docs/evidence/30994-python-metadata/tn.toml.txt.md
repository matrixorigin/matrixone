# `tn.toml.txt`

Artifact: {"name":"tn.toml.txt","bytes":789,"sha256":"4e49dd8a004f361e69d97d6bb990b7f388374d315d198f92dc520ccc03c89cca","encoding":"utf-8","stored_bytes":789}

````toml
service-type = "TN"
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
[tn]
uuid = "2240e5f8-ee76-486c-a192-ec88daeb9a51"
port-base = 56400

````
