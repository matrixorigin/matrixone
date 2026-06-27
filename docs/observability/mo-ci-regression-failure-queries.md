# MO CI Regression Failure Queries

Use these queries when investigating sysbench regression failures similar to
issues 25074 and 25164.

## Prometheus

CN/DN working set:

```promql
sum(container_memory_working_set_bytes{namespace=~"$namespace", pod=~"$pod", container!="", pod=~".*(cn|dn).*"}) by (pod)
```

CN/DN CPU usage:

```promql
sum(rate(container_cpu_usage_seconds_total{namespace=~"$namespace", pod=~"$pod", container!="", pod=~".*(cn|dn).*"}[$interval])) by (pod)
```

DN merge OOM governor:

```promql
sum(increase(mo_task_merge_oom_pause_total{namespace=~"$namespace", pod=~"$pod"}[$interval])) by (pod)
```

Proxy CN health and all-busy:

```promql
sum(rate(mo_proxy_cn_health_total{namespace=~"$namespace", pod=~"$pod"}[$interval])) by (event, cn_uuid)
sum(rate(mo_proxy_connect_counter{namespace=~"$namespace", pod=~"$pod", type="cn-all-busy"}[$interval])) by (pod)
```

Proxy backend handshake:

```promql
histogram_quantile(0.95, sum(rate(mo_proxy_backend_handshake_duration_seconds_bucket{namespace=~"$namespace", pod=~"$pod"}[$interval])) by (le, cn_uuid, result))
sum(mo_proxy_backend_handshake_inflight{namespace=~"$namespace", pod=~"$pod"}) by (cn_uuid)
```

MORPC and lockservice errors:

```promql
sum(rate(mo_rpc_backend_error_total{namespace=~"$namespace", pod=~"$pod"}[$interval])) by (name, backend, phase, error_type)
sum(rate(mo_lockservice_remote_rpc_error_total{namespace=~"$namespace", pod=~"$pod"}[$interval])) by (method, error_type)
```

## Loki

DN OOM governor:

```logql
{namespace="$namespace", pod=~"$pod"} |= "MergeExecutorEvent-PauseDueToOOMAlert"
```

Proxy health/all-busy:

```logql
{namespace="$namespace", pod=~"$pod"} |~ "proxy CN health tripped|proxy CN health probe failed|all CN servers are busy|all candidate CN servers are temporarily unhealthy"
```

CN handshake and RPC timeout symptoms:

```logql
{namespace="$namespace", pod=~"$pod"} |~ "cannot get salt|i/o timeout|failed to keep remote locks|cannot connect to backend"
```
