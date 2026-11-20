# Launch-tae-CN-tae-tn with docker-compose

- [docker compose version](https://docs.docker.com/compose/install/) >= v2.12.1
- support profiles: launch, launch-multi-cn

## build and up 

build new image

```shell
docker-compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn up -d --build
```

build with typecheck enabled (optional, default: disabled)

```shell
TYPECHECK=1 docker-compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn up -d --build
```

use default image

```shell
docker-compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn up -d
```

MinIO and mc are built from pinned official release binaries with SHA-256
verification. Their Compose services use linux/amd64 images.

The default launch builds `xtool/jstfu` from source and publishes the jar
through a Compose volume. CI can reuse an exact-head jar produced by its shared
build by setting all three variables below:

```bash
JSTFU_USE_PREBUILT=1 \
JSTFU_EXPECTED_SHA256=<sha256-of-xtool/jstfu/target/jstfu.jar> \
JSTFU_BUILDER_IMAGE=matrixorigin/matrixone:latest \
docker compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn up -d
```

Prebuilt mode is fail-closed: a missing jar, malformed checksum, or checksum
mismatch prevents the dependent jstfu sidecars and the BVT workload from
starting.

MatrixOne's trusted main-branch cache warmer keeps the Maven repository and
wrapper distribution available to PR CI. Pull-request jobs restore that cache
read-only; they never publish dependencies resolved from an untrusted head.

## Check log

```shell
# cn-0
docker compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn logs cn-0

# cn-1 
docker compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn logs cn-1

# proxy
docker compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn logs proxy

# tn
docker compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn logs tn

# logService
docker compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn logs logservice
```

## minio as s3 service

[localhost:9001](http://localhost:9001)

username: minio
password: minio123

## connect service through proxy

```shell
mysql -h 127.0.0.1 -P 6001 -udump -p111
```

Port `6001` is the Proxy entrypoint. The two ordinary CNs remain eligible
backends. The dedicated `cn-catalog` uses full catalog subscription and a
reserved account label, so normal BVT logins do not select it through Proxy.
Only its SQL and debug endpoints are published on ephemeral loopback ports.

On fresh startup, `catalog-placement` waits for the StorageUsage cron worker
to apply its label and for a labeled instance to run on `cn-catalog`, before
the ordinary CNs start. `observability.reset-storage-usage-label` applies
`task-label` to StorageUsage only; the existing `reset-task-label` still
applies to both StorageUsage and ETLMergeTask.

In the grouped Compose BVT, files containing `SHOW ACCOUNTS` use the full CN
for the entire file (including session variables and `RESULT_SCAN`); other
files use the original tester configuration, in the original order. The runner
requires Python 3 and PyYAML. To opt into this routing in a local group run:

```bash
compose=etc/launch-tae-compose/compose.yaml
export BVT_FULL_CATALOG_ADDR=$(docker compose -f "$compose" --profile launch-multi-cn port cn-catalog 6001)
debug=$(docker compose -f "$compose" --profile launch-multi-cn port cn-catalog 12345)
export BVT_FULL_CATALOG_DEBUG_PORT=${debug##*:}
bash optools/run_bvt_group.sh /path/to/mo-tester test/distributed/cases 0
```

## down

```shell
docker-compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn down --remove-orphans
```

## clean dangling image

```shell
docker image prune -f
```

## remove minio storage
```shell
docker volume rm launch-tae-compose_minio_storage
```
