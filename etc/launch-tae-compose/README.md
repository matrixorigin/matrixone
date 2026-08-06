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
docker-compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn pull
docker-compose -f etc/launch-tae-compose/compose.yaml --profile launch-multi-cn up -d
```

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

Port `6001` is the Proxy entrypoint. CN SQL ports are intentionally not
published to the host; clients should enter through Proxy so that both CNs are
eligible backends.

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
