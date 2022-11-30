# Launch-tae-CN-tae-dn with docker-compose

- [docker compose version](https://docs.docker.com/compose/install/) >= v2.12.1
- support profiles: launch-tae-multi-CN-tae-DN, launch-tae-CN-tae-DN

## build and up 

build new image

```shell
docker-compose -f etc/launch-tae-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN up -d --build
```

use default image

```shell
docker-compose -f etc/launch-tae-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN pull
docker-compose -f etc/launch-tae-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN up -d
```

## Check log

```shell
# cn-0
docker compose -f etc/launch-tae-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN logs cn-0

# cn-1 
docker compose -f etc/launch-tae-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN logs cn-1

# dn
docker compose -f etc/launch-tae-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN logs dn

# logService
docker compose -f etc/launch-tae-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN logs logservice
```

## minio as s3 service

[localhost:9001](http://localhost:9001)

username: minio
password: minio123

## connect service

cn-0
```shell
mysql -h 127.0.0.1 -P 7001 -udump -p111
```

cn-1:
```shell
mysql -h 127.0.0.1 -P 8001 -udump -p111
```

## down

```shell
docker-compose -f etc/launch-tae-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN down --remove-orphans
```

## clean dangling image

```shell
docker image prune -f
```

## remove minio storage
```shell
docker volume rm launch-tae-compose_minio_storage
```
