# Launch-tae-CN-tae-dn with docker-compose

- docker compose version >= v2.12.1

## build and up 

build new image

```shell
docker-compose -f etc/launch-tae-compose/compose.yaml up -d --build
```

use default image

```shell
docker-compose -f etc/launch-tae-compose/compose.yaml pull
docker-compose -f etc/launch-tae-compose/compose.yaml up -d
```

## Check log

```shell
# cn
docker compose -f etc/launch-tae-compose/compose.yaml logs cn

# dn
docker compose -f etc/launch-tae-compose/compose.yaml logs dn

# logService
docker compose -f etc/launch-tae-compose/compose.yaml logs logService
```

## minio as s3 service

[localhost:9001](http://localhost:9001)

username: minio
password: minio123

## down

```shell
docker-compose -f etc/launch-tae-compose/compose.yaml down --remove-orphans
```

## clean dangling image

```shell
docker image prune -f
```

## remove minio storage
```shell
docker volume rm launch-tae-compose_minio_storage
```
