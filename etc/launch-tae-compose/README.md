# Launch-tae-CN-tae-dn with docker-compose

## Export aws configuration

```shell
export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
export AWS_REGION=<AWS_REGION>
```

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

## down

```shell
docker-compose -f etc/launch-tae-compose/compose.yaml down --remove-orphans
```

## clean dangling image

```shell
docker image prune -f
```
