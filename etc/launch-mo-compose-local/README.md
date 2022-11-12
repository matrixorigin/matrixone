# Launch mo local

## Bootstrap service

```
docker-compose -f etc/launch-mo-compose-local/compose.yaml pull
docker-compose -f etc/launch-mo-compose-local/compose.yaml up -d
```

## Stop service

```
docker-compose -f etc/launch-mo-compose-local/compose.yaml stop
```

## Shutdown service

```
docker-compose -f etc/launch-mo-compose-local/compose.yaml down --remove-orphans
```
