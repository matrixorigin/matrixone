# Launch with S3

## export env 

```
export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
export AWS_REGION=us-west-2
```

## create buket for mo

create bucket 

```
aws s3api create-bucket --bucket matrixorigin-docker-compose-test  --region us-west-2 --create-bucket-configuration LocationConstraint=us-west-2 
```

or choose a exist bucket, config it (replace matrixorigin-docker-compose-test) at 

cn-0.toml dn.toml log.toml

```
bucket = "matrixorigin-docker-compose-test"
```

## Bootstrap service

```
docker-compose -f etc/launch-mo-compose-s3/compose.yaml pull
docker-compose -f etc/launch-mo-compose-s3/compose.yaml up -d
```

## Stop service

```
docker-compose -f etc/launch-mo-compose-s3/compose.yaml stop
```

## Shutdown service

```
docker-compose -f etc/launch-mo-compose-s3/compose.yaml down --remove-orphans
```

## delete s3 bucket

```
aws s3 rb s3://matrixorigin-docker-compose-test --force 
```