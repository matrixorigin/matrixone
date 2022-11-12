# Launch mo with docker compose

## mo with minio

export env

```shell
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=minio123
export LAUNCH=minio
```

bootstrap

```shell
# build image
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN-minio up -d --build

# or pull image
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN-minio pull

# 2 cn 1 dn 1 logservice 
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN-minio up -d 
```

stop serive

```shell
# 2 cn 1 dn 1 logservice 
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN-minio down  --remove-orphans
```

## mo with s3

Using default bucket: `matrixorigin-docker-compose-test`

Using default region: `us-west-2`

for using cutom bucket

```shell
# create bucket
aws s3api create-bucket --bucket <YOUR_BUCKET_NAME>  --region us-west-2 --create-bucket-configuration LocationConstraint=us-west-2
```

Repleace default bucket, change config (config/s3/*)

```
bucket = <YOUR_BUCKET_NAME>
```

after testing, can delete bucket

```
aws s3 rb s3://<YOUR_BUCKET_NAME> --force
```

export env

```shell
export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
export AWS_REGION=us-west-2
export LAUNCH=s3
```

bootstrap

```shell
# build image
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN up -d --build

# or pull image
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN pull

# 2 cn 1 dn 1 logservice 
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN up -d 
```

stop service

```shell
# 2 cn 1 dn 1 logservice 
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN down  --remove-orphans
```

## mo local

export env

```shell
export LAUNCH=local
```

bootstrap service

```shell
# build image
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN up -d --build

# or pull image
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN pull

# 2 cn 1 dn 1 logservice 
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN up -d 
```

stop service

```shell
# 2 cn 1 dn 1 logservice 
docker-compose -f etc/launch-mo-compose/compose.yaml --profile launch-tae-multi-CN-tae-DN down  --remove-orphans
```