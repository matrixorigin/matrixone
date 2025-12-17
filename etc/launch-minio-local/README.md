# Local Development with MinIO Storage

This configuration directory is for local development environment using MinIO as object storage.

## Features

- ✅ Independent launch and configuration files
- ✅ MinIO service started via docker-compose
- ✅ MinIO data mounted to local directory `mo-data/minio-data/` with user permissions
- ✅ Support local code compilation and debugging with MinIO storage

## Quick Start

### 1. Start MinIO Service

```bash
# Execute from project root directory
make dev-up-minio-local
```

This will:
- Create `mo-data/minio-data/` directory (if it doesn't exist)
- Start MinIO container with local directory mounted
- Automatically create `mo-test` bucket
- Set bucket to public access

MinIO Service Information:
- API Address: http://127.0.0.1:9000
- Console Address: http://127.0.0.1:9001
- Access Key: `minio`
- Secret Key: `minio123`

### 2. Build and Start MatrixOne

```bash
# Build and start (using MinIO storage)
make launch-minio
```

Or execute step by step:

```bash
# 1. Build
make build

# 2. Start MinIO (if not already started)
make dev-up-minio-local

# 3. Start MatrixOne
./mo-service -launch etc/launch-minio-local/launch.toml
```

### 3. Debug Mode Startup

```bash
# Build and start in debug mode
make launch-minio-debug
```

## Common Commands

```bash
# Start MinIO service
make dev-up-minio-local

# Check MinIO status
make dev-status-minio-local

# View MinIO logs
make dev-logs-minio-local

# Restart MinIO service
make dev-restart-minio-local

# Stop MinIO
make dev-down-minio-local

# Clean MinIO data (will delete all data!)
make dev-clean-minio-local
```

## Configuration Files

- `launch.toml`: Cluster startup configuration, specifying configuration file paths for each service
- `log.toml`: Log service configuration, using MinIO as SHARED and ETL storage
- `tn.toml`: TN service configuration, using MinIO as SHARED and ETL storage
- `cn.toml`: CN service configuration, using MinIO as SHARED and ETL storage
- `docker-compose.yml`: MinIO service configuration

## Storage Configuration

All services are configured with three fileservices:

1. **LOCAL**: Uses local disk storage (`./mo-data`)
2. **SHARED**: Uses MinIO storage (bucket: `mo-test`, prefix: `server/data`)
3. **ETL**: Uses MinIO storage (bucket: `mo-test`, prefix: `server/etl`)

MinIO Configuration:
- Endpoint: `http://127.0.0.1:9000`
- Bucket: `mo-test`
- Access Key: `minio`
- Secret Key: `minio123`

## Data Directory

All data is stored in the `etc/launch-minio-local/mo-data/` directory:
- MinIO data: `etc/launch-minio-local/mo-data/minio-data/`
- MatrixOne data: `etc/launch-minio-local/mo-data/`

To clean up data, simply delete the `etc/launch-minio-local/` directory.

## Notes

1. **Port Conflicts**: Ensure ports 9000 and 9001 are not occupied by other services
2. **Permission Issues**: MinIO container uses current user's UID/GID, ensure you have permission to access the `mo-data/minio-data/` directory
3. **First Startup**: Bucket will be automatically created on first startup. If creation fails, you can manually create it via MinIO Console
4. **Data Persistence**: All data (MinIO and MatrixOne) is stored in the `mo-data/` directory. Deleting the `etc/launch-minio-local/` directory will lose all data

## Connect to MatrixOne

After successful startup, you can connect using:

```bash
mysql -h 127.0.0.1 -P 6001 -u root -p
```

Default password is usually empty or `111` (depending on configuration).

## Troubleshooting

### MinIO Cannot Start

1. Check if ports are occupied:
   ```bash
   lsof -i :9000
   lsof -i :9001
   ```

2. Check directory permissions:
   ```bash
   ls -la etc/launch-minio-local/mo-data/minio-data/
   ```

3. View logs:
   ```bash
   make dev-logs-minio-local
   ```

### MatrixOne Cannot Connect to MinIO

1. Confirm MinIO is started:
   ```bash
   make dev-status-minio-local
   ```

2. Test MinIO connection:
   ```bash
   curl http://127.0.0.1:9000
   ```

3. Check if the endpoint in configuration files is correct (should be `http://127.0.0.1:9000`)

### Bucket Does Not Exist

If bucket creation fails, you can manually create it:

1. Access MinIO Console: http://127.0.0.1:9001
2. Login with `minio` / `minio123`
3. Create bucket `mo-test`
4. Set bucket to public

Or use mc client:

```bash
docker run --rm --network launch-minio-local_mo-minio-network \
  -it minio/mc:RELEASE.2023-10-30T18-43-32Z \
  alias set myminio http://minio:9000 minio minio123 && \
  mc mb myminio/mo-test && \
  mc anonymous set public myminio/mo-test
```
