# Local Development with MinIO Storage

This configuration directory is for local development environment using MinIO as object storage. It also starts a local Nessie REST catalog for Iceberg Tier A integration tests.

## Features

- ✅ Independent launch and configuration files
- ✅ MinIO service started via docker-compose
- ✅ Nessie REST catalog started via docker-compose
- ✅ Optional Docker-free Tier A startup via local Homebrew `minio`, `mc`, and `nessie`
- ✅ MinIO data mounted to local directory `mo-data/minio-data/` with user permissions
- ✅ Support local code compilation and debugging with MinIO storage

## Quick Start

### 1. Start MinIO and Nessie Services

```bash
# Execute from project root directory
make dev-up-minio-local
```

This will:
- Create `mo-data/minio-data/` directory (if it doesn't exist)
- Start MinIO container with local directory mounted
- Start Nessie REST catalog
- Automatically create `mo-test` and `mo-iceberg` buckets
- Set bucket to public access

MinIO Service Information:
- API Address: http://127.0.0.1:9000
- Console Address: http://127.0.0.1:9001
- Access Key: `minio`
- Secret Key: `minio123`

Iceberg Tier A Information:
- Nessie REST Catalog: http://127.0.0.1:19120/iceberg
- Warehouse: `s3://mo-iceberg/warehouse`
- MinIO endpoint for Spark/MO: `http://127.0.0.1:9000`

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

# Start the Iceberg Tier A stack (alias for MinIO + Nessie)
make dev-up-iceberg-tier-a

# Or start the Iceberg Tier A stack with local Homebrew binaries
make dev-up-iceberg-tier-a-brew

# Seed deterministic Tier A Iceberg tables and generate test env
make dev-seed-iceberg-tier-a

# Run Tier A tests after seeding
make dev-test-iceberg-tier-a

# Check MinIO status
make dev-status-minio-local

# Check Tier A service status
make dev-status-iceberg-tier-a

# Check Homebrew-backed Tier A service status
make dev-status-iceberg-tier-a-brew

# View MinIO logs
make dev-logs-minio-local

# View MinIO and Nessie logs
make dev-logs-iceberg-tier-a

# View Homebrew-backed MinIO and Nessie logs
make dev-logs-iceberg-tier-a-brew

# Restart MinIO service
make dev-restart-minio-local

# Stop MinIO and Nessie
make dev-down-minio-local

# Stop Homebrew-backed Tier A services
make dev-down-iceberg-tier-a-brew

# Clean MinIO data (will delete all data!)
make dev-clean-minio-local
```

## Configuration Files

- `launch.toml`: Cluster startup configuration, specifying configuration file paths for each service
- `log.toml`: Log service configuration, using MinIO as SHARED and ETL storage
- `tn.toml`: TN service configuration, using MinIO as SHARED and ETL storage
- `cn.toml`: CN service configuration, using MinIO as SHARED and ETL storage
- `docker-compose.yml`: MinIO and Nessie service configuration

## Storage Configuration

All services are configured with three fileservices:

1. **LOCAL**: Uses local disk storage (`./mo-data`)
2. **SHARED**: Uses MinIO storage (bucket: `mo-test`, prefix: `server/data`)
3. **ETL**: Uses MinIO storage (bucket: `mo-test`, prefix: `server/etl`)

MinIO Configuration:
- Endpoint: `http://127.0.0.1:9000`
- Bucket: `mo-test`
- Iceberg warehouse bucket: `mo-iceberg`
- Access Key: `minio`
- Secret Key: `minio123`

Nessie Configuration:
- REST catalog URI: `http://127.0.0.1:19120/iceberg`
- Branch: `main`

## Iceberg Tier A Test Gate

The Tier A tests are opt-in and fail closed when enabled:

```bash
make dev-up-iceberg-tier-a

# Requires spark-sql. Optionally set MO_ICEBERG_MO_SQL_CMD to also create MO mappings.
# Set MO_ICEBERG_SPARK_PACKAGES when the local spark-sql version needs different Iceberg runtime coordinates.
MO_ICEBERG_MO_SQL_CMD='mysql -h 127.0.0.1 -P 6001 -u root -p111 -e {sql}' \
make dev-seed-iceberg-tier-a

source etc/launch-minio-local/tier-a/tier_a.generated.env

MO_ICEBERG_REPORT_DIR=test/iceberg/reports/run_$(date -u +%Y%m%dT%H%M%SZ) \
go test ./pkg/sql/iceberg -run TestIcebergTierA -count=1
```

For a Docker-free service preflight on macOS:

```bash
make dev-up-iceberg-tier-a-brew

MO_ICEBERG_IT=1 \
MO_ICEBERG_CATALOG_URI=http://127.0.0.1:19120/iceberg \
MO_ICEBERG_S3_ENDPOINT=http://127.0.0.1:9000 \
go test ./pkg/sql/iceberg -run TestIcebergTierALocalNessieMinIOPreflight -count=1
```

`tier-a/seed.spark.sql` creates deterministic read/time-travel/schema-evolution/delete/DML/ref tables. `tier-a/seed-iceberg-tier-a.sh` writes `tier-a/tier_a.generated.env`, including snapshot ids, `MO_ICEBERG_SPARK_SQL_CMD`, optional `MO_ICEBERG_MO_SQL_CMD`, and the MO/Spark comparison SQL consumed by `pkg/sql/iceberg/testdata/tier_a_scenarios.json`. Set `MO_ICEBERG_SPARK_PACKAGES` when the local `spark-sql` version needs different Iceberg runtime coordinates than the default Spark 3.5 artifact. `MO_ICEBERG_REPORT_DIR` is optional; when set, each scenario writes `metadata.json`, `diff.json`, `mo.out`, `spark.out`, and `summary.md`.

## Data Directory

All data is stored in the `etc/launch-minio-local/mo-data/` directory:
- MinIO data: `etc/launch-minio-local/mo-data/minio-data/`
- MatrixOne data: `etc/launch-minio-local/mo-data/`

To clean up data, simply delete the `etc/launch-minio-local/` directory.

## Notes

1. **Port Conflicts**: Ensure ports 9000, 9001 and 19120 are not occupied by other services
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
