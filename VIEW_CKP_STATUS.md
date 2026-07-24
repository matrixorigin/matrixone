# view_ckp 分支状态总结

## 分支概述

`view_ckp` 分支主题是 **checkpoint 离线工具链**：checkpoint 浏览、表 schema 解析、CSV 数据 dump、LOAD DATA 回灌兼容，以及远程 S3/MinIO fileservice 读取。

| 提交 | 内容 |
|------|------|
| `913f27d09` | 添加远程 checkpoint 工具和逻辑表视图 |
| `eaa1bfc11` | 修复 checkpoint schema 回放和 CSV 列映射 |
| `953b6e76b` | 优化 checkpoint CSV dump 并添加 layout 兼容性 |
| `9a0e208a0` | 修复 checkpoint 系统表 dump |
| `721cab6e5` | 使 CSV dump 兼容 LOAD DATA |

## 当前环境

- **mo-service**：本次验证结束后已停止；测试时 MySQL 端口为 `6001`，root 密码 `111`
- **存储后端**：当前 `etc/launch-minio-local/*.toml` 使用 MinIO 作为 `SHARED`/`ETL`，`LOCAL` 使用 DISK
- **checkpoint 目录**：`etc/launch-minio-local/mo-data`
- **当前 checkpoint**：`Total Entries: 44`，`Global: 11`，`Incremental: 33`，`Latest TS: 1781084792256166694-1`
- **构建命令**：`make mo-tool`

注意：在当前 Codex/exec 环境里，`nohup ... &` 启动的后台进程可能会随命令会话结束被清理；本次验证使用前台 exec session 启动 `mo-service`。普通 shell 环境可继续按后台方式启动。

## 已修复问题

### 1. 远程 S3/MinIO checksum 不匹配

现象：

```text
Error: open checkpoint dir: internal error: checksum not match
```

根因：

- MO 写入 S3/MinIO 的 object 文件以 `pkg/objectio/object.go` 中的 `Magic = 0xFFFFFFFF` 开头。
- `mo-tool` 的 `lazyCacheFS` 把远程文件下载到本地临时目录后，再通过 `LocalFS.Read` 读取。
- `LocalFS.Read` 会按本地 fileservice checksum 格式解析，期望文件块以 4 字节 CRC32 开头。
- 远程 object 文件的 Magic 前缀不是 LocalFS checksum，因此必然报 checksum mismatch。

修复：

- `pkg/tools/toolfs/lazy_cache_fs.go` 中，缓存文件仍落本地，但读取缓存时直接按 OS file range 读取，不再通过 `LocalFS.Read` 的 checksum 包装路径。
- `Write` 后清理本地缓存，避免读到旧缓存。

验证：

```bash
go test ./pkg/tools/toolfs
```

结果：通过。

### 2. 用户表 CSV dump 无法解析 mo_columns

原始报错：

```text
cannot resolve visible columns for table 272535 from checkpoint metadata;
mo_columns data is unavailable or incomplete
```

根因不是单纯的固定列宽问题。实测当前 checkpoint raw logical view：

- `mo_tables` 可见导出为 19 列，但 raw view 可能带尾部额外列。
- `mo_columns` raw view 为 `data=28`，目标行字段从 `att_uniq_name` 开始，额外列不是稳定前缀。
- 当前源码里的 `catalog.MoTablesSchema`/`catalog.MoColumnsSchema` 与 checkpoint 中的 catalog 布局不完全一致，尤其涉及 `CPKey`、`ExtraInfo`、生成列等字段。
- 只按 `len(schema)+offset` 做一次 layout 推断，会在 `pre-cpk/current/legacy` 布局之间误判。

修复：

- 新增 `preCPKLayout`，覆盖无 `CPKey`、无 `ExtraInfo` 的历史 catalog 布局。
- `mo_columns` 和 `mo_tables` 解析改为候选布局试读：优先使用实际 header 名称；header 只有 `col_N` 时，按已知 catalog 布局和偏移逐个尝试，只有能读出目标 table_id 的列/DDL 时才采纳。
- `mo_columns` 字段名改为当前 catalog 常量，例如 `attr_seqnum`、`attr_update`，避免旧的 `att_seqnum` 写法导致索引失配。
- `show-create-table` 优先通过候选布局读取 `mo_tables.rel_createsql`，避免退化到 `mo_columns.atttyp` 的内部二进制类型编码。

相关文件：

- `pkg/tools/checkpointtool/table_dump.go`
- `pkg/tools/checkpointtool/table_dump_test.go`

验证：

```bash
CGO_CFLAGS="-I$(pwd)/thirdparties/install/include " \
CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib" \
LD_LIBRARY_PATH="$(pwd)/thirdparties/install/lib:$LD_LIBRARY_PATH" \
go test ./pkg/tools/checkpointtool

make mo-tool
```

结果：通过。

## 实测结果

### 普通表导出与 LOAD DATA

测试表：

```sql
CREATE TABLE test_ckp.employees (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    salary DECIMAL(10,2),
    department VARCHAR(50),
    hire_date DATE,
    is_active BOOL
);
```

table_id：

```text
272535
```

导出：

```bash
./mo-tool ckp dump --table-id=272535 --header \
  -o /tmp/employees_dump.csv \
  etc/launch-minio-local/mo-data
```

结果：

```text
104 /tmp/employees_dump.csv
id,name,age,salary,department,hire_date,is_active
1,"Alice",30,75000.00,"Engineering",2020-01-15,true
2,"Bob",\N,\N,\N,\N,\N
3,"Charlie",\N,65000.00,\N,2021-03-10,false
```

`show-create-table`：

```bash
./mo-tool ckp show-create-table --table-id=272535 etc/launch-minio-local/mo-data
```

结果正确返回原始 DDL：

```sql
CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    salary DECIMAL(10,2),
    department VARCHAR(50),
    hire_date DATE,
    is_active BOOL
)
```

LOAD DATA 校验：

```text
original_rows loaded_rows
103           103

diff_rows
0
```

### MinIO 远程 checkpoint 导出与 LOAD DATA

本次追加验证时间：`2026-06-11`。

测试环境：

- 使用本地 `/usr/local/bin/minio` 启动 MinIO，API 为 `http://127.0.0.1:9000`
- bucket：`mo-test`
- `SHARED` key-prefix：`server/data`
- 访问密钥：`minio` / `minio123`
- Docker compose 路径未使用：`docker pull minio/minio:RELEASE.2023-11-01T18-37-25Z` 通过当前 Docker registry 代理返回 500，因此改用本地 MinIO 二进制

测试表：

```sql
CREATE TABLE test_ckp_minio.employees_minio (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    salary DECIMAL(10,2),
    department VARCHAR(50),
    hire_date DATE,
    is_active BOOL
);
```

table_id：

```text
272535
```

确认 MO 实际使用 MinIO：

```text
new object storage {"sdk": "minio", "arguments": {"Name":"SHARED","Bucket":"mo-test","Endpoint":"127.0.0.1:9000","KeyPrefix":"server/data","IsMinio":true}}
new object storage {"sdk": "minio", "arguments": {"Name":"ETL","Bucket":"mo-test","Endpoint":"127.0.0.1:9000","KeyPrefix":"server/etl","IsMinio":true}}
```

远程 checkpoint ready：

```bash
./mo-tool ckp dump --table-id=2 \
  --fs-config etc/launch-minio-local/tn.toml \
  --fs-name SHARED \
  -o /tmp/minio_mo_tables_probe.csv
```

结果：

```text
2008 /tmp/minio_mo_tables_probe.csv
```

`ckp info` 验证了两条远程打开路径：

```bash
./mo-tool ckp info \
  --fs-config etc/launch-minio-local/tn.toml \
  --fs-name SHARED

./mo-tool ckp info \
  --backend MINIO \
  --s3 bucket=mo-test,endpoint=http://127.0.0.1:9000,key-prefix=server/data,key-id=minio,key-secret=minio123
```

结果均为：

```text
Total Entries: 2
  Global:      0
  Incremental: 2
Latest TS:     1781142243226306710-0
```

用户表 dump：

```bash
./mo-tool ckp dump --table-id=272535 --header \
  --fs-config etc/launch-minio-local/tn.toml \
  --fs-name SHARED \
  -o /tmp/minio_employees_dump.csv
```

结果：

```text
104 /tmp/minio_employees_dump.csv
id,name,age,salary,department,hire_date,is_active
```

`--s3` 直连路径也通过，且与 `--fs-config` 输出一致：

```bash
./mo-tool ckp dump --table-id=272535 --header \
  --backend MINIO \
  --s3 bucket=mo-test,endpoint=http://127.0.0.1:9000,key-prefix=server/data,key-id=minio,key-secret=minio123 \
  -o /tmp/minio_employees_dump_s3.csv

cmp -s /tmp/minio_employees_dump.csv /tmp/minio_employees_dump_s3.csv
```

结果：

```text
104 /tmp/minio_employees_dump_s3.csv
cmp_exit=0
```

`show-create-table`：

```bash
./mo-tool ckp show-create-table --table-id=272535 \
  --fs-config etc/launch-minio-local/tn.toml \
  --fs-name SHARED
```

结果正确返回原始 DDL：

```sql
CREATE TABLE employees_minio (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    salary DECIMAL(10,2),
    department VARCHAR(50),
    hire_date DATE,
    is_active BOOL
)
```

LOAD DATA 校验：

```text
original_rows loaded_rows
103           103

diff_rows
0
```

结论：

- `mo-tool ckp info/dump/show-create-table` 通过 MinIO 远程 fileservice 可用。
- `--fs-config ... --fs-name SHARED` 和 `--backend MINIO --s3 ...` 两条路径均可读取 checkpoint。
- 未再出现 `checksum not match`。
- 从 MinIO checkpoint 导出的 CSV 可直接 `LOAD DATA` 回灌，行数和差异校验通过。

### ALTER TABLE 后新旧数据兼容

测试过程：

1. 创建旧布局表并插入 3 行：

```sql
CREATE TABLE test_ckp.alter_compat (
  id INT PRIMARY KEY,
  name VARCHAR(50)
);
INSERT INTO alter_compat VALUES (1, 'old_a'), (2, 'old_b'), (3, 'old_c');
```

2. 等待旧 table_id `272537` 能从 checkpoint 导出：

```text
id,name
1,"old_a"
2,"old_b"
3,"old_c"
```

3. 执行 ALTER 并插入新布局数据：

```sql
ALTER TABLE alter_compat ADD COLUMN note VARCHAR(50) DEFAULT 'legacy';
INSERT INTO alter_compat VALUES (4, 'new_d', 'fresh'), (5, 'new_e', NULL);
```

4. ALTER 后 MatrixOne 将当前 catalog table_id 切到 `272538`：

```text
rel_id relname
272538 alter_compat
```

用旧 table_id `272537` 导出会失败，因为当前 catalog metadata 已经不再暴露旧 table_id 的 `mo_columns` 可见列；这是预期边界。应使用 ALTER 后的当前 table_id `272538` 导出。

导出 `272538` 结果：

```text
id,name,note
1,"old_a","legacy"
2,"old_b","legacy"
3,"old_c","legacy"
4,"new_d","fresh"
5,"new_e",\N
```

LOAD DATA 校验：

```text
original_rows loaded_rows
5             5

diff_rows
0
```

兼容性结论：

- ALTER 前旧对象和 ALTER 后新对象在当前 table_id `272538` 的 checkpoint 视图下可一起导出。
- 新增列默认值已在导出结果中物化，旧行 `note='legacy'`。
- 使用 ALTER 前旧 table_id 导出当前表不是支持目标；旧 ID 在 catalog 中已被新 ID 替代，schema 解析会失败。

## catalog/checkpoint 版本兼容性结论

当前实现支持以下 catalog layout：

| Layout | 适用场景 | 处理方式 |
|--------|----------|----------|
| `current` | 当前源码中的 `catalog.MoTablesSchema` / `catalog.MoColumnsSchema` | 直接按当前 schema 或 header 名解析 |
| `pre-cpk` | checkpoint 中 catalog 尚未包含 `CPKey`/`ExtraInfo` 的布局 | 移除这些字段后按候选布局试读 |
| `3.0-dev` | 已知 3.0 开发期尾部字段较少的布局 | 保留原兼容路径 |

重要规则：

- 如果 checkpoint logical view 提供真实列名，优先按列名解析。
- 如果只有 `col_N`，不再相信单一宽度推断；按候选 layout 和偏移逐个试读。
- dataWidth 可能包含尾部额外物理列，不能只按 `len(schema)+offset` 精确匹配。
- 未知未来 catalog layout 若无法通过候选试读解析出目标 table_id，会明确失败，不回退到物理列导出，避免把隐藏列或错列导出成用户数据。

## 推荐测试步骤

### 1. 构建

```bash
make mo-tool
```

### 2. 启动 mo-service

普通 shell 可用：

```bash
kill -9 $(pgrep mo-service) 2>/dev/null || true
rm -rf etc/launch-minio-local/mo-data
mkdir -p etc/launch-minio-local/mo-data
./mo-service -launch etc/launch-minio-local/launch.toml > /tmp/mo-service.log 2>&1 &
until mysqladmin -h 127.0.0.1 -P 6001 -u root -p111 ping >/dev/null 2>&1; do
  sleep 1
done
```

在 Codex exec 环境中建议前台运行：

```bash
./mo-service -launch etc/launch-minio-local/launch.toml
```

### 3. 等 checkpoint 可用于 dump

不要只等 `Incremental: [1-9]`。第一次 incremental 可能还没有目标表或系统表范围。更稳妥的方式是直接等系统表 dump 成功：

```bash
until ./mo-tool ckp dump --table-id=2 -o /tmp/mo_tables_probe.csv \
  etc/launch-minio-local/mo-data >/dev/null 2>&1; do
  sleep 5
done
```

### 4. 导出并回灌

```bash
TABLE_ID=272535

./mo-tool ckp dump --table-id=${TABLE_ID} --header \
  -o /tmp/employees_dump.csv \
  etc/launch-minio-local/mo-data

wc -l /tmp/employees_dump.csv
head -5 /tmp/employees_dump.csv
```

```sql
CREATE TABLE test_ckp.employees_load (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    salary DECIMAL(10,2),
    department VARCHAR(50),
    hire_date DATE,
    is_active BOOL
);

LOAD DATA INFILE '/tmp/employees_dump.csv'
INTO TABLE test_ckp.employees_load
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SELECT (SELECT COUNT(*) FROM test_ckp.employees) AS original_rows,
       (SELECT COUNT(*) FROM test_ckp.employees_load) AS loaded_rows;

SELECT COUNT(*) FROM
  (SELECT * FROM test_ckp.employees EXCEPT SELECT * FROM test_ckp.employees_load) AS diff;
```

## 常用命令

```bash
./mo-tool ckp info etc/launch-minio-local/mo-data
./mo-tool ckp show-create-table --table-id=<TABLE_ID> etc/launch-minio-local/mo-data
./mo-tool ckp dump --table-id=<TABLE_ID> --header -o /tmp/table.csv etc/launch-minio-local/mo-data
./mo-tool ckp dump --table-id=2 -o /tmp/mo_tables.csv etc/launch-minio-local/mo-data
./mo-tool ckp dump --table-id=3 -o /tmp/mo_columns.csv etc/launch-minio-local/mo-data
```
