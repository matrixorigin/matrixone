# Checkpoint CSV Dump 工具使用文档

## 概述

`mo-tool ckp` 是从 MatrixOne checkpoint 数据离线导出 CSV 的命令行工具。支持以表、database、租户（account）为单位导出，mo-data 可以是本地目录或远程 S3/MinIO，可指定时间戳导出历史快照数据。

---

## 一、构建

```bash
make mo-tool
```

构建产物为 `./mo-tool`。

---

## 二、以表为单位 dump

### 2.1 命令

```bash
./mo-tool ckp dump --table-id=<TABLE_ID> [选项] <mo-data路径>
```

### 2.2 参数说明

| 参数 | 必需 | 说明 |
|------|------|------|
| `--table-id` | 是 | MatrixOne 内部 table_id，可从 `mo_tables` 系统表或交互式 viewer 中获取 |
| `--ts` | 否 | 快照时间戳。不指定则使用最新的 checkpoint 时间戳 |
| `-o` / `--output` | 否 | 输出路径。纯 CSV 模式指定文件路径，`--load-script` 模式指定目录（工具自动生成文件名）。不指定则输出到 stdout |
| `--header` | 否 | 在 CSV 第一行输出列名头行 |
| `--meta-comments` | 否 | 在 CSV 文件头部输出 DDL 和行数统计注释（以 `--` 开头） |
| `--row-order` | 否 | 行排序方式：`storage`（默认，流式按存储顺序）或 `lexical`（按可见列字典序排序） |
| `--load-script` | 否 | 切换为 LOAD 脚本输出模式：生成包含 CREATE DATABASE + CREATE TABLE + LOAD DATA 的 SQL 文件 |
| `--no-load` | 否 | 配合 `--load-script` 使用，跳过 LOAD DATA 语句，只输出 DDL |

### 2.3 示例

**导出到文件**：

```bash
./mo-tool ckp dump --table-id=272535 --header -o /tmp/employees.csv /path/to/mo-data
```

**导出到 stdout**：

```bash
./mo-tool ckp dump --table-id=272535 --header /path/to/mo-data
```

**指定时间戳导出**：

```bash
./mo-tool ckp dump --table-id=272535 --ts=1781084792256166694:1 --header -o /tmp/employees.csv /path/to/mo-data
```

**生成 LOAD 脚本（含建库建表 + LOAD DATA）**：

```bash
./mo-tool ckp dump --table-id=272535 --load-script -o /tmp/ /path/to/mo-data
```

**带 DDL 元数据注释导出**：

```bash
./mo-tool ckp dump --table-id=272535 --header --meta-comments -o /tmp/employees.csv /path/to/mo-data
```

此时输出文件头部会包含：

```text
-- CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(100), ...)
-- Database: test_ckp
-- Table: employees
-- Visible rows: 103 (deleted: 0, physical: 103)
id,name,age,salary,department,hire_date,is_active
...
```

### 2.4 输出位置

| 模式 | `-o` 含义 | 输出文件 |
|------|----------|---------|
| 纯 CSV（默认） | 文件路径 | `-o /tmp/employees.csv` → `/tmp/employees.csv` |
| `--load-script` | 目录路径 | `-o /tmp/` → `/tmp/restore.sql` |

未指定 `-o` 时输出到 stdout。

---

## 三、以 database 为单位 dump

### 3.1 命令

```bash
./mo-tool ckp dump --database-id=<DB_ID> --output-dir=<DIR> [选项] <mo-data路径>
```

### 3.2 参数说明

| 参数 | 必需 | 说明 |
|------|------|------|
| `--database-id` | 是 | Database ID（uint64），可从 `mo_database` 系统表或 `ckp list --type=databases` 获取 |
| `--output-dir` | 是 | 输出根目录。该目录会自动创建 |
| `--ts` | 否 | 快照时间戳，不指定则用最新 |
| `--header` | 否 | 在每个 CSV 文件第一行输出列名头行 |
| `--meta-comments` | 否 | 在每个 CSV 文件头部输出 DDL 和行数统计注释 |
| `--row-order` | 否 | 行排序方式 |
| `--load-script` | 否 | 切换为 LOAD 脚本输出模式：生成单个 SQL 文件，包含 CREATE DATABASE + 所有表的 CREATE TABLE + LOAD DATA |
| `--no-load` | 否 | 配合 `--load-script` 使用，跳过 LOAD DATA 语句，只输出 DDL |

### 3.3 示例

**导出指定 database ID 下的所有表**：

```bash
./mo-tool ckp dump --database-id=9001 --output-dir=./dump_out --header /path/to/mo-data
```

**生成该 database 的 LOAD 恢复脚本**：

```bash
./mo-tool ckp dump --database-id=9001 --load-script -o /tmp/ /path/to/mo-data
```

### 3.4 输出目录结构

以 database 为单位 dump 时，CSV 文件按以下目录结构组织：

```text
<output-dir>/
├── restore.sql                          # 如果指定了 --load-script，生成恢复脚本
└── account_<account_id>/
    └── db_<database_id>/
        ├── <table_name>_<table_id>.csv
        ├── <table_name_2>_<table_id_2>.csv
        └── ...
```

具体示例：

**dump CSV + LOAD 脚本**：

```bash
./mo-tool ckp dump --database-id=9001 --output-dir=./dump_out --header \
  --load-script -o ./dump_out/ /path/to/mo-data
```

```text
dump_out/
├── restore.sql
└── account_7/
    └── db_9001/
        ├── employees_272535.csv
        ├── departments_272536.csv
        └── alter_compat_272538.csv
```

其中 `restore.sql` 的内容：

```sql
CREATE DATABASE IF NOT EXISTS test_ckp;
USE test_ckp;

CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    ...
);

LOAD DATA INFILE 'dump_out/account_7/db_9001/employees_272535.csv'
INTO TABLE employees
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

CREATE TABLE departments (...);

LOAD DATA INFILE 'dump_out/account_7/db_9001/departments_272536.csv'
INTO TABLE departments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
```

**只 dump CSV（不含 LOAD 脚本）**：

```bash
./mo-tool ckp dump --database-id=9001 --output-dir=./dump_out --header /path/to/mo-data
```

```text
dump_out/
└── account_7/
    └── db_9001/
        ├── employees_272535.csv
        ├── departments_272536.csv
        └── alter_compat_272538.csv
```

目录命名规则：

- **account 目录**：`account_<account_id>`，其中 `account_id` 为十进制数字。系统租户的 account_id 为 `0`
- **database 目录**：`db_<database_id>`，其中 `database_id` 为十进制数字
- **CSV 文件名**：`<table_name>_<table_id>.csv`，其中 `table_name` 为表名，`table_id` 为内部数字 ID。表名中的特殊字符会被替换为下划线

> **注意**：`relkind='v'` 的视图不会被导出。

### 3.5 命令输出

执行过程中，每成功导出一个表会打印一行信息：

```text
Table 272535 test_ckp.employees dumped to dump_out/account_7/db_9001/employees_272535.csv
Table 272536 test_ckp.departments dumped to dump_out/account_7/db_9001/departments_272536.csv
Dumped 2 tables to dump_out
```

---

## 四、以租户（account）为单位 dump

### 4.1 命令

```bash
./mo-tool ckp dump --account-id=<ACCOUNT_ID> --output-dir=<DIR> [选项] <mo-data路径>
```

### 4.2 参数说明

| 参数 | 必需 | 说明 |
|------|------|------|
| `--account-id` | 是 | 租户 ID（uint32） |
| `--output-dir` | 是 | 输出根目录 |
| `--ts` | 否 | 快照时间戳 |
| `--header` | 否 | CSV 列名头行 |
| `--meta-comments` | 否 | DDL 和行数统计注释 |
| `--row-order` | 否 | 行排序方式 |
| `--load-script` | 否 | 切换为 LOAD 脚本输出模式：生成单个 SQL 文件，包含所有 database 的 CREATE DATABASE + CREATE TABLE + LOAD DATA |
| `--no-load` | 否 | 配合 `--load-script` 使用，跳过 LOAD DATA 语句，只输出 DDL |

### 4.3 示例

**导出某个租户的所有表**：

```bash
./mo-tool ckp dump --account-id=7 --output-dir=./dump_out --header /path/to/mo-data
```

**生成该租户的 LOAD 恢复脚本**：

```bash
./mo-tool ckp dump --account-id=7 --load-script -o /tmp/ /path/to/mo-data
```

### 4.4 输出目录结构

以租户为单位 dump 时，目录结构与 database 级别一致：

```text
<output-dir>/
├── restore.sql                          # 如果指定了 --load-script，生成恢复脚本
└── account_<account_id>/
    ├── db_<database_id_1>/
    │   ├── <table_name>_<table_id>.csv
    │   └── ...
    ├── db_<database_id_2>/
    │   └── ...
    └── ...
```

具体示例：

```bash
./mo-tool ckp dump --account-id=7 --output-dir=./dump_out --header \
  --load-script -o ./dump_out/ /path/to/mo-data
```

```text
dump_out/
├── restore.sql
└── account_7/
    ├── db_9001/
    │   ├── employees_272535.csv
    │   └── alter_compat_272538.csv
    └── db_9002/
        ├── orders_272540.csv
        ├── lineitem_272541.csv
        └── ...
```

`restore.sql` 包含该租户下所有 database 的建表和数据导入语句。

---

## 五、本地 mo-data

直接传入本地 mo-data 目录路径即可，适用于 checkpoint 数据存储在本地磁盘的场景。

```bash
./mo-tool ckp dump --table-id=272535 --header -o /tmp/table.csv /path/to/mo-data
```

内部使用 `fileservice.NewLocalFS` 创建本地文件服务，直接读取 checkpoint 元数据和数据对象文件。

---

## 六、远程 S3/MinIO mo-data

当 checkpoint 数据存储在 S3 或 MinIO 对象存储上时，支持两种远程访问方式。

### 6.1 方式一：通过 MO 配置文件

使用 MatrixOne 的 TOML 配置文件（如 `tn.toml`），其中的 `[fileservice]` 段包含对象存储的连接信息：

```bash
./mo-tool ckp dump --table-id=272535 --header -o /tmp/table.csv \
  --fs-config etc/launch-minio-local/tn.toml \
  --fs-name SHARED
```

- `--fs-config`：MO 配置文件路径
- `--fs-name`：使用的 fileservice 名称（默认为 `SHARED`），对应配置中 `[fileservice]` 段的 `name` 字段

### 6.2 方式二：直接指定 S3 参数

不依赖 MO 配置文件，直接传入对象存储的连接参数：

```bash
./mo-tool ckp dump --table-id=272535 --header -o /tmp/table.csv \
  --backend MINIO \
  --s3 bucket=mo-test,endpoint=http://127.0.0.1:9000,key-prefix=server/data,key-id=minio,key-secret=minio123
```

S3 参数格式（逗号分隔的 key=value 对）：

| 参数 | 必需 | 说明 |
|------|------|------|
| `bucket` | 是 | S3 bucket 名称 |
| `endpoint` | 是 | S3/MinIO API 地址 |
| `key-prefix` | 是 | mo-data 在 bucket 中的路径前缀 |
| `key-id` | 是 | 访问密钥 ID |
| `key-secret` | 是 | 访问密钥 Secret |
| `region` | 否 | S3 region |

`--backend` 可选值：
- `S3`：AWS S3 或兼容 S3 协议的服务（默认）
- `MINIO`：MinIO 对象存储

### 6.3 远程读取机制

远程读取使用 `lazyCacheFS` 实现按需缓存：

1. 首次访问远程文件时，下载到本地临时目录
2. 后续读取同一文件直接命中本地缓存
3. 绕过 MO 内部的 checksum 格式验证，直接按 OS file range 读取
4. 工具退出时自动清理临时缓存目录

checkpoint 元数据列表（`List` 操作）直接对远程服务执行，不下载全部文件。

### 6.4 远程 dump 示例

```bash
# 远程单表 dump
./mo-tool ckp dump --table-id=272535 --header -o /tmp/remote_table.csv \
  --fs-config etc/launch-minio-local/tn.toml \
  --fs-name SHARED

# 远程 database dump
./mo-tool ckp dump --database-id=9001 --output-dir=./remote_dump --header \
  --fs-config etc/launch-minio-local/tn.toml

# 远程 ckp info
./mo-tool ckp info --fs-config etc/launch-minio-local/tn.toml --fs-name SHARED
```

---

## 七、CSV 格式说明

### 7.1 格式规范

导出的 CSV 文件兼容 MySQL/MariaDB/MatrixOne 的 `LOAD DATA` 语句，具体格式：

| 特性 | 约定 |
|------|------|
| 字段分隔符 | `,`（逗号） |
| 字段引用符 | `"`（双引号） |
| 转义符 | `\`（反斜杠） |
| 行分隔符 | `\n`（换行） |
| NULL 表示 | `\N`（反斜杠+大写 N） |
| 字符串类型 | 总是用双引号括起（VARCHAR, CHAR, TEXT, BLOB, JSON, GEOMETRY, DATALINK 等） |
| 数值类型 | 不使用引号（INT, BIGINT, FLOAT, DOUBLE, DECIMAL, BOOL 等） |
| 日期时间类型 | 不使用引号（DATE, TIME, DATETIME, TIMESTAMP） |

### 7.2 特殊值处理

| 场景 | CSV 输出 |
|------|----------|
| NULL 值 | `\N` |
| 包含双引号的字符串（如 `a"b`） | `"a""b"` |
| 包含反斜杠的字符串（如 `a\b`） | `"a\\b"` |
| 包含逗号的字符串（如 `a,b`） | `"a,b"` |
| 包含换行的字符串 | `"a\nb"` |

### 7.3 CSV 输出示例

```csv
id,name,age,salary,department,hire_date,is_active
1,"Alice",30,75000.00,"Engineering",2020-01-15,true
2,"Bob",\N,\N,\N,\N,\N
3,"Charlie",\N,65000.00,\N,2021-03-10,false
4,"David""The Boss""",42,120000.00,"Management",2019-06-01,true
```

---

## 八、LOAD DATA 回灌

### 8.1 推荐方式：使用 `--load-script`

`--load-script` 一步生成完整的 SQL 恢复脚本（CREATE DATABASE + CREATE TABLE + LOAD DATA），在目标 MO 实例中直接执行即可：

```bash
# 生成恢复脚本
./mo-tool ckp dump --database-id=9001 --load-script -o /tmp/ /path/to/mo-data

# 在目标 MO 实例中执行
mysql -h 127.0.0.1 -P 6001 -u root -p111 < /tmp/restore.sql
```

详见第十一章。

### 8.2 手动回灌（不使用 `--load-script`）

如果不使用 `--load-script`，手动回灌流程如下：

**步骤 1：建表**

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
```

**步骤 2：LOAD DATA**

```sql
LOAD DATA INFILE '/tmp/employees.csv'
INTO TABLE test_ckp.employees_load
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;  -- 如果 dump 时使用了 --header
```

LOAD DATA 参数对应关系：

| CSV 格式特性 | LOAD DATA 参数 |
|-------------|---------------|
| 逗号分隔字段 | `FIELDS TERMINATED BY ','` |
| 双引号括起字符串 | `ENCLOSED BY '"'` |
| 反斜杠转义 | 默认（MySQL/MO 默认转义符为 `\`） |
| 换行分隔 | `LINES TERMINATED BY '\n'` |
| NULL 为 `\N` | 默认（MySQL/MO 默认 NULL 表示即 `\N`） |
| 有 header 行 | `IGNORE 1 LINES` |
| 无 header 行 | 不需要 `IGNORE 1 LINES` |

### 8.3 数据校验

```sql
-- 对比原始行数和导入行数
SELECT (SELECT COUNT(*) FROM test_ckp.employees) AS original_rows,
       (SELECT COUNT(*) FROM test_ckp.employees_load) AS loaded_rows;

-- 对比数据差异
SELECT COUNT(*) AS diff_rows
FROM (
    SELECT * FROM test_ckp.employees
    EXCEPT
    SELECT * FROM test_ckp.employees_load
) AS diff;
```

---

## 九、CSV 行排序选项

### 9.1 storage 顺序（默认）

```bash
./mo-tool ckp dump --table-id=272535 --row-order=storage --header -o /tmp/table.csv /path/to/mo-data
```

- 按物理存储顺序输出，流式写入
- 内存占用小，适合大表
- 行顺序与存储布局一致，不可预测

### 9.2 lexical 顺序

```bash
./mo-tool ckp dump --table-id=272535 --row-order=lexical --header -o /tmp/table.csv /path/to/mo-data
```

- 按所有可见列的字典序排序后输出
- 需要将全部行加载到内存中排序
- 适合需要确定性输出顺序的场景
- 不适合超大表

---

## 十、交互式浏览器

除了命令行 dump，`mo-tool ckp` 还提供了交互式 TUI 浏览器，用于探索 checkpoint 内容：

```bash
./mo-tool ckp view /path/to/mo-data
```

交互式浏览器支持以下操作：

- **浏览 checkpoint 列表**：查看所有 GCKP/ICKP 条目及其时间范围
- **浏览表列表**：选择一个 checkpoint 条目后，查看其中包含的所有表（可按 account 筛选）
- **浏览对象详情**：选择一个表后，查看其 data/tombstone 对象列表
- **浏览逻辑表视图**：查看 tombstone 过滤后的实际数据行
- **打开对象文件**：深入到单个数据对象中查看原始内容

远程 checkpoint 也可使用交互式浏览器：

```bash
./mo-tool ckp view --fs-config etc/launch-minio-local/tn.toml --fs-name SHARED
```

---

## 十一、checkpoint 信息查看

### 11.1 查看 checkpoint 摘要

```bash
./mo-tool ckp info /path/to/mo-data
```

输出 checkpoint 的条目类型统计和时间范围。

### 11.2 生成 LOAD 脚本（`--load-script`）

dump 命令支持 `--load-script` 选项，生成一个可直接在目标 MatrixOne 实例中执行的 SQL 脚本，一行命令即可完成数据恢复。

脚本包含以下内容（按顺序）：

1. **CREATE DATABASE** — 重建 database
2. **USE database** — 切换到目标 database
3. **CREATE TABLE**（每个表一条） — 重建表结构，DDL 从 checkpoint 的 `mo_tables.rel_createsql` 解析
4. **LOAD DATA**（每个 CSV 文件一条，可选）— 导入数据

#### 单表生成 LOAD 脚本

```bash
./mo-tool ckp dump --table-id=272535 --load-script -o /tmp/ /path/to/mo-data
```

生成的 `restore.sql` 示例：

```sql
CREATE DATABASE IF NOT EXISTS test_ckp;
USE test_ckp;

CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    salary DECIMAL(10,2),
    department VARCHAR(50),
    hire_date DATE,
    is_active BOOL
);

LOAD DATA INFILE '/tmp/employees_272535.csv'
INTO TABLE employees
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
```

#### database 级别生成 LOAD 脚本

```bash
./mo-tool ckp dump --database-id=9001 --load-script -o /tmp/ /path/to/mo-data
```

生成的脚本包含该 database 下所有表的 DDL 和 LOAD DATA 语句。CSV 文件输出到脚本同级目录。

#### 租户级别生成 LOAD 脚本

```bash
./mo-tool ckp dump --account-id=7 --load-script -o /tmp/ /path/to/mo-data
```

生成的脚本包含该租户下所有 database 和表的 DDL 及 LOAD DATA 语句。

#### LOAD DATA 可选

```bash
# 只生成 DDL，不含 LOAD DATA（仅建库建表）
./mo-tool ckp dump --database-id=9001 --load-script --no-load -o /tmp/ /path/to/mo-data
```

`--no-load` 跳过 LOAD DATA 语句，只输出 CREATE DATABASE + CREATE TABLE。

---

## 十二、常用命令速查

```bash
# 构建
make mo-tool

# 查看 checkpoint 摘要
./mo-tool ckp info /path/to/mo-data

# 单表 dump 到文件
./mo-tool ckp dump --table-id=272535 --header -o /tmp/table.csv /path/to/mo-data

# 单表 dump 到 stdout
./mo-tool ckp dump --table-id=272535 --header /path/to/mo-data

# 指定时间戳单表 dump
./mo-tool ckp dump --table-id=272535 --ts=1781084792256166694:1 --header -o /tmp/table.csv /path/to/mo-data

# database 级别批量 dump
./mo-tool ckp dump --database-id=9001 --output-dir=./dump_out --header /path/to/mo-data

# 租户级别批量 dump
./mo-tool ckp dump --account-id=7 --output-dir=./dump_out --header /path/to/mo-data

# 生成 LOAD 脚本（单表/database/租户）
./mo-tool ckp dump --table-id=272535 --load-script -o /tmp/ /path/to/mo-data
./mo-tool ckp dump --database-id=9001 --load-script -o /tmp/ /path/to/mo-data
./mo-tool ckp dump --account-id=7 --load-script -o /tmp/ /path/to/mo-data
./mo-tool ckp dump --database-id=9001 --load-script --no-load -o /tmp/ /path/to/mo-data

# dump 系统表
./mo-tool ckp dump --table-id=2 -o /tmp/mo_tables.csv /path/to/mo-data    # mo_tables
./mo-tool ckp dump --table-id=3 -o /tmp/mo_columns.csv /path/to/mo-data   # mo_columns

# 远程 S3/MinIO dump（方式一：配置文件）
./mo-tool ckp dump --table-id=272535 --header -o /tmp/table.csv \
  --fs-config etc/launch-minio-local/tn.toml --fs-name SHARED

# 远程 S3/MinIO dump（方式二：直接指定参数）
./mo-tool ckp dump --table-id=272535 --header -o /tmp/table.csv \
  --backend MINIO \
  --s3 bucket=mo-test,endpoint=http://127.0.0.1:9000,key-prefix=server/data,key-id=minio,key-secret=minio123

# 交互式浏览器
./mo-tool ckp view /path/to/mo-data
```

---

## 十三、恢复流程模板

### 推荐方式：`--load-script` 一键恢复

```bash
# 1. 从 checkpoint 生成恢复脚本
./mo-tool ckp dump --database-id=<DB_ID> --load-script --header -o ./ /path/to/mo-data

# 2. 在目标 MO 实例执行
mysql -h <host> -P <port> -u <user> -p<password> < restore.sql
```

生成的 `restore.sql` 结构：

```sql
CREATE DATABASE IF NOT EXISTS <db_name>;
USE <db_name>;

CREATE TABLE <table_1> (...);
LOAD DATA INFILE '<csv_path_1>' INTO TABLE <table_1> ...;

CREATE TABLE <table_2> (...);
LOAD DATA INFILE '<csv_path_2>' INTO TABLE <table_2> ...;
```

### 手动方式

```sql
-- 建表
CREATE TABLE <db>.<table> (
    ...
);

-- 导入 CSV（dump 时使用了 --header）
LOAD DATA INFILE '<csv_file_path>'
INTO TABLE <db>.<table>
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;
```
