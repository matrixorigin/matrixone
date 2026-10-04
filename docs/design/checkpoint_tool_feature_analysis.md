# Checkpoint CSV Dump 工具使用文档

## 概述

`mo-tool ckp` 是从 MatrixOne checkpoint 数据离线导出 CSV 的命令行工具。支持以表、database、租户（account）为单位导出，mo-data 可以是本地目录或远程 S3/MinIO，可指定时间戳导出历史快照数据。

---

## 一、构建

```bash
make mo-tool
```

构建产物为 `./mo-tool`。

### 1.1 构建注意事项

- **从 git worktree 构建**：如果从 `git worktree add --detach` 创建的 detached HEAD 状态构建，需要先在 Makefile 中给 `go build` 加上 `-buildvcs=false`，否则会报 `error obtaining VCS status: exit status 128`。具体修改：在 Makefile 的 `build` 目标中将 `go build` 改为 `go build -buildvcs=false`。
- **构建 mo-service**：`make build` 生成 `./mo-service`，`make debug` 生成 race-detector 版本。

---

## 二、ckp list — 浏览 checkpoint 目录

`ckp list` 用于从 checkpoint 中列出数据库、表、租户等元数据，是 dump 前的关键查询命令。

### 2.1 命令

```bash
./mo-tool ckp list [directory] [flags]
```

### 2.2 参数说明

| 参数 | 必需 | 说明 |
|------|------|------|
| `--type` | 否 | 列出类型：`tables`（默认）、`databases` 或 `accounts` |
| `--account-id` | 否 | 按租户 ID 过滤 |
| `--database-id` | 否 | 按数据库 ID 过滤 |
| `--ts` | 否 | 快照时间戳，默认使用 latest |
| `--include-views` | 否 | 包含视图（默认只列普通表） |

### 2.3 示例

```bash
# 列出所有数据库
./mo-tool ckp list /path/to/mo-data/shared --type databases

# 列出指定数据库下的所有表
./mo-tool ckp list /path/to/mo-data/shared --database-id 272528

# 列出所有租户
./mo-tool ckp list /path/to/mo-data/shared --type accounts

# 远程 S3/MinIO 下列出
./mo-tool ckp list --fs-config etc/launch-minio-local/tn.toml --type databases
```

---

### 2.4 ckp show-create-table — 从 checkpoint 获取 DDL

从 checkpoint 中获取指定表的完整 CREATE TABLE DDL，无需连接数据库实例即可查看表结构。

```bash
./mo-tool ckp show-create-table /path/to/mo-data/shared --table-id 272537
```

输出示例：

```sql
CREATE TABLE `t_all_wide` (
  `id` INT NOT NULL,
  `c_tiny` TINYINT DEFAULT NULL,
  ...
  PRIMARY KEY (`id`)
);
```

支持远程访问：`--fs-config` / `--s3` 参数与 `dump` 命令相同。

---

## 三、以表为单位 dump

### 3.1 命令

```bash
./mo-tool ckp dump --table-id=<TABLE_ID> [选项] <mo-data路径>
```

### 3.2 参数说明

| 参数 | 必需 | 说明 |
|------|------|------|
| `--table-id` | 是 | MatrixOne 内部 table_id，可从 `mo_tables` 系统表或 `ckp list` 中获取 |
| `--ts` | 否 | 快照时间戳。不指定则使用最新的 checkpoint 时间戳。支持格式：`physical:logical`（如 `1781084792256166694:1`）、`physical-logical`、RFC3339、或本地时间格式 |
| `-o` / `--output` | 否 | 输出路径。纯 CSV 模式指定文件路径，`--load-script` 模式指定目录（工具自动生成文件名）。不指定则输出到 stdout |
| `--header` | 否 | 在 CSV 第一行输出列名头行 |
| `--meta-comments` | 否 | 在 CSV 文件头部输出 DDL 和行数统计注释（以 `--` 开头） |
| `--row-order` | 否 | 行排序方式：`storage`（默认，流式按存储顺序）或 `lexical`（按可见列字典序排序） |
| `--load-script` | 否 | 切换为 LOAD 脚本输出模式：生成包含 CREATE DATABASE + CREATE TABLE + LOAD DATA 的 SQL 文件 |
| `--no-load` | 否 | 配合 `--load-script` 使用，跳过 LOAD DATA 语句，只输出 DDL |

### 3.3 示例

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

### 3.4 输出位置

| 模式 | `-o` 含义 | 输出文件 |
|------|----------|---------|
| 纯 CSV（默认） | 文件路径 | `-o /tmp/employees.csv` → `/tmp/employees.csv` |
| `--load-script` | 目录路径 | `-o /tmp/` → `/tmp/restore.sql` |

未指定 `-o` 时输出到 stdout。

---

## 四、以 database 为单位 dump

### 4.1 命令

```bash
./mo-tool ckp dump --database-id=<DB_ID> --output-dir=<DIR> [选项] <mo-data路径>
```

### 4.2 参数说明

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

### 4.3 示例

**导出指定 database ID 下的所有表**：

```bash
./mo-tool ckp dump --database-id=9001 --output-dir=./dump_out --header /path/to/mo-data
```

**生成该 database 的 LOAD 恢复脚本**：

```bash
./mo-tool ckp dump --database-id=9001 --load-script -o /tmp/ /path/to/mo-data
```

### 4.4 输出目录结构

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

### 4.5 命令输出

执行过程中，每成功导出一个表会打印一行信息：

```text
Table 272535 test_ckp.employees dumped to dump_out/account_7/db_9001/employees_272535.csv
Table 272536 test_ckp.departments dumped to dump_out/account_7/db_9001/departments_272536.csv
Dumped 2 tables to dump_out
```

---

## 五、以租户（account）为单位 dump

### 5.1 命令

```bash
./mo-tool ckp dump --account-id=<ACCOUNT_ID> --output-dir=<DIR> [选项] <mo-data路径>
```

### 5.2 参数说明

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

### 5.3 示例

**导出某个租户的所有表**：

```bash
./mo-tool ckp dump --account-id=7 --output-dir=./dump_out --header /path/to/mo-data
```

**生成该租户的 LOAD 恢复脚本**：

```bash
./mo-tool ckp dump --account-id=7 --load-script -o /tmp/ /path/to/mo-data
```

### 5.4 输出目录结构

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

## 六、本地 mo-data

直接传入本地 mo-data 目录路径即可，适用于 checkpoint 数据存储在本地磁盘的场景。

```bash
./mo-tool ckp dump --table-id=272535 --header -o /tmp/table.csv /path/to/mo-data
```

内部使用 `fileservice.NewLocalFS` 创建本地文件服务，直接读取 checkpoint 元数据和数据对象文件。

---

## 七、远程 S3/MinIO mo-data

当 checkpoint 数据存储在 S3 或 MinIO 对象存储上时，支持两种远程访问方式。

### 7.1 方式一：通过 MO 配置文件

使用 MatrixOne 的 TOML 配置文件（如 `tn.toml`），其中的 `[fileservice]` 段包含对象存储的连接信息：

```bash
./mo-tool ckp dump --table-id=272535 --header -o /tmp/table.csv \
  --fs-config etc/launch-minio-local/tn.toml \
  --fs-name SHARED
```

- `--fs-config`：MO 配置文件路径
- `--fs-name`：使用的 fileservice 名称（默认为 `SHARED`），对应配置中 `[fileservice]` 段的 `name` 字段

### 7.2 方式二：直接指定 S3 参数

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

### 7.3 远程读取机制

远程读取使用 `lazyCacheFS` 实现按需缓存：

1. 首次访问远程文件时，下载到本地临时目录
2. 后续读取同一文件直接命中本地缓存
3. 绕过 MO 内部的 checksum 格式验证，直接按 OS file range 读取
4. 工具退出时自动清理临时缓存目录

checkpoint 元数据列表（`List` 操作）直接对远程服务执行，不下载全部文件。

### 7.4 远程 dump 示例

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

### 7.5 远程访问已知限制

- **MINIO backend 数据文件不完整**：当 MatrixOne 以 MINIO 作为 SHARED fileservice 运行时，checkpoint 元数据文件（`ckp/meta_*.ckp`）正常写入，但部分数据对象文件可能因缓存策略未完全 flush 到 MINIO，导致 dump 时出现 `file is not found` 错误。
- **建议**：对于本地开发/测试场景，使用 DISK backend（见下文 十五、checkpoint 配置建议），将 SHARED 改为本地目录，确保所有数据文件可被 mo-tool 直接读取。
- **S3 region 参数**：使用 `--backend MINIO` 时必须显式指定 `region`，否则报 `Invalid region` 错误。例如 `region=us-east-1`。

---

## 八、CSV 格式说明

### 8.1 格式规范

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

### 8.2 特殊值处理

| 场景 | CSV 输出 |
|------|----------|
| NULL 值 | `\N` |
| 包含双引号的字符串（如 `a"b`） | `"a""b"` |
| 包含反斜杠的字符串（如 `a\b`） | `"a\\b"` |
| 包含逗号的字符串（如 `a,b`） | `"a,b"` |
| 包含换行的字符串 | `"a\nb"` |

### 8.3 CSV 输出示例

```csv
id,name,age,salary,department,hire_date,is_active
1,"Alice",30,75000.00,"Engineering",2020-01-15,true
2,"Bob",\N,\N,\N,\N,\N
3,"Charlie",\N,65000.00,\N,2021-03-10,false
4,"David""The Boss""",42,120000.00,"Management",2019-06-01,true
```

---

## 九、LOAD DATA 回灌

### 9.1 推荐方式：使用 `--load-script`

`--load-script` 一步生成完整的 SQL 恢复脚本（CREATE DATABASE + CREATE TABLE + LOAD DATA），在目标 MO 实例中直接执行即可：

```bash
# 生成恢复脚本
./mo-tool ckp dump --database-id=9001 --load-script -o /tmp/ /path/to/mo-data

# 在目标 MO 实例中执行
mysql -h 127.0.0.1 -P 6001 -u root -p111 < /tmp/restore.sql
```

详见第十一章。

### 9.2 手动回灌（不使用 `--load-script`）

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

### 9.3 数据校验

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

## 十、CSV 行排序选项

### 10.1 storage 顺序（默认）

```bash
./mo-tool ckp dump --table-id=272535 --row-order=storage --header -o /tmp/table.csv /path/to/mo-data
```

- 按物理存储顺序输出，流式写入
- 内存占用小，适合大表
- 行顺序与存储布局一致，不可预测

### 10.2 lexical 顺序

```bash
./mo-tool ckp dump --table-id=272535 --row-order=lexical --header -o /tmp/table.csv /path/to/mo-data
```

- 按所有可见列的字典序排序后输出
- 需要将全部行加载到内存中排序
- 适合需要确定性输出顺序的场景
- 不适合超大表

---

## 十一、交互式浏览器

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

## 十二、checkpoint 信息查看

### 12.1 查看 checkpoint 摘要

```bash
./mo-tool ckp info /path/to/mo-data
```

输出 checkpoint 的条目类型统计和时间范围。

### 12.2 生成 LOAD 脚本（`--load-script`）

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

## 十三、常用命令速查

```bash
# 构建
make mo-tool

# === ckp info ===
# 查看 checkpoint 摘要
./mo-tool ckp info /path/to/mo-data/shared
./mo-tool ckp info --fs-config etc/launch-minio-local/tn.toml   # 远程

# === ckp list ===
# 列出所有数据库
./mo-tool ckp list /path/to/mo-data/shared --type databases

# 列出指定数据库的表
./mo-tool ckp list /path/to/mo-data/shared --database-id <DB_ID>

# 列出所有租户
./mo-tool ckp list /path/to/mo-data/shared --type accounts

# 远程列出
./mo-tool ckp list --fs-config etc/launch-minio-local/tn.toml --type databases

# === ckp show-create-table ===
# 查看 checkpoint 中表的 DDL
./mo-tool ckp show-create-table /path/to/mo-data/shared --table-id <TABLE_ID>

# === ckp dump ===
# 单表 dump 到文件
./mo-tool ckp dump --table-id=272535 --header -o /tmp/table.csv /path/to/mo-data/shared

# 单表 dump 到 stdout
./mo-tool ckp dump --table-id=272535 --header /path/to/mo-data/shared

# 指定时间戳单表 dump
./mo-tool ckp dump --table-id=272535 --ts=1781084792256166694:1 --header -o /tmp/table.csv /path/to/mo-data/shared

# database 级别批量 dump
./mo-tool ckp dump --database-id=9001 --output-dir=./dump_out --header /path/to/mo-data/shared

# 租户级别批量 dump
./mo-tool ckp dump --account-id=7 --output-dir=./dump_out --header /path/to/mo-data/shared

# 生成 LOAD 脚本（单表/database/租户）
./mo-tool ckp dump --table-id=272535 --load-script -o /tmp/ /path/to/mo-data/shared
./mo-tool ckp dump --database-id=9001 --load-script -o /tmp/ /path/to/mo-data/shared
./mo-tool ckp dump --account-id=7 --load-script -o /tmp/ /path/to/mo-data/shared
./mo-tool ckp dump --database-id=9001 --load-script --no-load -o /tmp/ /path/to/mo-data/shared

# dump 系统表
./mo-tool ckp dump --table-id=2 -o /tmp/mo_tables.csv /path/to/mo-data/shared    # mo_tables
./mo-tool ckp dump --table-id=3 -o /tmp/mo_columns.csv /path/to/mo-data/shared   # mo_columns

# 远程 S3/MinIO dump（方式一：配置文件）
./mo-tool ckp dump --table-id=272535 --header -o /tmp/table.csv \
  --fs-config etc/launch-minio-local/tn.toml --fs-name SHARED

# 远程 S3/MinIO dump（方式二：直接指定参数）
./mo-tool ckp dump --table-id=272535 --header -o /tmp/table.csv \
  --backend MINIO \
  --s3 bucket=mo-test,endpoint=http://127.0.0.1:9000,region=us-east-1,key-prefix=server/data,key-id=minio,key-secret=minio123

# 交互式浏览器
./mo-tool ckp view /path/to/mo-data/shared

# === 测试数据准备 ===
./prepare_ckp_dump_coverage_data.sh --host 127.0.0.1 --port 6001 --user dump --password 111 --db-prefix ckp --drop-existing
```

---

## 十四、恢复流程模板

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

---

## 十五、测试数据准备

### 15.1 prepare_ckp_dump_coverage_data.sh

仓库根目录下的 `prepare_ckp_dump_coverage_data.sh` 脚本用于快速生成 checkpoint dump 的覆盖测试数据。

**功能**：创建 4 个精心设计的测试 database：

| Database | 内容 |
|---|---|
| `<prefix>_types` | 各种数据类型及边界值（有符号/无符号整数、浮点/定点数、bool/bit、字符串/JSON、binary/blob、时间类型、enum/set、uuid、vecf32、array、datalink） |
| `<prefix>_constraints` | 约束场景（外键 RESTRICT/CASCADE/SET NULL、自增列、复合主键/唯一键、特殊标识符、全文索引、向量索引） |
| `<prefix>_tables` | 表形态（普通表、空表、view、CTAS、LIKE、hash 分区、key 分区、cluster by、特殊表名） |
| `<prefix>_mvcc_perf` | DML 历史（insert/update/delete）、truncate、alter add column、大规模行表（默认 10000 行）、32 列宽表 |

**用法**：

```bash
# 默认时间戳命名（每次生成不同的 database 名）
./prepare_ckp_dump_coverage_data.sh --host 127.0.0.1 --port 6001 --user dump --password 111

# 固定 database 名前缀 + 大规模数据，便于反复测试
./prepare_ckp_dump_coverage_data.sh \
  --host 127.0.0.1 --port 6001 \
  --user dump --password 111 \
  --db-prefix ckp \
  --scale 10000 \
  --drop-existing
```

**主要参数**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--db-prefix` | `ckp_cov_<timestamp>` | database 名前缀 |
| `--scale` | `10000`（最大 `1000000`） | 大规模表的行数 |
| `--drop-existing` | false | 先删除同名 database |
| `--generate-only` | false | 只生成 SQL 文件，不连接数据库执行 |
| `--out-dir` | `/tmp/ckp_dump_coverage_<ts>` | SQL 文件输出目录 |

### 15.2 已知兼容性问题

不同 MO 版本对 SQL 的支持不同，以下语法可能在特定版本上失败（脚本中 `/11_types_optional.sql` 和 `/21_indexes_optional.sql` 以 `--force` 模式执行，失败不中断）：

| 语法 | 说明 |
|------|------|
| `ARRAY(VARCHAR(20))` | 3.0-dev 不支持 ARRAY 类型 |
| `LEAST()` | 3.0-dev 不支持 LEAST 函数 |
| `TEMPORARY TABLE` | 3.0-dev 不支持临时表 |
| `CHAR(n)` 函数 | 3.0-dev 不支持 `CHAR()` 函数，需用 `UNHEX()` 替代 |
| `COUNT(*) AS rows` | `rows` 是 3.0-dev 保留关键字，需改别名 |

---

## 十六、checkpoint 配置建议

MO 的 checkpoint 行为由 TN 配置中的 `[tn.Ckp]` 段控制。**测试时**，默认配置可能导致 checkpoint 生成过慢，建议使用以下激进配置：

```toml
# 默认配置（checkpoint 生成慢，不适合测试）
[tn.Ckp]
flush-interval = "60s"
min-count = 100
scan-interval = "5s"
incremental-interval = "180s"
global-min-count = 60

# 测试推荐配置（checkpoint 快速生成）
[tn.Ckp]
flush-interval = "10s"
min-count = 1
scan-interval = "2s"
incremental-interval = "30s"
global-min-count = 1
```

**关键参数说明**：

| 参数 | 说明 |
|------|------|
| `flush-interval` | checkpoint flush 间隔 |
| `min-count` | 触发 flush 的最小 log entry 数量 |
| `incremental-interval` | 增量 checkpoint 间隔 |
| `global-min-count` | 触发全局 checkpoint 的最小 log entry 数量。全局 checkpoint 包含完整的 catalog 快照（mo_tables/mo_columns），是 mo-tool dump 数据的前提 |

**触发 checkpoint 的方式**：
- 自动触发：满足时间或 log count 阈值
- 关闭触发：优雅关闭 MO（`SIGTERM`）会触发一次 shutdown checkpoint，将未持久化的数据写入 checkpoint

**SHARED fileservice backend 选择**：

| backend | 适用场景 | 说明 |
|---------|---------|------|
| `DISK` | 本地开发/测试 | mo-data 直接存储为本地文件，mo-tool 无需额外配置即可读取 |
| `MINIO` / `S3` | 生产环境 | mo-data 存储在对象存储上，需通过 `--fs-config` 或 `--s3` 参数访问（见 七、远程 S3/MinIO mo-data） |

本地测试推荐使用 DISK backend 的配置：

```toml
[[fileservice]]
backend = "DISK"
data-dir = "./etc/launch-minio-local/mo-data/shared"
name = "SHARED"
```

---

## 十七、版本兼容性

### 17.1 view_ckp mo-tool 读取 3.0-dev checkpoint

| 功能 | 状态 | 说明 |
|------|------|------|
| `ckp info` | ✅ | checkpoint 条目统计和时间范围正常 |
| `ckp list --type databases` | ✅ | 系统和用户 database 全部列出 |
| `ckp list --type tables` | ✅ | 表元数据完整 |
| `ckp show-create-table` | ✅ | DDL 正确还原 |
| `ckp dump`（catalog 表） | ✅ | mo_tables/mo_columns 等系统表 dump 正常 |
| `ckp dump`（用户表） | ⚠️ | CSV header 正常生成，但数据行可能为空（取决于 checkpoint 是否完整包含数据对象） |
| `ckp dump --load-script` | ⚠️ | DDL 部分正常，数据导入可能无数据 |

### 17.2 数据完整性检查

如果 dump 用户表得到 0 行数据（`visible_rows=0`），请检查：

1. **checkpoint 是否包含数据对象**：在 `ckp info` 中确认 `Global ≥ 1`。如果只有 Incremental 条目且缺少对应的 Global checkpoint，数据对象可能未关联到 catalog。
2. **checkpoint 时间戳**：使用 `--ts` 指定更早或更晚的 checkpoint 时间戳试试。
3. **mo-data 文件完整性**：检查 `shared` 目录下是否有与 checkpoint meta 文件中引用的 UUID 对应的数据文件。
4. **SHARED backend**：使用 MINIO 时可能存在缓存未 flush 问题，建议切换到 DISK backend（见 十六、checkpoint 配置建议）。

### 17.3 完整测试流程

```bash
# 1. 启动 MO（3.0-dev，使用 DISK SHARED backend + 激进 checkpoint 配置）
cd matrixone-3.0-dev
./mo-service -launch etc/launch-minio-local/launch.toml &

# 2. 等待 MO 就绪
mysql -h 127.0.0.1 -P 6001 -u dump -p111 -e "SELECT 1"

# 3. 生成测试数据
./prepare_ckp_dump_coverage_data.sh \
  --host 127.0.0.1 --port 6001 --user dump --password 111 \
  --db-prefix ckp --scale 10000 --drop-existing

# 4. 等待 checkpoint 生成（约 1-2 分钟）
sleep 120

# 5. 优雅关闭 MO（触发 shutdown checkpoint）
pkill -TERM mo-service
sleep 10

# 6. 查看 checkpoint 信息
./mo-tool ckp info /path/to/mo-data/shared

# 7. 列出数据库
./mo-tool ckp list /path/to/mo-data/shared --type databases

# 8. 列出某个 database 的表
./mo-tool ckp list /path/to/mo-data/shared --database-id <DB_ID>

# 9. 查看表 DDL
./mo-tool ckp show-create-table /path/to/mo-data/shared --table-id <TABLE_ID>

# 10. dump 单表
./mo-tool ckp dump --table-id <TABLE_ID> --header -o /tmp/table.csv /path/to/mo-data/shared

# 11. dump 整个 database（含 LOAD 脚本）
./mo-tool ckp dump --database-id <DB_ID> --load-script -o /tmp/dump_out /path/to/mo-data/shared
```
