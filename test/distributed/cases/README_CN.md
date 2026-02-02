# BVT Test Case Tags Reference

本文档说明 mo-tester 支持的测试标签用法，供编写 BVT 测试用例时参考。

## 文件级标签

### `-- @skip:issue#{IssueNo.}`
跳过整个测试文件，用于已知问题暂时无法修复的场景。

```sql
-- @skip:issue#16438

drop database if exists db1;
create database db1;
```

## SQL 级标签

### `-- @bvt:issue#{IssueNo.}` / `-- @bvt:issue`
标记因已知 issue 暂时跳过的 SQL 语句块。运行时使用 `-g` 参数可跳过这些语句。

```sql
-- @bvt:issue#5790
drop table if exists t1;
create table t1(a int, b varchar(20), unique key(a));
insert into t1 values(null, '2');
-- @bvt:issue
```

### `-- @ignore:{col_index},...`
忽略结果集中指定列的比较，列索引从 0 开始。适用于包含时间戳、随机值等不稳定数据的查询。

```sql
-- @ignore:5,6
show publications;

-- @ignore:0,4
show columns from `procs_priv`;
```

### `-- @sortkey:{col_index},...`
指定结果集的排序键列，用于结果顺序不确定的查询。

```sql
-- @sortkey:0,1
SELECT col1, col2 FROM t1;
```

### `-- @regex("<pattern>", <include:boolean>)`
正则表达式匹配检查。`include=true` 表示结果必须包含匹配项，`false` 表示不能包含。

```sql
-- @regex("acc_save",true)
-- @regex("root",false)
show accounts;
```

## 会话控制标签

### `-- @session:id={N}&user={user}&password={pwd}` / `-- @session}`
创建新连接执行 SQL 语句块，用于测试并发事务场景。

```sql
begin;
select * from t1;
-- @session:id=1{
insert into t1 values (100);
select * from t1;
-- @session}
commit;
```

参数说明：
- `id`: 会话 ID，默认 1
- `user`: 用户名，格式 `account:user`，默认使用 mo.yml 配置
- `password`: 密码，默认使用 mo.yml 配置

### `-- @wait:{session_id}:{commit|rollback}`
等待指定会话提交或回滚后再继续执行，用于测试事务隔离性。

```sql
begin;
update t1 set a = 1;
-- @session:id=1{
-- @wait:0:commit
update t1 set a = 2;  -- 等待 session 0 提交后执行
-- @session}
commit;
```

## 元数据比较标签

### `--- @metacmp(boolean)` (文档级)
控制整个文件是否比较结果集元数据（列名、类型等）。

```sql
--- @metacmp(false)
-- 文件中所有 SQL 都不比较元数据
```

### `-- @metacmp(boolean)` (SQL级)
控制单条 SQL 是否比较元数据，优先级高于文档级和全局设置。

```sql
-- @metacmp(true)
SELECT * FROM t1;  -- 比较元数据
```

## 测试用例编写规范

1. **自包含**: 测试文件应独立运行，不依赖其他测试的状态
2. **清理资源**: 测试结束时清理创建的数据库、表等资源
3. **复用数据库**: 尽量复用已存在的数据库，避免创建过多临时数据库

## 运行测试

```bash
# 运行单个测试文件
cd /root/mo-tester && ./run.sh -n -g -p /root/matrixone/test/distributed/cases/your_test.test

# 生成结果文件（新测试用例）
cd /root/mo-tester && ./run.sh -m genrs -n -g -p /root/matrixone/test/distributed/cases/your_test.test
```

## 结果文件格式说明

### 列元数据格式

在生成的 `.result` 文件中，每列的元数据格式为：`column_name[type,precision,scale]`

示例：
```
➤ id[4,32,0]  ¦  name[12,255,0]  ¦  price[3,10,2]  𝄀
```

### 列类型编码对照表

mo-tester 使用 JDBC `java.sql.Types` 定义的整型编码表示列类型：

| 类型编码 | 类型名称 | 说明 |
|---------|---------|------|
| -7 | BIT | 位类型 |
| -6 | TINYINT | 微整型 |
| -5 | BIGINT | 大整型 |
| -4 | LONGVARBINARY | 长变长二进制 |
| -3 | VARBINARY | 变长二进制 |
| -2 | BINARY | 二进制 |
| -1 | LONGVARCHAR | 长变长字符 |
| 0 | NULL | 空类型 |
| 1 | CHAR | 定长字符 |
| 2 | NUMERIC | 数值类型 |
| 3 | DECIMAL | 十进制数 |
| 4 | INTEGER | 整型 |
| 5 | SMALLINT | 小整型 |
| 6 | FLOAT | 浮点型 |
| 7 | REAL | 实数 |
| 8 | DOUBLE | 双精度浮点 |
| 12 | VARCHAR | 变长字符 |
| 16 | BOOLEAN | 布尔型 |
| 91 | DATE | 日期 |
| 92 | TIME | 时间 |
| 93 | TIMESTAMP | 时间戳 |
| 2003 | ARRAY | 数组 |
| 2004 | BLOB | 二进制大对象 |
| 2005 | CLOB | 字符大对象 |

**常用类型示例：**
- `[4,32,0]` - INTEGER，精度 32，标度 0
- `[12,255,0]` - VARCHAR(255)
- `[3,10,2]` - DECIMAL(10,2)
- `[-5,64,0]` - BIGINT
- `[93,64,0]` - TIMESTAMP

完整类型列表参考 JDBC `java.sql.Types` 规范。

## 废弃标签（请勿在新用例中使用）

### `-- @separator:table` ⚠️ 已废弃
此标签已废弃，目前没有实际效果，仅用于已有 case 的兼容。新测试用例请勿使用。

### `-- @pattern` ⚠️ 已废弃
此标签已废弃，请使用 `-- @regex` 替代。

迁移示例：
```sql
-- 旧写法（废弃）
-- @pattern
insert into t1 values(1,'bell'),(2,'app'),(1,'com');

-- 新写法（推荐）
-- @regex("Duplicate entry",true)
insert into t1 values(1,'bell'),(2,'app'),(1,'com');
```