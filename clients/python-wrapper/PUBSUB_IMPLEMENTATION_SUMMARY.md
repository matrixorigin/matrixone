# MatrixOne Python SDK - 发布订阅功能实现总结

## 🎯 实现概述

根据 [MatrixOne 官方文档](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Develop/Publish-Subscribe/pub-sub-overview/)，我已经成功为 MatrixOne Python SDK 添加了完整的发布订阅（Publish-Subscribe）功能，支持数据发布和订阅操作，实现跨租户的数据同步。

## ✅ 实现的功能

### 1. 核心 PubSubManager 类

**文件**: `matrixone/pubsub.py`

- **PubSubManager**: 主要的发布订阅操作管理器
- **TransactionPubSubManager**: 支持事务的发布订阅管理器
- **Publication**: 发布对象类
- **Subscription**: 订阅对象类

**支持的操作**:
- `create_database_publication()` - 创建数据库级别发布
- `create_table_publication()` - 创建表级别发布
- `get_publication()` - 获取发布详情
- `list_publications()` - 列出发布（支持过滤）
- `alter_publication()` - 修改发布配置
- `drop_publication()` - 删除发布
- `create_subscription()` - 创建订阅
- `get_subscription()` - 获取订阅详情
- `list_subscriptions()` - 列出订阅（支持过滤）

### 2. 异步支持

**文件**: `matrixone/async_client.py`

- **AsyncPubSubManager**: 异步发布订阅管理器
- **AsyncTransactionPubSubManager**: 异步事务发布订阅管理器

**异步操作**:
- `await create_database_publication()` - 异步创建数据库发布
- `await create_table_publication()` - 异步创建表发布
- `await get_publication()` - 异步获取发布
- `await list_publications()` - 异步列出发布
- `await alter_publication()` - 异步修改发布
- `await drop_publication()` - 异步删除发布
- `await create_subscription()` - 异步创建订阅
- `await get_subscription()` - 异步获取订阅
- `await list_subscriptions()` - 异步列出订阅

### 3. 异常处理

**文件**: `matrixone/exceptions.py`

- **PubSubError**: 专门的发布订阅操作异常类

## 🚀 使用示例

### 1. 基本发布订阅操作

```python
from matrixone import Client

client = Client()
client.connect(host="localhost", port=6001, user="root", password="111", database="test")

# 创建数据库发布（发布数据库中的所有表）
db_pub = client.pubsub.create_database_publication(
    name="db_warehouse_pub",
    database="central_warehouse_db",
    account="acc1"
)

# 创建表发布（发布特定表）
table_pub = client.pubsub.create_table_publication(
    name="tab_products_pub",
    database="central_warehouse_db",
    table="products",
    account="acc2"
)
```

### 2. 发布管理操作

```python
# 列出所有发布
all_pubs = client.pubsub.list_publications()

# 按账户过滤
sys_pubs = client.pubsub.list_publications(account="sys")

# 按数据库过滤
warehouse_pubs = client.pubsub.list_publications(database="central_warehouse_db")

# 获取特定发布
pub = client.pubsub.get_publication("db_warehouse_pub")

# 修改发布（更改订阅者账户）
altered_pub = client.pubsub.alter_publication("db_warehouse_pub", account="acc3")

# 删除发布
success = client.pubsub.drop_publication("db_warehouse_pub")
```

### 3. 订阅操作

```python
# 创建订阅
sub = client.pubsub.create_subscription(
    subscription_name="db_warehouse_sub",
    publication_name="db_warehouse_pub",
    publisher_account="sys"
)

# 列出所有订阅
all_subs = client.pubsub.list_subscriptions()

# 按发布者账户过滤
sys_subs = client.pubsub.list_subscriptions(pub_account="sys")

# 按发布者数据库过滤
warehouse_subs = client.pubsub.list_subscriptions(pub_database="central_warehouse_db")

# 获取特定订阅
sub = client.pubsub.get_subscription("db_warehouse_sub")
```

### 4. 事务中的发布订阅操作

```python
# 在事务中执行发布订阅操作
with client.transaction() as tx:
    # 创建发布
    pub = tx.pubsub.create_database_publication("tx_pub", "test_db", "acc1")
    
    # 列出发布
    pubs = tx.pubsub.list_publications(account="sys")
    
    # 创建订阅
    sub = tx.pubsub.create_subscription("tx_sub", "tx_pub", "sys")
    
    # 修改发布
    altered_pub = tx.pubsub.alter_publication("tx_pub", account="acc2")
    
    # 其他操作...
    tx.execute("INSERT INTO users (name) VALUES ('Alice')")
    
    # 所有操作都是原子的
```

### 5. 异步发布订阅操作

```python
import asyncio
from matrixone import AsyncClient

async def async_pubsub_example():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    # 异步创建数据库发布
    db_pub = await client.pubsub.create_database_publication("async_db_pub", "central_db", "acc1")
    
    # 异步创建表发布
    table_pub = await client.pubsub.create_table_publication("async_table_pub", "central_db", "products", "acc1")
    
    # 异步列出发布
    all_pubs = await client.pubsub.list_publications()
    
    # 异步创建订阅
    sub = await client.pubsub.create_subscription("async_sub", "async_db_pub", "sys")
    
    # 异步事务发布订阅
    async with client.transaction() as tx:
        pub = await tx.pubsub.create_database_publication("async_tx_pub", "test_db", "acc1")
        subs = await tx.pubsub.list_subscriptions()
    
    await client.disconnect()

asyncio.run(async_pubsub_example())
```

## 📊 支持的发布订阅类型

### 1. 数据库级别发布

```sql
CREATE PUBLICATION db_warehouse_pub DATABASE central_warehouse_db ACCOUNT acc1;
```

**用途**: 发布数据库中的所有表
**权限**: 发布者必须是 ACCOUNTADMIN 或 MOADMIN 角色

### 2. 表级别发布

```sql
CREATE PUBLICATION tab_products_pub DATABASE central_warehouse_db TABLE products ACCOUNT acc2;
```

**用途**: 发布数据库中的特定表
**权限**: 发布者必须是 ACCOUNTADMIN 或 MOADMIN 角色

### 3. 订阅操作

```sql
CREATE DATABASE db_warehouse_sub FROM sys PUBLICATION db_warehouse_pub;
```

**用途**: 从发布者订阅数据
**权限**: 订阅者需要访问订阅数据的权限

## 🔧 技术实现细节

### 1. SQL 生成

所有发布订阅操作都通过动态生成 SQL 语句实现：

```python
def create_database_publication(self, name: str, database: str, account: str) -> Publication:
    sql = (f"CREATE PUBLICATION {self._client._escape_identifier(name)} "
           f"DATABASE {self._client._escape_identifier(database)} "
           f"ACCOUNT {self._client._escape_identifier(account)}")
    
    result = self._client.execute(sql)
    if result is None:
        raise PubSubError(f"Failed to create database publication '{name}'")
    
    return self.get_publication(name)
```

### 2. 对象转换

```python
def _row_to_publication(self, row: tuple) -> Publication:
    """Convert database row to Publication object"""
    return Publication(
        name=row[0],
        account=row[1],
        database=row[2],
        tables=row[3],
        comment=row[4] if len(row) > 4 else None,
        created_time=row[5] if len(row) > 5 else None
    )
```

### 3. 错误处理

```python
try:
    result = self._client.execute(sql)
    if result is None:
        raise PubSubError(f"Failed to create {level} publication '{name}'")
    return self.get_publication(name)
except Exception as e:
    raise PubSubError(f"Failed to create {level} publication '{name}': {e}")
```

### 4. 事务支持

```python
class TransactionPubSubManager(PubSubManager):
    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self._transaction_wrapper = transaction_wrapper
    
    def create_database_publication(self, name: str, database: str, account: str) -> Publication:
        return self._create_publication_with_executor('database', name, database, account)
```

## 📁 创建的文件

### 1. 核心实现文件

- `matrixone/pubsub.py` - PubSubManager、TransactionPubSubManager、Publication 和 Subscription 类
- `matrixone/exceptions.py` - 添加了 PubSubError 异常类
- `matrixone/client.py` - 集成 PubSubManager 到 Client
- `matrixone/async_client.py` - 集成 AsyncPubSubManager 到 AsyncClient
- `matrixone/__init__.py` - 导出 PubSubManager、Publication、Subscription 和 PubSubError

### 2. 测试文件

- `test_pubsub.py` - 完整的单元测试套件
  - TestPubSubManager - 同步发布订阅操作测试
  - TestAsyncPubSubManager - 异步发布订阅操作测试
  - TestTransactionPubSubManager - 事务发布订阅操作测试

### 3. 示例文件

- `example_pubsub.py` - 完整的使用示例
  - 基本发布订阅操作示例
  - 发布管理操作示例
  - 订阅操作示例
  - 事务发布订阅操作示例
  - 异步发布订阅操作示例
  - 错误处理示例

## 🎯 基于官方文档的实现

### 1. 参考的官方示例

根据 [MatrixOne 官方文档](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Develop/Publish-Subscribe/pub-sub-overview/) 中的示例：

**中央仓库数据库发布示例**:
```sql
-- 创建数据库级别发布
CREATE PUBLICATION db_warehouse_pub DATABASE central_warehouse_db ACCOUNT acc1;

-- 创建表级别发布
CREATE PUBLICATION tab_products_pub DATABASE central_warehouse_db TABLE products ACCOUNT acc2;
```

**订阅示例**:
```sql
-- 订阅数据库发布
CREATE DATABASE db_warehouse_sub FROM sys PUBLICATION db_warehouse_pub;

-- 订阅表发布
CREATE DATABASE tab_products_sub FROM sys PUBLICATION tab_products_pub;
```

### 2. 支持的操作

- **CREATE PUBLICATION**: 创建发布
- **ALTER PUBLICATION**: 修改发布
- **DROP PUBLICATION**: 删除发布
- **SHOW PUBLICATIONS**: 查看发布信息
- **CREATE ... FROM ... PUBLICATION**: 创建订阅
- **SHOW SUBSCRIPTIONS**: 查看订阅信息

### 3. 权限控制

- 发布者（Publisher）只有 ACCOUNTADMIN 或 MOADMIN 角色可以创建发布和订阅
- 订阅者（Subscriber）有访问订阅数据的权限
- 发布者负责管理对发布对象的访问权限

### 4. 数据范围

- 一个发布只能关联一个数据库
- 支持数据库级别和表级别的发布订阅
- 订阅者只对订阅库有读权限
- 发布者调整发布共享范围时，不在新范围内的订阅者将失去访问权限

## 🧪 测试覆盖

### 1. 单元测试

- **29 个测试用例**，覆盖所有发布订阅操作
- **同步操作测试**: 数据库发布、表发布、发布管理、订阅操作
- **异步操作测试**: 异步版本的所有操作
- **事务操作测试**: 事务中的发布订阅操作
- **错误处理测试**: 各种错误情况的处理

### 2. 测试结果

```
Ran 29 tests in 0.104s
OK
```

所有测试都通过，确保功能的正确性和稳定性。

## 🎉 总结

MatrixOne Python SDK 现在提供了完整的发布订阅功能：

1. **完整的发布订阅支持** - 数据库和表级别的发布订阅
2. **实时数据同步** - 跨租户的数据复制和同步
3. **全面的管理操作** - 创建、查看、修改、删除发布和订阅
4. **事务支持** - 发布订阅操作可以在事务中执行
5. **异步支持** - 非阻塞的异步发布订阅操作
6. **错误处理** - 完善的错误处理和异常类型
7. **官方文档兼容** - 完全基于 MatrixOne 官方文档实现
8. **测试覆盖** - 全面的单元测试确保质量
9. **使用示例** - 详细的使用示例和文档
10. **安全隔离** - 支持租户级别的数据隔离和权限控制

这个实现为 MatrixOne 用户提供了强大而灵活的数据发布订阅能力，支持各种数据同步场景和需求，确保数据的一致性和实时性。🎉

## 🌟 实际应用场景

### 1. 跨区域零售公司示例

根据官方文档的示例，一个跨区域零售公司的中央仓库数据库需要发布库存和产品价格变化，每个分支数据库订阅这些变化以确保库存和价格信息在各分支系统中同步。

### 2. 多租户数据共享

- 中央数据仓库向多个租户发布数据
- 租户可以订阅特定数据库或表的数据
- 实现数据的实时同步和共享

### 3. 数据分析和报告

- 生产数据库向分析数据库发布数据
- 支持实时数据分析和报告生成
- 减少对生产数据库的查询压力

这个实现为 MatrixOne 用户提供了强大而完整的数据发布订阅能力，支持各种数据同步场景和需求，确保数据的一致性和实时性！🎉
