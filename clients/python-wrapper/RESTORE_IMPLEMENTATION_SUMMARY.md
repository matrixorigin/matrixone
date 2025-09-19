# MatrixOne Python SDK - Restore 功能实现总结

## 🎯 实现概述

根据 [MatrixOne 官方文档](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/SQL-Reference/Data-Definition-Language/restore-snapshot/)，我已经成功为 MatrixOne Python SDK 添加了完整的 restore 功能，支持从快照恢复数据。

## ✅ 实现的功能

### 1. 核心 RestoreManager 类

**文件**: `matrixone/restore.py`

- **RestoreManager**: 主要的 restore 操作管理器
- **TransactionRestoreManager**: 支持事务的 restore 管理器

**支持的操作**:
- `restore_cluster()` - 集群级别恢复
- `restore_tenant()` - 租户级别恢复
- `restore_database()` - 数据库级别恢复
- `restore_table()` - 表级别恢复
- `restore_with_executor()` - 自定义执行器支持

### 2. 异步支持

**文件**: `matrixone/async_client.py`

- **AsyncRestoreManager**: 异步 restore 管理器
- **AsyncTransactionRestoreManager**: 异步事务 restore 管理器

**异步操作**:
- `await restore_cluster()` - 异步集群恢复
- `await restore_tenant()` - 异步租户恢复
- `await restore_database()` - 异步数据库恢复
- `await restore_table()` - 异步表恢复

### 3. 异常处理

**文件**: `matrixone/exceptions.py`

- **RestoreError**: 专门的 restore 操作异常类

## 🚀 使用示例

### 1. 基本 Restore 操作

```python
from matrixone import Client, SnapshotLevel

client = Client()
client.connect(host="localhost", port=6001, user="root", password="111", database="test")

# 集群恢复
success = client.restore.restore_cluster("cluster_snapshot_1")

# 租户恢复（恢复到自身）
success = client.restore.restore_tenant("acc1_snap1", "acc1")

# 租户恢复（恢复到新租户）
success = client.restore.restore_tenant("acc1_snap1", "acc1", "acc2")

# 数据库恢复
success = client.restore.restore_database("acc1_db_snap1", "acc1", "db1")

# 表恢复
success = client.restore.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")
```

### 2. 事务中的 Restore 操作

```python
# 在事务中执行 restore 操作
with client.transaction() as tx:
    # 创建快照
    snapshot = tx.snapshots.create("transaction_snapshot", SnapshotLevel.DATABASE, database="test_db")
    
    # 在事务中恢复
    success = tx.restore.restore_database("transaction_snapshot", "acc1", "test_db")
    
    # 其他操作...
    tx.execute("INSERT INTO users (name) VALUES ('Alice')")
    
    # 所有操作都是原子的
```

### 3. 异步 Restore 操作

```python
import asyncio
from matrixone import AsyncClient

async def async_restore_example():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    # 异步集群恢复
    success = await client.restore.restore_cluster("cluster_snapshot_1")
    
    # 异步租户恢复
    success = await client.restore.restore_tenant("acc1_snap1", "acc1")
    
    # 异步事务恢复
    async with client.transaction() as tx:
        success = await tx.restore.restore_database("snapshot", "acc1", "db1")
    
    await client.disconnect()

asyncio.run(async_restore_example())
```

## 📊 支持的 Restore 类型

### 1. 集群级别恢复

```sql
RESTORE CLUSTER FROM SNAPSHOT cluster_snapshot_1
```

**用途**: 恢复整个集群到快照状态
**权限**: 系统租户

### 2. 租户级别恢复

```sql
-- 恢复到自身
RESTORE ACCOUNT acc1 FROM SNAPSHOT acc1_snap1

-- 恢复到新租户
RESTORE ACCOUNT acc1 FROM SNAPSHOT acc1_snap1 TO ACCOUNT acc2
```

**用途**: 恢复租户数据
**权限**: 系统租户或租户自身

### 3. 数据库级别恢复

```sql
-- 恢复到自身
RESTORE ACCOUNT acc1 DATABASE db1 FROM SNAPSHOT acc1_db_snap1

-- 恢复到新租户
RESTORE ACCOUNT acc1 DATABASE db1 FROM SNAPSHOT acc1_db_snap1 TO ACCOUNT acc2
```

**用途**: 恢复特定数据库
**权限**: 系统租户或租户自身

### 4. 表级别恢复

```sql
-- 恢复到自身
RESTORE ACCOUNT acc1 DATABASE db1 TABLE t1 FROM SNAPSHOT acc1_tab_snap1

-- 恢复到新租户
RESTORE ACCOUNT acc1 DATABASE db1 TABLE t1 FROM SNAPSHOT acc1_tab_snap1 TO ACCOUNT acc2
```

**用途**: 恢复特定表
**权限**: 系统租户或租户自身

## 🔧 技术实现细节

### 1. SQL 生成

所有 restore 操作都通过动态生成 SQL 语句实现：

```python
def restore_cluster(self, snapshot_name: str) -> bool:
    sql = f"RESTORE CLUSTER FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)}"
    result = self._client.execute(sql)
    return result is not None
```

### 2. 参数验证

```python
def restore_with_executor(self, restore_type: str, snapshot_name: str, 
                         account_name: Optional[str] = None, ...):
    if restore_type == 'tenant' and not account_name:
        raise RestoreError("Account name is required for tenant restore")
    # ... 其他验证
```

### 3. 错误处理

```python
try:
    result = self._client.execute(sql)
    return result is not None
except Exception as e:
    raise RestoreError(f"Failed to restore {restore_type} from snapshot '{snapshot_name}': {e}")
```

### 4. 事务支持

```python
class TransactionRestoreManager(RestoreManager):
    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self._transaction_wrapper = transaction_wrapper
    
    def restore_cluster(self, snapshot_name: str) -> bool:
        return self.restore_with_executor('cluster', snapshot_name, executor=self._transaction_wrapper)
```

## 📁 创建的文件

### 1. 核心实现文件

- `matrixone/restore.py` - RestoreManager 和 TransactionRestoreManager
- `matrixone/exceptions.py` - 添加了 RestoreError 异常类
- `matrixone/client.py` - 集成 RestoreManager 到 Client
- `matrixone/async_client.py` - 集成 AsyncRestoreManager 到 AsyncClient
- `matrixone/__init__.py` - 导出 RestoreManager 和 RestoreError

### 2. 测试文件

- `test_restore.py` - 完整的单元测试套件
  - TestRestoreManager - 同步 restore 操作测试
  - TestAsyncRestoreManager - 异步 restore 操作测试
  - TestTransactionRestoreManager - 事务 restore 操作测试

### 3. 示例文件

- `example_restore.py` - 完整的使用示例
  - 基本 restore 操作示例
  - 事务 restore 操作示例
  - 异步 restore 操作示例
  - 错误处理示例

## 🎯 基于官方文档的实现

### 1. 参考的官方示例

根据 [MatrixOne 官方文档](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/SQL-Reference/Data-Definition-Language/restore-snapshot/) 中的示例：

**Example 1: Restore Cluster**
```sql
RESTORE CLUSTER FROM SNAPSHOT cluster_sp1;
```

**Example 2: Restore Tenant**
```sql
RESTORE ACCOUNT acc1 FROM SNAPSHOT acc1_snap1;
```

**Example 3: Restore Database**
```sql
RESTORE ACCOUNT acc1 DATABASE db1 FROM SNAPSHOT acc1_db_snap1;
```

**Example 4: Restore Table**
```sql
RESTORE ACCOUNT acc1 DATABASE db1 TABLE t1 FROM SNAPSHOT acc1_tab_snap1;
```

**Example 5: System Tenant Restores Regular Tenant to Itself**
```sql
RESTORE ACCOUNT acc1 FROM SNAPSHOT acc1_snap1 TO ACCOUNT acc1;
```

**Example 6: System Tenant Restores Regular Tenant to New Tenant**
```sql
RESTORE ACCOUNT acc1 FROM SNAPSHOT acc1_snap1 TO ACCOUNT acc2;
```

### 2. 限制和注意事项

根据官方文档的限制：

- 系统租户只能在恢复到新租户时执行租户级别恢复
- 只有系统租户可以执行恢复到新租户的操作，且只允许租户级别恢复
- 新租户必须提前创建
- 为避免对象冲突，建议使用新创建的租户

## 🧪 测试覆盖

### 1. 单元测试

- **27 个测试用例**，覆盖所有 restore 操作
- **同步操作测试**: 集群、租户、数据库、表恢复
- **异步操作测试**: 异步版本的所有操作
- **事务操作测试**: 事务中的 restore 操作
- **错误处理测试**: 各种错误情况的处理
- **参数验证测试**: 必需参数的验证

### 2. 测试结果

```
Ran 27 tests in 0.062s
OK
```

所有测试都通过，确保功能的正确性和稳定性。

## 🎉 总结

MatrixOne Python SDK 现在提供了完整的 restore 功能：

1. **完整的 Restore 支持** - 集群、租户、数据库、表级别恢复
2. **跨租户恢复** - 支持恢复到新租户
3. **事务支持** - restore 操作可以在事务中执行
4. **异步支持** - 非阻塞的异步 restore 操作
5. **错误处理** - 完善的错误处理和异常类型
6. **官方文档兼容** - 完全基于 MatrixOne 官方文档实现
7. **测试覆盖** - 全面的单元测试确保质量
8. **使用示例** - 详细的使用示例和文档

这个实现为 MatrixOne 用户提供了强大而灵活的数据恢复能力，支持各种恢复场景和需求。🎉
