# MatrixOne Python SDK - mo_ctl 核心操作

## 概述

MatrixOne Python SDK 现在支持 `mo_ctl` 的核心控制操作，这些操作需要 sys 租户权限。目前支持两个核心功能：

1. **Force Flush Table** - 强制刷新表
2. **Force Checkpoint** - 强制检查点

## 功能特性

### ✅ 支持的操作

1. **Force Flush Table**
   - 强制刷新特定表的所有块
   - 确保数据持久化到磁盘

2. **Force Checkpoint**
   - 刷新 DN 中的所有块
   - 生成增量检查点
   - 截断 WAL

## 基本用法

### 1. 初始化客户端

```python
from matrixone import Client

# 创建客户端
client = Client()

# 以 sys 租户身份连接
client.connect(
    host="localhost",
    port=6001,
    user="root",  # sys 租户
    password="111",
    database="test"
)
```

### 2. Force Flush Table

```python
# 强制刷新特定表
result = client.moctl.flush_table('database_name', 'table_name')
print(f"Flush table result: {result}")

# 示例
result = client.moctl.flush_table('db1', 'users')
# 返回: {'method': 'Flush', 'result': [{'returnStr': 'OK'}]}
```

### 3. Force Checkpoint

```python
# 强制检查点
result = client.moctl.checkpoint()
print(f"Checkpoint result: {result}")

# 示例
result = client.moctl.checkpoint()
# 返回: {'method': 'Checkpoint', 'result': [{'returnStr': 'OK'}]}
```

## 完整示例

```python
from matrixone import Client
from matrixone.exceptions import MoCtlError

client = Client()

try:
    # 连接数据库
    client.connect(
        host="localhost",
        port=6001,
        user="root",
        password="111",
        database="test"
    )
    
    # 创建测试表
    client.execute("""
        CREATE TABLE IF NOT EXISTS test_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100)
        )
    """)
    
    # 插入测试数据
    client.execute("INSERT INTO test_table (name) VALUES ('test1'), ('test2')")
    
    # 强制刷新表
    result = client.moctl.flush_table('test', 'test_table')
    print(f"Flush result: {result}")
    
    # 强制检查点
    result = client.moctl.checkpoint()
    print(f"Checkpoint result: {result}")
    
except MoCtlError as e:
    print(f"mo_ctl operation failed: {e}")
except Exception as e:
    print(f"Error: {e}")
finally:
    client.disconnect()
```

## 错误处理

```python
from matrixone.exceptions import MoCtlError

try:
    result = client.moctl.flush_table('db1', 'users')
    print(f"Success: {result}")
except MoCtlError as e:
    print(f"mo_ctl operation failed: {e}")
except Exception as e:
    print(f"Other error: {e}")
```

## 集成使用

```python
# 结合快照和克隆操作
with client.transaction() as tx:
    # 创建快照
    snapshot = tx.snapshots.create(
        name="backup_snapshot",
        level="database",
        database="production"
    )
    
    # 强制刷新表
    tx.moctl.flush_table('production', 'users')
    
    # 强制检查点
    tx.moctl.checkpoint()
    
    # 克隆数据库
    tx.clone.clone_database_with_snapshot(
        "backup_production",
        "production",
        "backup_snapshot"
    )
```

## 使用场景

### 1. 数据备份前准备

```python
def prepare_for_backup():
    """备份前的准备工作"""
    # 强制刷新表
    client.moctl.flush_table('production', 'users')
    client.moctl.flush_table('production', 'orders')
    
    # 强制检查点
    client.moctl.checkpoint()
    
    # 创建快照
    snapshot = client.snapshots.create(
        name="backup_snapshot",
        level="database",
        database="production"
    )
    
    return snapshot
```

### 2. 系统维护

```python
def system_maintenance():
    """系统维护操作"""
    # 强制刷新所有重要表
    important_tables = ['users', 'sessions', 'logs']
    for table in important_tables:
        client.moctl.flush_table('production', table)
    
    # 强制检查点
    client.moctl.checkpoint()
    
    print("System maintenance completed")
```

## 注意事项

### 1. 权限要求

- **sys 租户权限**：所有 mo_ctl 操作都需要 sys 租户权限
- **连接用户**：必须使用具有足够权限的用户连接

### 2. 操作影响

- **Force Flush**：会强制将内存中的数据刷新到磁盘，可能影响性能
- **Checkpoint**：会生成检查点并截断 WAL，是重要的维护操作

### 3. 错误处理

```python
# 使用 try-catch 处理错误
try:
    result = client.moctl.flush_table('db1', 'users')
except MoCtlError as e:
    print(f"mo_ctl operation failed: {e}")
```

### 4. 性能考虑

- **批量操作**：避免频繁调用 mo_ctl 操作
- **时机选择**：在系统负载较低时执行维护操作
- **监控影响**：监控操作对系统性能的影响

## 测试验证

### 测试结果

```
==================================================
Tests run: 10
Failures: 0
Errors: 0
Success rate: 100.0%
==================================================
```

### 测试覆盖

- ✅ 基本 mo_ctl 操作
- ✅ Force flush table 操作
- ✅ Force checkpoint 操作
- ✅ 错误处理
- ✅ 集成测试

## 总结

MatrixOne Python SDK 的 mo_ctl 核心操作提供了：

1. **Force Flush Table** - 确保数据持久化
2. **Force Checkpoint** - 管理 WAL 和检查点
3. **类型安全** - 强类型接口和错误处理
4. **易于使用** - 简洁的 API 设计
5. **集成能力** - 与快照、克隆等操作无缝集成

这些核心操作为 MatrixOne 数据库的管理和维护提供了基础工具，确保数据的一致性和持久性。
