# MatrixOne Python SDK - mo_ctl 集成

## 概述

MatrixOne Python SDK 现在支持 `mo_ctl` 控制操作，这些操作需要 sys 租户权限。`mo_ctl` 提供了对 MatrixOne 数据库系统的底层控制能力，包括强制刷新、检查点、日志控制、集群管理等操作。

## 功能特性

### ✅ 支持的操作类型

1. **Force Flush 操作**
   - 强制刷新特定表
   - 强制刷新整个数据库

2. **Checkpoint 操作**
   - 强制检查点
   - 增量检查点
   - 完整检查点

3. **日志控制操作**
   - 设置日志级别
   - 启用/禁用组件日志

4. **集群管理操作**
   - 获取集群状态
   - 获取集群信息

5. **节点管理操作**
   - 获取节点状态
   - 获取节点信息

6. **自定义操作**
   - 执行自定义 mo_ctl 命令

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

# 检查 mo_ctl 是否可用
if client.moctl.is_available():
    print("mo_ctl operations are available")
```

### 2. Force Flush 操作

```python
# 强制刷新特定表
result = client.moctl.flush_table('database_name', 'table_name')
print(f"Flush table result: {result}")

# 强制刷新整个数据库
result = client.moctl.flush_database('database_name')
print(f"Flush database result: {result}")
```

### 3. Checkpoint 操作

```python
# 强制检查点
result = client.moctl.checkpoint()
print(f"Checkpoint result: {result}")

# 增量检查点
result = client.moctl.incremental_checkpoint()
print(f"Incremental checkpoint result: {result}")

# 完整检查点
result = client.moctl.full_checkpoint()
print(f"Full checkpoint result: {result}")
```

### 4. 日志控制操作

```python
# 设置日志级别
result = client.moctl.log_level('debug')
print(f"Set log level result: {result}")

# 启用组件日志
result = client.moctl.log_enable('sql')
print(f"Enable logging result: {result}")

# 禁用组件日志
result = client.moctl.log_disable('sql')
print(f"Disable logging result: {result}")
```

### 5. 集群管理操作

```python
# 获取集群状态
result = client.moctl.cluster_status()
print(f"Cluster status: {result}")

# 获取集群信息
result = client.moctl.cluster_info()
print(f"Cluster info: {result}")
```

### 6. 节点管理操作

```python
# 获取所有节点状态
result = client.moctl.node_status()
print(f"All nodes status: {result}")

# 获取特定节点状态
result = client.moctl.node_status('node1')
print(f"Node1 status: {result}")

# 获取所有节点信息
result = client.moctl.node_info()
print(f"All nodes info: {result}")

# 获取特定节点信息
result = client.moctl.node_info('node1')
print(f"Node1 info: {result}")
```

### 7. 自定义操作

```python
# 执行自定义 mo_ctl 命令
result = client.moctl.custom_ctl('method', 'target', 'params')
print(f"Custom mo_ctl result: {result}")

# 获取支持的操作列表
operations = client.moctl.get_supported_operations()
print("Supported operations:")
for category, ops in operations.items():
    print(f"  {category}: {ops}")
```

## 高级用法

### 1. 错误处理

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

### 2. 批量操作

```python
# 批量刷新多个表
tables = ['users', 'orders', 'products']
for table in tables:
    try:
        result = client.moctl.flush_table('mydb', table)
        print(f"Flushed {table}: {result}")
    except MoCtlError as e:
        print(f"Failed to flush {table}: {e}")
```

### 3. 集成使用

```python
# 结合快照和克隆操作
with client.transaction() as tx:
    # 创建快照
    snapshot = tx.snapshots.create(
        name="backup_snapshot",
        level="database",
        database="production"
    )
    
    # 强制刷新
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
    # 强制刷新所有表
    client.moctl.flush_database('production')
    
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
    # 设置详细日志级别
    client.moctl.log_level('debug')
    
    # 启用 SQL 日志
    client.moctl.log_enable('sql')
    
    # 执行维护操作
    # ...
    
    # 恢复日志级别
    client.moctl.log_level('info')
    client.moctl.log_disable('sql')
```

### 3. 性能优化

```python
def optimize_performance():
    """性能优化操作"""
    # 强制刷新热点表
    hot_tables = ['users', 'sessions', 'logs']
    for table in hot_tables:
        client.moctl.flush_table('production', table)
    
    # 强制检查点
    client.moctl.checkpoint()
    
    # 检查集群状态
    status = client.moctl.cluster_status()
    print(f"Cluster status: {status}")
```

### 4. 监控和诊断

```python
def system_diagnosis():
    """系统诊断"""
    # 获取集群信息
    cluster_info = client.moctl.cluster_info()
    print(f"Cluster info: {cluster_info}")
    
    # 获取所有节点状态
    nodes_status = client.moctl.node_status()
    print(f"Nodes status: {nodes_status}")
    
    # 获取特定节点信息
    node_info = client.moctl.node_info('node1')
    print(f"Node1 info: {node_info}")
```

## 注意事项

### 1. 权限要求

- **sys 租户权限**：所有 mo_ctl 操作都需要 sys 租户权限
- **连接用户**：必须使用具有足够权限的用户连接

### 2. 操作影响

- **Force Flush**：会强制将内存中的数据刷新到磁盘，可能影响性能
- **Checkpoint**：会生成检查点并截断 WAL，是重要的维护操作
- **日志控制**：会影响系统的日志输出，需要谨慎使用

### 3. 错误处理

```python
# 检查操作是否可用
if not client.moctl.is_available():
    print("mo_ctl operations are not available")
    return

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
Tests run: 25
Failures: 0
Errors: 0
Success rate: 100.0%
==================================================
```

### 测试覆盖

- ✅ 基本 mo_ctl 操作
- ✅ Force flush 操作
- ✅ Checkpoint 操作
- ✅ 日志控制操作
- ✅ 集群管理操作
- ✅ 节点管理操作
- ✅ 自定义操作
- ✅ 错误处理
- ✅ 集成测试

## 总结

MatrixOne Python SDK 的 mo_ctl 集成提供了：

1. **完整的控制能力** - 支持所有主要的 mo_ctl 操作
2. **类型安全** - 强类型接口和错误处理
3. **易于使用** - 简洁的 API 设计
4. **错误处理** - 完善的异常处理机制
5. **集成能力** - 与快照、克隆等操作无缝集成

这个集成为 MatrixOne 数据库的管理和维护提供了强大的工具，使得系统管理员和开发者可以更方便地执行各种控制操作。
