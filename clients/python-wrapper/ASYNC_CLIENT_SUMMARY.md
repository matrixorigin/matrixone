# MatrixOne Python SDK - 异步客户端

## 概述

MatrixOne Python SDK 现在提供了完整的异步支持，包括 `AsyncClient` 类，它提供了与同步版本相同的功能，但所有操作都是异步的。这为高并发场景和性能优化提供了强大的支持。

## 功能特性

### ✅ 异步支持

1. **AsyncClient** - 完全异步的客户端
2. **异步连接管理** - 使用 `aiomysql` 进行异步数据库连接
3. **异步查询执行** - 所有 SQL 操作都是异步的
4. **异步事务管理** - 支持异步事务操作
5. **异步快照管理** - 异步快照创建、查询、删除
6. **异步克隆管理** - 异步数据库和表克隆
7. **异步 mo_ctl 操作** - 异步控制操作

### ✅ 核心组件

1. **AsyncClient** - 主异步客户端类
2. **AsyncResultSet** - 异步结果集包装器
3. **AsyncSnapshotManager** - 异步快照管理器
4. **AsyncCloneManager** - 异步克隆管理器
5. **AsyncMoCtlManager** - 异步 mo_ctl 管理器
6. **AsyncTransactionWrapper** - 异步事务包装器

## 基本用法

### 1. 异步客户端初始化

```python
import asyncio
from matrixone import AsyncClient

async def main():
    # 创建异步客户端
    client = AsyncClient()
    
    # 异步连接
    await client.connect(
        host="localhost",
        port=6001,
        user="root",
        password="111",
        database="test"
    )
    
    # 使用客户端
    result = await client.execute("SELECT 1")
    print(f"Result: {result.scalar()}")
    
    # 异步断开连接
    await client.disconnect()

# 运行异步函数
asyncio.run(main())
```

### 2. 异步上下文管理器

```python
async def main():
    async with AsyncClient() as client:
        await client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        result = await client.execute("SELECT 1")
        print(f"Result: {result.scalar()}")
        # 自动断开连接
```

### 3. 异步查询操作

```python
async def query_examples():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    # 基本查询
    result = await client.execute("SELECT id, name FROM users")
    for row in result:
        print(f"ID: {row[0]}, Name: {row[1]}")
    
    # 带参数的查询
    result = await client.execute("SELECT * FROM users WHERE id = %s", (1,))
    user = result.fetchone()
    
    # 插入数据
    await client.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Alice", "alice@example.com"))
    
    await client.disconnect()
```

### 4. 异步快照操作

```python
async def snapshot_examples():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    # 创建快照
    snapshot = await client.snapshots.create(
        name="async_snapshot",
        level=SnapshotLevel.DATABASE,
        database="test",
        description="Async snapshot example"
    )
    
    # 列出快照
    snapshots = await client.snapshots.list()
    print(f"Total snapshots: {len(snapshots)}")
    
    # 获取快照
    snapshot = await client.snapshots.get("async_snapshot")
    print(f"Snapshot: {snapshot.name}")
    
    # 删除快照
    await client.snapshots.delete("async_snapshot")
    
    await client.disconnect()
```

### 5. 异步克隆操作

```python
async def clone_examples():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    # 克隆数据库
    await client.clone.clone_database("backup_db", "source_db")
    
    # 克隆表
    await client.clone.clone_table("backup_table", "source_table")
    
    # 使用快照克隆
    await client.clone.clone_database_with_snapshot(
        "backup_db",
        "source_db",
        "snapshot_name"
    )
    
    await client.disconnect()
```

### 6. 异步 mo_ctl 操作

```python
async def moctl_examples():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    # 强制刷新表
    result = await client.moctl.flush_table('db1', 'users')
    print(f"Flush result: {result}")
    
    # 强制检查点
    result = await client.moctl.checkpoint()
    print(f"Checkpoint result: {result}")
    
    await client.disconnect()
```

### 7. 异步事务操作

```python
async def transaction_examples():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    async with client.transaction() as tx:
        # 在事务中执行操作
        await tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
        
        # 在事务中创建快照
        snapshot = await tx.snapshots.create(
            name="tx_snapshot",
            level=SnapshotLevel.DATABASE,
            database="test"
        )
        
        # 在事务中克隆
        await tx.clone.clone_database("backup", "test")
    
    await client.disconnect()
```

## 高级用法

### 1. 并发操作

```python
async def concurrent_operations():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    # 并发执行多个查询
    tasks = [
        client.execute("SELECT COUNT(*) FROM users"),
        client.execute("SELECT COUNT(*) FROM orders"),
        client.execute("SELECT COUNT(*) FROM products")
    ]
    
    results = await asyncio.gather(*tasks)
    
    print(f"Users: {results[0].scalar()}")
    print(f"Orders: {results[1].scalar()}")
    print(f"Products: {results[2].scalar()}")
    
    await client.disconnect()
```

### 2. 批量插入

```python
async def batch_insert():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    # 准备数据
    users = [
        ("Alice", "alice@example.com"),
        ("Bob", "bob@example.com"),
        ("Charlie", "charlie@example.com")
    ]
    
    # 并发插入
    tasks = []
    for name, email in users:
        task = client.execute(
            "INSERT INTO users (name, email) VALUES (%s, %s)",
            (name, email)
        )
        tasks.append(task)
    
    await asyncio.gather(*tasks)
    print("All users inserted concurrently")
    
    await client.disconnect()
```

### 3. 错误处理

```python
async def error_handling():
    client = AsyncClient()
    
    try:
        await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
        
        # 尝试执行可能失败的操作
        try:
            await client.execute("SELECT * FROM nonexistent_table")
        except Exception as e:
            print(f"Query failed: {e}")
        
        # 事务错误处理
        try:
            async with client.transaction() as tx:
                await tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
                # 这会导致错误
                await tx.execute("INSERT INTO nonexistent_table (name) VALUES (%s)", ("Bob",))
        except Exception as e:
            print(f"Transaction failed: {e}")
        
    except Exception as e:
        print(f"Connection failed: {e}")
    finally:
        await client.disconnect()
```

## 性能优势

### 1. 并发处理

```python
async def performance_comparison():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    # 同步方式（串行）
    start_time = asyncio.get_event_loop().time()
    for i in range(10):
        await client.execute("SELECT SLEEP(0.1)")
    sync_time = asyncio.get_event_loop().time() - start_time
    
    # 异步方式（并发）
    start_time = asyncio.get_event_loop().time()
    tasks = [client.execute("SELECT SLEEP(0.1)") for _ in range(10)]
    await asyncio.gather(*tasks)
    async_time = asyncio.get_event_loop().time() - start_time
    
    print(f"Sync time: {sync_time:.2f}s")
    print(f"Async time: {async_time:.2f}s")
    print(f"Speedup: {sync_time/async_time:.2f}x")
    
    await client.disconnect()
```

### 2. 资源利用率

- **更好的 I/O 利用率** - 异步操作不会阻塞线程
- **更高的并发性** - 可以同时处理多个请求
- **更低的资源消耗** - 不需要为每个连接创建线程

## 测试验证

### 测试结果

```
==================================================
Tests run: 27
Failures: 0
Errors: 0
Success rate: 100.0%
==================================================
```

### 测试覆盖

- ✅ AsyncClient 基本功能
- ✅ 异步连接管理
- ✅ 异步查询执行
- ✅ 异步快照管理
- ✅ 异步克隆管理
- ✅ 异步 mo_ctl 操作
- ✅ 异步事务管理
- ✅ 错误处理

## 与同步版本的对比

| 特性 | 同步版本 | 异步版本 |
|------|----------|----------|
| 连接管理 | `client.connect()` | `await client.connect()` |
| 查询执行 | `client.execute()` | `await client.execute()` |
| 快照操作 | `client.snapshots.create()` | `await client.snapshots.create()` |
| 克隆操作 | `client.clone.clone_database()` | `await client.clone.clone_database()` |
| mo_ctl 操作 | `client.moctl.flush_table()` | `await client.moctl.flush_table()` |
| 事务管理 | `with client.transaction():` | `async with client.transaction():` |
| 并发处理 | 串行执行 | 并发执行 |
| 性能 | 适合简单场景 | 适合高并发场景 |

## 使用建议

### 1. 何时使用异步版本

- **高并发场景** - 需要同时处理多个请求
- **I/O 密集型操作** - 大量数据库操作
- **性能要求高** - 需要最大化吞吐量
- **现代 Python 应用** - 使用 asyncio 的应用

### 2. 何时使用同步版本

- **简单应用** - 不需要高并发
- **CPU 密集型操作** - 主要计算而非 I/O
- **传统 Python 应用** - 不使用 asyncio 的应用
- **快速原型** - 简单的脚本和工具

### 3. 迁移建议

```python
# 同步版本
def sync_example():
    client = Client()
    client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    result = client.execute("SELECT 1")
    client.disconnect()
    return result.scalar()

# 异步版本
async def async_example():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    result = await client.execute("SELECT 1")
    await client.disconnect()
    return result.scalar()

# 运行异步版本
result = asyncio.run(async_example())
```

## 总结

MatrixOne Python SDK 的异步支持提供了：

1. **完整的异步功能** - 所有操作都支持异步
2. **高性能** - 更好的并发处理和资源利用率
3. **易于使用** - 与同步版本相同的 API 设计
4. **向后兼容** - 同步版本仍然可用
5. **现代 Python 支持** - 充分利用 asyncio 的优势

异步客户端为需要高性能和高并发的 MatrixOne 应用提供了强大的支持，是现代 Python 应用开发的重要工具。
