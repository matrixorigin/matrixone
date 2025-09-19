# MatrixOne Async + SQLAlchemy 事务集成

## 概述

MatrixOne Python SDK 提供了完整的异步支持，并且可以与 SQLAlchemy ORM 在同一个事务中无缝协作。这确保了所有操作（MatrixOne 异步 SQL、快照管理、克隆操作、SQLAlchemy ORM）都具有 ACID 属性。

## 🎯 核心特性

### 1. 统一事务管理
- **单一事务** - 所有操作在同一个数据库事务中
- **ACID 属性** - 原子性、一致性、隔离性、持久性
- **自动回滚** - 任何操作失败时自动回滚所有更改
- **数据一致性** - 确保数据状态始终有效

### 2. 混合操作支持
- **SQLAlchemy ORM** - 对象关系映射操作
- **MatrixOne 异步 SQL** - 原生异步 SQL 操作
- **快照管理** - 异步快照创建和管理
- **克隆操作** - 异步数据库和表克隆
- **mo_ctl 操作** - 异步控制操作

## 🚀 基本用法

### 1. 创建统一事务

```python
import asyncio
from matrixone import AsyncClient, SnapshotLevel
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)

async def main():
    client = AsyncClient()
    await client.connect(
        host="localhost",
        port=6001,
        user="root",
        password="111",
        database="test"
    )
    
    # 统一事务
    async with client.transaction() as tx:
        # 获取 SQLAlchemy 会话
        session = await tx.get_sqlalchemy_session()
        
        # SQLAlchemy ORM 操作
        user = User(name="Alice", email="alice@example.com")
        session.add(user)
        session.flush()
        
        # MatrixOne 异步 SQL 操作
        await tx.execute(
            "INSERT INTO users (name, email) VALUES (%s, %s)",
            ("Bob", "bob@example.com")
        )
        
        # MatrixOne 异步快照操作
        snapshot = await tx.snapshots.create(
            name="transaction_snapshot",
            level=SnapshotLevel.DATABASE,
            database="test"
        )
        
        # MatrixOne 异步克隆操作
        await tx.clone.clone_database_with_snapshot(
            "backup",
            "test",
            "transaction_snapshot"
        )
    
    await client.disconnect()

asyncio.run(main())
```

### 2. 错误处理和回滚

```python
async def transaction_with_error_handling():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    try:
        async with client.transaction() as tx:
            session = await tx.get_sqlalchemy_session()
            
            # SQLAlchemy 操作
            user = User(name="Alice", email="alice@example.com")
            session.add(user)
            session.flush()
            
            # MatrixOne 操作
            await tx.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Bob", "bob@example.com"))
            
            # 模拟错误
            await tx.execute("INSERT INTO nonexistent_table (name) VALUES ('test')")
            
    except Exception as e:
        print(f"Transaction failed: {e}")
        # 所有操作自动回滚
    
    await client.disconnect()
```

### 3. 并发操作

```python
async def concurrent_operations():
    client = AsyncClient()
    await client.connect(host="localhost", port=6001, user="root", password="111", database="test")
    
    async with client.transaction() as tx:
        session = await tx.get_sqlalchemy_session()
        
        # 并发创建用户
        async def create_user_async(name: str, email: str):
            await tx.execute(
                "INSERT INTO users (name, email) VALUES (%s, %s)",
                (name, email)
            )
        
        async def create_user_sqlalchemy(name: str, email: str):
            user = User(name=name, email=email)
            session.add(user)
            session.flush()
        
        # 并发执行
        tasks = [
            create_user_async("User1", "user1@example.com"),
            create_user_sqlalchemy("User2", "user2@example.com"),
            create_user_async("User3", "user3@example.com"),
            create_user_sqlalchemy("User4", "user4@example.com")
        ]
        
        await asyncio.gather(*tasks)
    
    await client.disconnect()
```

## 📊 性能优势

### 1. 事务效率
- **单一事务** - 减少事务开销
- **批量操作** - 支持批量插入和更新
- **并发执行** - 事务内并发操作
- **资源优化** - 更好的连接和资源利用

### 2. 操作效率
- **混合操作** - SQLAlchemy ORM + MatrixOne 异步 SQL
- **快照集成** - 事务内快照创建和管理
- **克隆集成** - 事务内数据库克隆
- **控制操作** - 事务内 mo_ctl 操作

## 🔧 技术实现

### 1. 事务包装器

```python
class AsyncTransactionWrapper:
    def __init__(self, connection, client):
        self.connection = connection
        self.client = client
        self._sqlalchemy_session = None
        self._sqlalchemy_engine = None
    
    async def get_sqlalchemy_session(self):
        """获取绑定到当前事务的 SQLAlchemy 会话"""
        if self._sqlalchemy_session is None:
            # 创建 SQLAlchemy 引擎和会话
            # 绑定到当前数据库连接
            self._sqlalchemy_session = Session()
            self._sqlalchemy_session.begin()
        
        return self._sqlalchemy_session
    
    async def commit_sqlalchemy(self):
        """提交 SQLAlchemy 会话"""
        if self._sqlalchemy_session:
            self._sqlalchemy_session.commit()
    
    async def rollback_sqlalchemy(self):
        """回滚 SQLAlchemy 会话"""
        if self._sqlalchemy_session:
            self._sqlalchemy_session.rollback()
```

### 2. 事务管理

```python
@asynccontextmanager
async def transaction(self):
    """异步事务上下文管理器"""
    if not self._connection:
        raise ConnectionError("Not connected to database")
    
    tx_wrapper = None
    try:
        await self._connection.begin()
        tx_wrapper = AsyncTransactionWrapper(self._connection, self)
        yield tx_wrapper
        
        # 先提交 SQLAlchemy 会话
        await tx_wrapper.commit_sqlalchemy()
        # 再提交主事务
        await self._connection.commit()
        
    except Exception as e:
        # 先回滚 SQLAlchemy 会话
        if tx_wrapper:
            await tx_wrapper.rollback_sqlalchemy()
        # 再回滚主事务
        await self._connection.rollback()
        raise e
    finally:
        # 清理 SQLAlchemy 资源
        if tx_wrapper:
            await tx_wrapper.close_sqlalchemy()
```

## 🎯 使用场景

### 1. 数据迁移
```python
async def data_migration():
    async with client.transaction() as tx:
        session = await tx.get_sqlalchemy_session()
        
        # 使用 SQLAlchemy 查询源数据
        old_users = session.query(OldUser).all()
        
        # 使用 MatrixOne 异步插入新数据
        for user in old_users:
            await tx.execute(
                "INSERT INTO new_users (name, email) VALUES (%s, %s)",
                (user.name, user.email)
            )
        
        # 创建迁移快照
        await tx.snapshots.create("migration_snapshot", SnapshotLevel.DATABASE, database="test")
```

### 2. 数据备份
```python
async def data_backup():
    async with client.transaction() as tx:
        session = await tx.get_sqlalchemy_session()
        
        # 使用 SQLAlchemy 验证数据
        user_count = session.query(User).count()
        
        # 创建备份快照
        snapshot = await tx.snapshots.create("backup_snapshot", SnapshotLevel.DATABASE, database="test")
        
        # 克隆数据库
        await tx.clone.clone_database_with_snapshot("backup_db", "test", "backup_snapshot")
```

### 3. 批量处理
```python
async def batch_processing():
    async with client.transaction() as tx:
        session = await tx.get_sqlalchemy_session()
        
        # 批量处理用户
        users = session.query(User).filter(User.status == 'pending').all()
        
        # 并发更新状态
        tasks = []
        for user in users:
            task = tx.execute(
                "UPDATE users SET status = %s WHERE id = %s",
                ("processed", user.id)
            )
            tasks.append(task)
        
        await asyncio.gather(*tasks)
        
        # 创建处理完成快照
        await tx.snapshots.create("processed_snapshot", SnapshotLevel.DATABASE, database="test")
```

## ⚠️ 注意事项

### 1. 事务边界
- 所有操作必须在事务内完成
- 事务外无法访问 SQLAlchemy 会话
- 确保正确使用 `async with` 语法

### 2. 错误处理
- 任何操作失败都会导致整个事务回滚
- 使用 try-catch 处理特定错误
- 确保资源正确清理

### 3. 性能考虑
- 长时间运行的事务可能影响并发性能
- 合理使用批量操作
- 避免在事务中进行大量计算

## 📚 示例文件

- `example_async_sqlalchemy_simple.py` - 简单示例
- `example_async_sqlalchemy_transaction.py` - 完整示例

## 🎉 总结

MatrixOne 异步接口与 SQLAlchemy 的事务集成提供了：

1. **统一事务管理** - 所有操作在同一个事务中
2. **ACID 属性** - 完整的事务特性
3. **混合操作支持** - SQLAlchemy ORM + MatrixOne 异步
4. **高性能** - 并发操作和资源优化
5. **数据一致性** - 确保数据状态始终有效

这种集成为构建高性能、高可靠性的数据库应用提供了强大的工具！
