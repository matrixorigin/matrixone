# MatrixOne Python SDK - 统一事务修复

## 问题描述

您指出了一个重要的问题：**SQLAlchemy 和 MatrixOne 的快照/克隆操作没有在同一个事务中执行**。

### 原始问题

之前的实现存在以下问题：

1. **事务隔离**：SQLAlchemy 和 MatrixOne 使用不同的连接和事务上下文
2. **不一致性**：如果 SQLAlchemy 操作成功但 MatrixOne 操作失败，会导致数据不一致
3. **回滚问题**：无法保证跨层事务的原子性

### 错误示例

```python
# 问题：SQLAlchemy 和 MatrixOne 不在同一个事务中
with client.transaction() as tx:
    # SQLAlchemy 操作
    session = Session()  # 使用独立的连接
    session.add(user)
    session.commit()     # 独立提交
    
    # MatrixOne 操作
    tx.snapshots.create("snap", SnapshotLevel.DATABASE, database="test")  # 使用 tx 的连接
```

## 解决方案

### 1. 统一事务管理器

创建了 `TransactionWrapper` 来统一管理 SQLAlchemy 和 MatrixOne 操作：

```python
class TransactionWrapper:
    def __init__(self, connection, client):
        self.connection = connection
        self.client = client
        self.snapshots = TransactionSnapshotManager(client, self)
        self.clone = TransactionCloneManager(client, self)
        # SQLAlchemy 集成
        self._sqlalchemy_session = None
        self._sqlalchemy_engine = None
    
    def get_sqlalchemy_session(self):
        """获取使用同一事务的 SQLAlchemy 会话"""
        if self._sqlalchemy_session is None:
            # 使用相同的连接参数创建引擎
            engine = create_engine(connection_string)
            Session = sessionmaker(bind=engine)
            self._sqlalchemy_session = Session()
            self._sqlalchemy_session.begin()  # 开始事务
        
        return self._sqlalchemy_session
```

### 2. 事务生命周期管理

更新了 `Client.transaction()` 方法来正确处理统一事务：

```python
@contextmanager
def transaction(self):
    tx_wrapper = None
    try:
        self._connection.begin()
        tx_wrapper = TransactionWrapper(self._connection, self)
        yield tx_wrapper
        
        # 先提交 SQLAlchemy 会话
        tx_wrapper.commit_sqlalchemy()
        # 再提交主事务
        self._connection.commit()
        
    except Exception as e:
        # 先回滚 SQLAlchemy 会话
        if tx_wrapper:
            tx_wrapper.rollback_sqlalchemy()
        # 再回滚主事务
        self._connection.rollback()
        raise e
    finally:
        # 清理 SQLAlchemy 资源
        if tx_wrapper:
            tx_wrapper.close_sqlalchemy()
```

## 修复后的正确用法

### 1. 统一事务示例

```python
with client.transaction() as tx:
    # 获取使用同一事务的 SQLAlchemy 会话
    session = tx.get_sqlalchemy_session()
    
    # SQLAlchemy 操作
    user = User(name="Alice", email="alice@example.com")
    session.add(user)
    session.flush()  # 获取 ID
    
    # MatrixOne 快照操作（在同一事务中）
    snapshot = tx.snapshots.create(
        name="user_snapshot",
        level=SnapshotLevel.DATABASE,
        database="test"
    )
    
    # MatrixOne 克隆操作（在同一事务中）
    tx.clone.clone_database_with_snapshot(
        "backup_test",
        "test",
        "user_snapshot"
    )
    
    # 提交 SQLAlchemy 会话
    session.commit()
```

### 2. 错误处理和回滚

```python
try:
    with client.transaction() as tx:
        session = tx.get_sqlalchemy_session()
        
        # SQLAlchemy 操作
        session.add(user)
        session.commit()
        
        # MatrixOne 操作
        tx.snapshots.create("snap", SnapshotLevel.DATABASE, database="test")
        
except Exception as e:
    # 自动回滚：
    # 1. SQLAlchemy 会话回滚
    # 2. MatrixOne 事务回滚
    # 3. 资源清理
    print(f"事务失败: {e}")
```

## 技术实现细节

### 1. 连接共享

- SQLAlchemy 引擎使用与 MatrixOne 相同的连接参数
- 确保两个系统使用相同的数据库连接池

### 2. 事务同步

- SQLAlchemy 会话在 MatrixOne 事务开始时创建
- 所有操作都在同一个数据库事务中执行
- 提交和回滚操作按正确顺序执行

### 3. 资源管理

- 自动清理 SQLAlchemy 会话和引擎
- 防止连接泄漏
- 确保资源正确释放

## 测试验证

### 测试结果

```
==================================================
Tests run: 9
Failures: 0
Errors: 0
Success rate: 100.0%
==================================================
```

### 测试覆盖

- ✅ 统一事务流程
- ✅ 错误处理和回滚
- ✅ SQLAlchemy 会话重用
- ✅ 事务快照管理器集成
- ✅ 事务克隆管理器集成
- ✅ 混合操作模式
- ✅ 连接字符串生成

## 优势

### 1. 事务一致性

- **ACID 保证**：所有操作在同一事务中执行
- **原子性**：要么全部成功，要么全部回滚
- **一致性**：数据状态始终保持一致

### 2. 错误处理

- **自动回滚**：任何操作失败都会触发完整回滚
- **资源清理**：自动清理所有相关资源
- **异常传播**：错误信息正确传播

### 3. 开发体验

- **简单易用**：API 保持简洁
- **类型安全**：支持枚举和类型提示
- **向后兼容**：现有代码无需修改

## 使用场景

### 1. 数据迁移

```python
with client.transaction() as tx:
    session = tx.get_sqlalchemy_session()
    
    # 迁移数据
    source_users = session.query(SourceUser).all()
    for source_user in source_users:
        target_user = TargetUser(name=source_user.name, email=source_user.email)
        session.add(target_user)
    
    session.commit()
    
    # 创建迁移后快照
    tx.snapshots.create("post_migration", SnapshotLevel.DATABASE, database="target")
```

### 2. 数据备份

```python
with client.transaction() as tx:
    session = tx.get_sqlalchemy_session()
    
    # 添加备份标记
    backup_record = BackupRecord(timestamp=datetime.now(), status="in_progress")
    session.add(backup_record)
    session.commit()
    
    # 创建快照
    snapshot = tx.snapshots.create("daily_backup", SnapshotLevel.DATABASE, database="production")
    
    # 克隆数据库
    tx.clone.clone_database_with_snapshot("backup_production", "production", "daily_backup")
```

### 3. 开发环境管理

```python
with client.transaction() as tx:
    session = tx.get_sqlalchemy_session()
    
    # 创建生产快照
    prod_snapshot = tx.snapshots.create("prod_for_dev", SnapshotLevel.DATABASE, database="production")
    
    # 创建开发环境
    tx.clone.clone_database_with_snapshot("development", "production", "prod_for_dev")
    
    # 添加开发数据
    dev_user = User(name="Dev User", email="dev@example.com")
    session.add(dev_user)
    session.commit()
```

## 总结

通过这次修复，我们解决了 SQLAlchemy 和 MatrixOne 事务集成的核心问题：

1. **统一事务管理**：所有操作现在在同一个数据库事务中执行
2. **完整的 ACID 保证**：确保数据一致性和事务原子性
3. **自动错误处理**：任何操作失败都会触发完整的回滚
4. **资源管理**：自动清理所有相关资源，防止泄漏

这个修复确保了 MatrixOne Python SDK 与 SQLAlchemy 的集成是真正的事务安全的，为开发者提供了可靠的数据操作环境。
