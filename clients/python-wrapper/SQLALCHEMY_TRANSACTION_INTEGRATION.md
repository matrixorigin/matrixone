# MatrixOne Python SDK - SQLAlchemy 事务集成

## 概述

MatrixOne Python SDK 与 SQLAlchemy 完美集成，支持在 SQLAlchemy 事务中执行快照和克隆操作，以及在 MatrixOne 事务中使用 SQLAlchemy ORM。这种集成提供了强大的数据操作能力和事务一致性保证。

## 功能特性

### ✅ 支持的集成模式

1. **SQLAlchemy 事务 + MatrixOne 快照**
   - 在 SQLAlchemy 事务中执行数据操作
   - 在事务外创建快照

2. **MatrixOne 事务 + SQLAlchemy ORM**
   - 在 MatrixOne 事务中使用 SQLAlchemy ORM
   - 在事务中创建快照和克隆

3. **混合事务模式**
   - SQLAlchemy 事务嵌套在 MatrixOne 事务中
   - 完整的 ACID 保证

4. **错误处理和回滚**
   - 自动回滚机制
   - 跨层事务一致性

## 基本用法

### 1. SQLAlchemy 事务 + MatrixOne 快照

```python
from matrixone import Client
from matrixone.snapshot import SnapshotLevel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime

# 定义模型
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.now)

# 初始化客户端
client = Client()
client.connect(host="localhost", port=6001, user="root", password="111", database="test")

# 创建 SQLAlchemy 引擎
engine = create_engine("mysql+pymysql://root:111@localhost:6001/test")
Session = sessionmaker(bind=engine)

# 创建表
Base.metadata.create_all(engine)

# SQLAlchemy 事务
session = Session()
try:
    # 插入数据
    users = [
        User(name="Alice Johnson", email="alice@example.com"),
        User(name="Bob Smith", email="bob@example.com")
    ]
    
    session.add_all(users)
    session.commit()
    print("SQLAlchemy 事务提交成功")
    
except Exception as e:
    session.rollback()
    print(f"SQLAlchemy 事务失败: {e}")
    raise
finally:
    session.close()

# 创建快照
snapshot = client.snapshots.create(
    name="sqlalchemy_snapshot",
    level=SnapshotLevel.DATABASE,
    database="test",
    description="SQLAlchemy 操作后的快照"
)
```

### 2. MatrixOne 事务 + SQLAlchemy ORM

```python
# MatrixOne 事务中使用 SQLAlchemy
with client.transaction() as tx:
    # 创建快照
    snapshot = tx.snapshots.create(
        name="transaction_snapshot",
        level=SnapshotLevel.DATABASE,
        database="test"
    )
    
    # SQLAlchemy 操作
    session = Session()
    try:
        # 添加用户
        new_user = User(name="Charlie Brown", email="charlie@example.com")
        session.add(new_user)
        session.commit()
        
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()
    
    # 克隆数据库
    tx.clone.clone_database_with_snapshot(
        "backup_test",
        "test",
        "transaction_snapshot",
        if_not_exists=True
    )
```

### 3. 错误处理和回滚

```python
try:
    with client.transaction() as tx:
        # 创建快照
        snapshot = tx.snapshots.create(
            name="error_test_snapshot",
            level=SnapshotLevel.DATABASE,
            database="test"
        )
        
        # SQLAlchemy 操作（会失败）
        session = Session()
        try:
            # 尝试插入重复邮箱
            duplicate_user = User(name="Duplicate", email="alice@example.com")
            session.add(duplicate_user)
            session.commit()
            
        except Exception as e:
            session.rollback()
            print(f"SQLAlchemy 错误: {e}")
            raise
        finally:
            session.close()
        
        # 这行不会执行，因为上面出错了
        tx.clone.clone_database("should_not_exist", "test")
        
except Exception as e:
    print(f"事务失败: {e}")

# 验证回滚
try:
    client.snapshots.get("error_test_snapshot")
    print("错误：快照不应该存在！")
except Exception:
    print("✓ 快照正确回滚")
```

## 高级用法

### 1. 批量操作

```python
# 批量插入
session = Session()
try:
    # 准备批量数据
    users_data = [
        {"name": f"User_{i}", "email": f"user{i}@example.com"}
        for i in range(1, 1001)  # 1000 个用户
    ]
    
    # 批量插入
    session.bulk_insert_mappings(User, users_data)
    session.commit()
    
    print(f"批量插入了 {len(users_data)} 个用户")
    
except Exception as e:
    session.rollback()
    raise
finally:
    session.close()

# 创建快照
snapshot = client.snapshots.create(
    name="batch_insert_snapshot",
    level=SnapshotLevel.DATABASE,
    database="test"
)
```

### 2. 连接池配置

```python
# 高级引擎配置
engine = create_engine(
    "mysql+pymysql://root:111@localhost:6001/test",
    echo=False,                    # 开发时设为 True
    pool_pre_ping=True,           # 使用前验证连接
    pool_recycle=300,             # 5分钟回收连接
    pool_size=10,                 # 连接池大小
    max_overflow=20,              # 额外连接数
    isolation_level="READ_COMMITTED"  # 事务隔离级别
)

Session = sessionmaker(bind=engine)

# 使用连接池
sessions = [Session() for _ in range(5)]

try:
    for i, session in enumerate(sessions):
        with client.transaction() as tx:
            # 创建快照
            tx.snapshots.create(
                name=f"pool_snapshot_{i}",
                level=SnapshotLevel.DATABASE,
                database="test"
            )
            
            # SQLAlchemy 操作
            user = User(name=f"Pool User {i}", email=f"pool{i}@example.com")
            session.add(user)
            session.commit()
            
finally:
    for session in sessions:
        session.close()
```

### 3. 事件处理

```python
from sqlalchemy import event

# 注册事件监听器
@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    print(f"执行 SQL: {statement[:100]}...")

@event.listens_for(engine, "after_cursor_execute")
def receive_after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    print("SQL 执行成功")

# 使用事件监听
session = Session()
try:
    with client.transaction() as tx:
        # 创建快照（会触发事件）
        snapshot = tx.snapshots.create(
            name="event_logged_snapshot",
            level=SnapshotLevel.DATABASE,
            database="test"
        )
        
        # SQLAlchemy 操作（会触发事件）
        user = User(name="Event User", email="event@example.com")
        session.add(user)
        session.commit()
        
finally:
    session.close()
```

## 最佳实践

### 1. 资源管理

```python
# 推荐：使用上下文管理器
def safe_operation():
    session = Session()
    try:
        with client.transaction() as tx:
            # 创建快照
            snapshot = tx.snapshots.create(
                name="safe_snapshot",
                level=SnapshotLevel.DATABASE,
                database="test"
            )
            
            # SQLAlchemy 操作
            user = User(name="Safe User", email="safe@example.com")
            session.add(user)
            session.commit()
            
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()

# 避免：手动管理资源
def unsafe_operation():
    session = Session()
    # 容易忘记关闭 session
    # 容易忘记处理异常
    pass
```

### 2. 错误处理

```python
def robust_operation():
    session = Session()
    try:
        with client.transaction() as tx:
            # 创建快照
            snapshot = tx.snapshots.create(
                name="robust_snapshot",
                level=SnapshotLevel.DATABASE,
                database="test"
            )
            
            # SQLAlchemy 操作
            session.begin()
            
            try:
                user = User(name="Robust User", email="robust@example.com")
                session.add(user)
                session.commit()
                
            except Exception as e:
                session.rollback()
                print(f"SQLAlchemy 操作失败: {e}")
                raise
                
    except Exception as e:
        print(f"整体操作失败: {e}")
        raise
    finally:
        session.close()
```

### 3. 性能优化

```python
# 批量操作
def batch_operation():
    session = Session()
    try:
        # 准备数据
        users_data = [
            {"name": f"Batch User {i}", "email": f"batch{i}@example.com"}
            for i in range(1, 1001)
        ]
        
        # 批量插入
        session.bulk_insert_mappings(User, users_data)
        session.commit()
        
        # 创建快照
        snapshot = client.snapshots.create(
            name="batch_snapshot",
            level=SnapshotLevel.DATABASE,
            database="test"
        )
        
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()

# 连接池优化
def optimized_engine():
    return create_engine(
        "mysql+pymysql://root:111@localhost:6001/test",
        pool_size=20,           # 增加连接池大小
        max_overflow=30,        # 增加额外连接
        pool_pre_ping=True,     # 验证连接
        pool_recycle=600,       # 10分钟回收
        echo=False              # 生产环境关闭 SQL 日志
    )
```

## 使用场景

### 1. 数据迁移

```python
def data_migration():
    """数据迁移场景"""
    session = Session()
    try:
        with client.transaction() as tx:
            # 创建迁移前快照
            pre_migration_snapshot = tx.snapshots.create(
                name="pre_migration",
                level=SnapshotLevel.DATABASE,
                database="source_db"
            )
            
            # 迁移数据
            source_users = session.query(SourceUser).all()
            for source_user in source_users:
                target_user = TargetUser(
                    name=source_user.name,
                    email=source_user.email
                )
                session.add(target_user)
            
            session.commit()
            
            # 创建迁移后快照
            post_migration_snapshot = tx.snapshots.create(
                name="post_migration",
                level=SnapshotLevel.DATABASE,
                database="target_db"
            )
            
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()
```

### 2. 数据备份

```python
def data_backup():
    """数据备份场景"""
    session = Session()
    try:
        # 创建备份快照
        backup_snapshot = client.snapshots.create(
            name="daily_backup",
            level=SnapshotLevel.DATABASE,
            database="production"
        )
        
        # 克隆到备份环境
        client.clone.clone_database_with_snapshot(
            "backup_production",
            "production",
            "daily_backup"
        )
        
        # 验证备份
        backup_count = client.execute("SELECT COUNT(*) FROM backup_production.users").scalar()
        original_count = session.query(User).count()
        
        if backup_count == original_count:
            print("✓ 备份验证成功")
        else:
            print("✗ 备份验证失败")
            
    except Exception as e:
        print(f"备份失败: {e}")
        raise
    finally:
        session.close()
```

### 3. 开发环境管理

```python
def dev_environment_setup():
    """开发环境管理场景"""
    session = Session()
    try:
        with client.transaction() as tx:
            # 创建生产快照
            prod_snapshot = tx.snapshots.create(
                name="prod_for_dev",
                level=SnapshotLevel.DATABASE,
                database="production"
            )
            
            # 创建开发环境
            tx.clone.clone_database_with_snapshot(
                "development",
                "production",
                "prod_for_dev"
            )
            
            # 添加开发数据
            dev_user = User(name="Dev User", email="dev@example.com")
            session.add(dev_user)
            session.commit()
            
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()
```

## 注意事项

### 1. 事务边界

```python
# 正确：在 MatrixOne 事务中使用 SQLAlchemy
with client.transaction() as tx:
    session = Session()
    try:
        # SQLAlchemy 操作
        user = User(name="Test", email="test@example.com")
        session.add(user)
        session.commit()
        
        # MatrixOne 操作
        tx.snapshots.create("test_snapshot", SnapshotLevel.DATABASE, database="test")
        
    finally:
        session.close()

# 错误：SQLAlchemy 事务嵌套在 MatrixOne 事务中
with client.transaction() as tx:
    with Session() as session:
        with session.begin():  # 嵌套事务可能导致问题
            pass
```

### 2. 连接管理

```python
# 正确：及时关闭连接
session = Session()
try:
    # 操作
    pass
finally:
    session.close()

# 错误：忘记关闭连接
session = Session()
# 操作
# 忘记 session.close()
```

### 3. 错误处理

```python
# 正确：完整的错误处理
session = Session()
try:
    with client.transaction() as tx:
        # 操作
        pass
except Exception as e:
    session.rollback()
    raise
finally:
    session.close()

# 错误：不完整的错误处理
session = Session()
try:
    # 操作
    pass
except Exception as e:
    # 忘记 session.rollback()
    raise
# 忘记 session.close()
```

## 总结

MatrixOne Python SDK 与 SQLAlchemy 的集成提供了：

1. **无缝集成** - SQLAlchemy ORM 与 MatrixOne 事务完美配合
2. **事务一致性** - 跨层事务的 ACID 保证
3. **错误处理** - 自动回滚和资源清理
4. **性能优化** - 连接池和批量操作支持
5. **灵活性** - 支持多种集成模式

这种集成使得开发者可以充分利用 SQLAlchemy 的强大 ORM 功能，同时享受 MatrixOne 的快照和克隆能力，构建更加可靠和高效的数据应用程序。
