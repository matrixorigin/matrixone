# MatrixOne Python SDK - 事务中的快照和克隆支持

## 概述

现在 MatrixOne Python SDK 完全支持在事务中执行快照和克隆操作！这意味着您可以：

- 在同一个事务中创建快照、执行克隆操作和其他 SQL 语句
- 确保所有操作的原子性（要么全部成功，要么全部回滚）
- 构建复杂的数据库工作流程

## 功能特性

### ✅ 支持的操作

1. **快照操作**
   - 创建快照 (`CREATE SNAPSHOT`)
   - 删除快照 (`DROP SNAPSHOT`)
   - 查询快照信息

2. **克隆操作**
   - 数据库克隆 (`CREATE DATABASE ... CLONE`)
   - 表克隆 (`CREATE TABLE ... CLONE`)
   - 带快照的克隆操作

3. **混合操作**
   - 快照 + 克隆 + 其他 SQL 语句在同一个事务中

### ✅ 事务保证

- **原子性**: 所有操作要么全部成功，要么全部回滚
- **一致性**: 事务中的操作保持数据一致性
- **隔离性**: 事务中的操作对其他连接不可见，直到提交
- **持久性**: 提交后的操作永久保存

## 使用方法

### 基本用法

```python
from matrixone import Client

client = Client()
client.connect(host="localhost", port=6001, user="root", password="111", database="test")

# 在事务中执行快照和克隆操作
with client.transaction() as tx:
    # 执行普通 SQL
    tx.execute("INSERT INTO users (name, email) VALUES (%s, %s)", ("Alice", "alice@example.com"))
    
    # 创建快照
    snapshot = tx.snapshots.create("backup_snap", "table", database="test", table="users")
    
    # 克隆表
    tx.clone.clone_table("backup_table", "test.users", if_not_exists=True)
    
    # 更多 SQL 操作
    tx.execute("UPDATE users SET status = 'active'")
```

### 高级用法

```python
# 复杂的工作流程
with client.transaction() as tx:
    # 1. 创建生产数据快照
    prod_snapshot = tx.snapshots.create(
        name="prod_backup_001",
        level="database",
        database="production",
        description="Production backup before deployment"
    )
    
    # 2. 从快照创建测试环境
    tx.clone.clone_database_with_snapshot(
        "testing_env",
        "production",
        "prod_backup_001",
        if_not_exists=True
    )
    
    # 3. 在测试环境中进行修改
    tx.execute("INSERT INTO testing_env.users (name, email) VALUES (%s, %s)", ("Test User", "test@example.com"))
    
    # 4. 创建测试环境快照
    test_snapshot = tx.snapshots.create(
        name="test_changes_001",
        level="database",
        database="testing_env",
        description="Test environment changes"
    )
    
    # 5. 从测试快照创建预发布环境
    tx.clone.clone_database_with_snapshot(
        "staging_env",
        "testing_env",
        "test_changes_001",
        if_not_exists=True
    )
```

## API 参考

### TransactionWrapper 类

事务包装器提供了在事务中执行操作的方法：

```python
class TransactionWrapper:
    def execute(self, sql: str, params: Optional[Tuple] = None) -> ResultSet:
        """在事务中执行 SQL"""
    
    @property
    def snapshots(self) -> TransactionSnapshotManager:
        """获取事务快照管理器"""
    
    @property
    def clone(self) -> TransactionCloneManager:
        """获取事务克隆管理器"""
```

### TransactionSnapshotManager 类

事务快照管理器，继承自 `SnapshotManager`，但所有操作都在事务中执行：

```python
class TransactionSnapshotManager:
    def create(self, name: str, level: str, database: Optional[str] = None, 
               table: Optional[str] = None, description: Optional[str] = None) -> Snapshot:
        """在事务中创建快照"""
    
    def get(self, name: str) -> Snapshot:
        """在事务中获取快照信息"""
    
    def delete(self, name: str) -> None:
        """在事务中删除快照"""
```

### TransactionCloneManager 类

事务克隆管理器，继承自 `CloneManager`，但所有操作都在事务中执行：

```python
class TransactionCloneManager:
    def clone_database(self, target_db: str, source_db: str, 
                      snapshot_name: Optional[str] = None, 
                      if_not_exists: bool = False) -> None:
        """在事务中克隆数据库"""
    
    def clone_table(self, target_table: str, source_table: str,
                   snapshot_name: Optional[str] = None,
                   if_not_exists: bool = False) -> None:
        """在事务中克隆表"""
    
    def clone_database_with_snapshot(self, target_db: str, source_db: str, 
                                   snapshot_name: str, if_not_exists: bool = False) -> None:
        """在事务中使用快照克隆数据库"""
    
    def clone_table_with_snapshot(self, target_table: str, source_table: str,
                                snapshot_name: str, if_not_exists: bool = False) -> None:
        """在事务中使用快照克隆表"""
```

## 使用场景

### 1. 数据库备份和恢复

```python
# 创建备份快照并克隆到备份环境
with client.transaction() as tx:
    # 创建快照
    backup_snapshot = tx.snapshots.create("daily_backup", "database", database="production")
    
    # 克隆到备份环境
    tx.clone.clone_database_with_snapshot("backup_prod", "production", "daily_backup")
    
    # 记录备份信息
    tx.execute("INSERT INTO backup_log (snapshot_name, created_at) VALUES (%s, %s)", 
               ("daily_backup", datetime.now()))
```

### 2. 开发环境管理

```python
# 从生产环境创建开发环境
with client.transaction() as tx:
    # 创建生产快照
    prod_snapshot = tx.snapshots.create("prod_snapshot", "database", database="production")
    
    # 创建开发环境
    tx.clone.clone_database_with_snapshot("development", "production", "prod_snapshot")
    
    # 添加开发数据
    tx.execute("INSERT INTO development.test_data (name) VALUES ('dev_test')")
    
    # 创建开发快照
    dev_snapshot = tx.snapshots.create("dev_snapshot", "database", database="development")
```

### 3. 数据迁移

```python
# 安全的数据迁移
with client.transaction() as tx:
    # 创建迁移前快照
    pre_migration_snapshot = tx.snapshots.create("pre_migration", "database", database="source_db")
    
    # 克隆到目标数据库
    tx.clone.clone_database_with_snapshot("target_db", "source_db", "pre_migration")
    
    # 执行数据转换
    tx.execute("UPDATE target_db.users SET email = LOWER(email)")
    tx.execute("INSERT INTO target_db.audit_log (action, timestamp) VALUES ('migration', NOW())")
    
    # 创建迁移后快照
    post_migration_snapshot = tx.snapshots.create("post_migration", "database", database="target_db")
```

### 4. 测试数据准备

```python
# 为测试准备数据
with client.transaction() as tx:
    # 创建测试快照
    test_snapshot = tx.snapshots.create("test_data", "table", database="production", table="users")
    
    # 克隆测试数据
    tx.clone.clone_table_with_snapshot("test_users", "production.users", "test_data")
    
    # 添加测试特定数据
    tx.execute("INSERT INTO test_users (name, email, role) VALUES ('Test Admin', 'admin@test.com', 'admin')")
    tx.execute("INSERT INTO test_users (name, email, role) VALUES ('Test User', 'user@test.com', 'user')")
```

## 错误处理

### 事务回滚

如果事务中的任何操作失败，整个事务会自动回滚：

```python
try:
    with client.transaction() as tx:
        # 创建快照
        snapshot = tx.snapshots.create("test_snap", "table", database="test", table="users")
        
        # 克隆表
        tx.clone.clone_table("backup_table", "test.users")
        
        # 这个操作会失败
        tx.execute("INSERT INTO non_existent_table VALUES (1)")
        
except Exception as e:
    print(f"事务失败并回滚: {e}")
    # 快照和克隆表都不会被创建
```

### 异常类型

- `SnapshotError`: 快照操作失败
- `CloneError`: 克隆操作失败
- `QueryError`: SQL 执行失败
- `ConnectionError`: 连接问题

## 最佳实践

### 1. 事务大小控制

```python
# 好的做法：合理的事务大小
with client.transaction() as tx:
    snapshot = tx.snapshots.create("backup", "database", database="prod")
    tx.clone.clone_database("backup_prod", "prod", snapshot_name="backup")
    tx.execute("INSERT INTO backup_log (snapshot_name) VALUES ('backup')")

# 避免：过大的事务
# with client.transaction() as tx:
#     # 不要在这里执行大量操作
#     for i in range(10000):
#         tx.execute(f"INSERT INTO large_table VALUES ({i})")
```

### 2. 错误处理

```python
try:
    with client.transaction() as tx:
        # 操作1
        snapshot = tx.snapshots.create("backup", "database", database="prod")
        
        # 操作2
        tx.clone.clone_database("backup_prod", "prod", snapshot_name="backup")
        
        # 操作3
        tx.execute("UPDATE backup_prod.users SET status = 'backup'")
        
except SnapshotError as e:
    print(f"快照操作失败: {e}")
except CloneError as e:
    print(f"克隆操作失败: {e}")
except QueryError as e:
    print(f"SQL 执行失败: {e}")
except Exception as e:
    print(f"未知错误: {e}")
```

### 3. 资源清理

```python
# 在事务中创建资源，在事务外清理
snapshots_to_cleanup = []

try:
    with client.transaction() as tx:
        snapshot = tx.snapshots.create("temp_snapshot", "database", database="prod")
        snapshots_to_cleanup.append(snapshot.name)
        
        tx.clone.clone_database("temp_backup", "prod", snapshot_name=snapshot.name)
        
finally:
    # 清理资源
    for snapshot_name in snapshots_to_cleanup:
        try:
            client.snapshots.delete(snapshot_name)
        except Exception as e:
            print(f"清理快照失败: {e}")
```

## 性能考虑

### 1. 事务持续时间

- 保持事务尽可能短
- 避免在事务中执行长时间运行的操作
- 考虑将大操作分解为多个小事务

### 2. 快照和克隆性能

- 快照创建是轻量级操作
- 克隆操作可能需要较长时间，取决于数据大小
- 在事务中执行克隆操作会锁定相关资源

### 3. 并发控制

- 事务中的快照和克隆操作会获取必要的锁
- 避免长时间持有锁
- 考虑使用适当的隔离级别

## 总结

MatrixOne Python SDK 现在完全支持在事务中执行快照和克隆操作，这为构建复杂的数据库工作流程提供了强大的支持。通过事务保证，您可以确保操作的原子性和一致性，同时享受快照和克隆功能的便利性。

这个功能特别适用于：
- 数据库备份和恢复
- 开发环境管理
- 数据迁移
- 测试数据准备
- 复杂的数据库工作流程

通过合理使用这些功能，您可以构建更加可靠和高效的数据库应用程序。
