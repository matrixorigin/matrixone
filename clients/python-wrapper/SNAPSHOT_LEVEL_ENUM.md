# MatrixOne Python SDK - SnapshotLevel 枚举支持

## 概述

现在 MatrixOne Python SDK 支持使用 `SnapshotLevel` 枚举来定义快照级别，提供了更好的类型安全、代码可读性和开发体验。

## 功能特性

### ✅ 枚举定义

```python
class SnapshotLevel(Enum):
    """Snapshot level enumeration"""
    CLUSTER = "cluster"
    ACCOUNT = "account"
    DATABASE = "database"
    TABLE = "table"
```

### ✅ 主要优势

1. **类型安全** - 编译时验证，避免拼写错误
2. **IDE 支持** - 自动补全和类型提示
3. **代码可读性** - 更清晰的代码意图
4. **向后兼容** - 仍然支持字符串值
5. **错误处理** - 更好的验证和错误信息

## 使用方法

### 基本用法

```python
from matrixone.snapshot import SnapshotLevel

# 使用枚举创建快照
snapshot = client.snapshots.create(
    name="backup_snapshot",
    level=SnapshotLevel.TABLE,  # 类型安全的枚举值
    database="test_db",
    table="users"
)

# 检查快照级别
if snapshot.level == SnapshotLevel.TABLE:
    print("This is a table-level snapshot")
```

### 向后兼容

```python
# 仍然支持字符串值（自动转换为枚举）
snapshot = client.snapshots.create(
    name="backup_snapshot",
    level="table",  # 字符串值（向后兼容）
    database="test_db",
    table="users"
)

# 结果仍然是枚举
print(type(snapshot.level))  # <enum 'SnapshotLevel'>
print(snapshot.level)        # SnapshotLevel.TABLE
```

### 在事务中使用

```python
with client.transaction() as tx:
    # 在事务中使用枚举
    snapshot = tx.snapshots.create(
        name="transaction_snapshot",
        level=SnapshotLevel.DATABASE,  # 枚举在事务中
        database="production"
    )
    
    # 克隆操作
    tx.clone.clone_database_with_snapshot(
        "backup_db",
        "production",
        "transaction_snapshot"
    )
```

## API 参考

### SnapshotLevel 枚举

```python
class SnapshotLevel(Enum):
    CLUSTER = "cluster"    # 集群级别快照
    ACCOUNT = "account"    # 账户级别快照
    DATABASE = "database"  # 数据库级别快照
    TABLE = "table"        # 表级别快照
```

### 枚举方法

```python
# 获取枚举值
level = SnapshotLevel.TABLE
print(level.value)  # "table"

# 从字符串创建枚举
level = SnapshotLevel("table")  # 返回 SnapshotLevel.TABLE

# 枚举比较
if level == SnapshotLevel.TABLE:
    print("Table level snapshot")

# 枚举迭代
for level in SnapshotLevel:
    print(f"{level.name}: {level.value}")
```

### 更新的方法签名

```python
# SnapshotManager.create
def create(self, name: str, level: Union[str, SnapshotLevel], 
           database: Optional[str] = None, 
           table: Optional[str] = None, 
           description: Optional[str] = None,
           executor=None) -> Snapshot:
    """创建快照 - 支持枚举和字符串级别"""

# Snapshot.__init__
def __init__(self, name: str, level: Union[str, SnapshotLevel], 
             created_at: datetime, 
             description: Optional[str] = None, 
             database: Optional[str] = None, 
             table: Optional[str] = None):
    """快照对象 - 支持枚举级别"""
```

## 使用场景

### 1. 类型安全的快照创建

```python
# 推荐：使用枚举
snapshot = client.snapshots.create(
    name="daily_backup",
    level=SnapshotLevel.DATABASE,  # 类型安全
    database="production"
)

# 避免：使用字符串（容易出错）
snapshot = client.snapshots.create(
    name="daily_backup",
    level="databse",  # 拼写错误！
    database="production"
)
```

### 2. 条件判断

```python
snapshots = client.snapshots.list()

for snapshot in snapshots:
    if snapshot.level == SnapshotLevel.CLUSTER:
        print(f"Cluster snapshot: {snapshot.name}")
    elif snapshot.level == SnapshotLevel.TABLE:
        print(f"Table snapshot: {snapshot.name}")
        print(f"  Database: {snapshot.database}")
        print(f"  Table: {snapshot.table}")
```

### 3. 批量操作

```python
# 按级别分组快照
snapshots_by_level = {
    SnapshotLevel.CLUSTER: [],
    SnapshotLevel.ACCOUNT: [],
    SnapshotLevel.DATABASE: [],
    SnapshotLevel.TABLE: []
}

snapshots = client.snapshots.list()
for snapshot in snapshots:
    snapshots_by_level[snapshot.level].append(snapshot)

# 处理不同级别的快照
for level, level_snapshots in snapshots_by_level.items():
    print(f"{level.value} snapshots: {len(level_snapshots)}")
```

### 4. 配置驱动

```python
# 配置文件
SNAPSHOT_CONFIG = {
    "cluster": {"level": SnapshotLevel.CLUSTER, "schedule": "daily"},
    "database": {"level": SnapshotLevel.DATABASE, "schedule": "hourly"},
    "table": {"level": SnapshotLevel.TABLE, "schedule": "every_6_hours"}
}

# 使用配置
for name, config in SNAPSHOT_CONFIG.items():
    if config["level"] == SnapshotLevel.TABLE:
        # 处理表级别快照
        pass
    elif config["level"] == SnapshotLevel.DATABASE:
        # 处理数据库级别快照
        pass
```

## 错误处理

### 无效枚举值

```python
try:
    # 无效的字符串值
    snapshot = client.snapshots.create(
        name="invalid_snapshot",
        level="invalid_level"  # 无效值
    )
except SnapshotError as e:
    print(f"Error: {e}")  # "Invalid snapshot level: invalid_level"
```

### 枚举验证

```python
def validate_snapshot_level(level_str):
    """验证快照级别字符串"""
    try:
        return SnapshotLevel(level_str.lower())
    except ValueError:
        raise ValueError(f"Invalid snapshot level: {level_str}")

# 使用验证函数
try:
    level = validate_snapshot_level("table")  # 返回 SnapshotLevel.TABLE
    level = validate_snapshot_level("invalid")  # 抛出 ValueError
except ValueError as e:
    print(f"Validation error: {e}")
```

## 最佳实践

### 1. 优先使用枚举

```python
# 推荐
level = SnapshotLevel.TABLE

# 避免（除非必要）
level = "table"
```

### 2. 类型提示

```python
from typing import Union
from matrixone.snapshot import SnapshotLevel

def create_snapshot(name: str, level: Union[str, SnapshotLevel]) -> Snapshot:
    """创建快照 - 支持枚举和字符串"""
    pass
```

### 3. 枚举比较

```python
# 推荐：直接比较枚举
if snapshot.level == SnapshotLevel.TABLE:
    pass

# 避免：比较字符串值
if snapshot.level.value == "table":
    pass
```

### 4. 错误处理

```python
try:
    snapshot = client.snapshots.create(name, level)
except SnapshotError as e:
    if "Invalid snapshot level" in str(e):
        print("Please use a valid snapshot level")
        print("Available levels:", [level.value for level in SnapshotLevel])
    else:
        print(f"Snapshot creation failed: {e}")
```

## 迁移指南

### 从字符串迁移到枚举

```python
# 旧代码
snapshot = client.snapshots.create("backup", "table", database="db", table="t")

# 新代码（推荐）
snapshot = client.snapshots.create("backup", SnapshotLevel.TABLE, database="db", table="t")

# 或者保持兼容（仍然工作）
snapshot = client.snapshots.create("backup", "table", database="db", table="t")
```

### 条件判断迁移

```python
# 旧代码
if snapshot.level == "table":
    pass

# 新代码（推荐）
if snapshot.level == SnapshotLevel.TABLE:
    pass

# 或者使用值比较（仍然工作）
if snapshot.level.value == "table":
    pass
```

## 测试覆盖

### 枚举功能测试

- ✅ 枚举值验证
- ✅ 字符串到枚举转换
- ✅ 大小写不敏感处理
- ✅ 无效值错误处理
- ✅ 快照创建使用枚举
- ✅ 快照查询返回枚举
- ✅ 事务中的枚举支持
- ✅ 向后兼容性测试

### 测试结果

```
==================================================
Tests run: 57 (42 + 15)
Failures: 0
Errors: 0
Success rate: 100.0%
==================================================
```

## 总结

`SnapshotLevel` 枚举的引入为 MatrixOne Python SDK 带来了：

1. **更好的类型安全** - 编译时验证，减少运行时错误
2. **改进的开发体验** - IDE 自动补全和类型提示
3. **更高的代码质量** - 更清晰、更可维护的代码
4. **完全的向后兼容** - 现有代码无需修改
5. **更好的错误处理** - 清晰的验证和错误信息

这个改进使得快照级别的使用更加安全和直观，同时保持了与现有代码的完全兼容性。
