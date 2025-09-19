# MatrixOne Python SDK - Basic Version

这是MatrixOne Python SDK的基础版本，实现了核心的连接和查询功能。

## 功能特性

- ✅ 基本的数据库连接
- ✅ SQL查询执行
- ✅ 事务支持
- ✅ SQLAlchemy集成
- ✅ 错误处理
- ✅ 连接池支持
- ✅ 快照管理
- ✅ 快照查询
- ✅ 快照上下文管理器
- ✅ 快照SQLAlchemy引擎

## 安装

```bash
# 安装依赖
pip install -r requirements.txt

# 或者直接安装
pip install PyMySQL SQLAlchemy
```

## 快速开始

### 基本连接和查询

```python
from matrixone import Client

# 创建客户端
client = Client()

# 连接到MatrixOne
client.connect(
    host="localhost",
    port=6001,
    user="root",
    password="111",
    database="test"
)

# 执行查询
result = client.execute("SELECT 1 as test_value")
print(result.fetchone())

# 断开连接
client.disconnect()
```

### 使用事务

```python
with client.transaction() as tx:
    tx.execute("INSERT INTO users (name) VALUES (%s)", ("John",))
    tx.execute("INSERT INTO users (name) VALUES (%s)", ("Jane",))
# 事务自动提交
```

### SQLAlchemy集成

```python
from sqlalchemy import text

# 获取SQLAlchemy引擎
engine = client.get_sqlalchemy_engine()

# 使用SQLAlchemy
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM users"))
    count = result.scalar()
    print(f"User count: {count}")
```

### 快照管理

```python
# 创建快照
snapshot = client.snapshots.create(
    name="daily_backup",
    level="table",
    database="test",
    table="users",
    description="Daily backup of users table"
)

# 列出所有快照
snapshots = client.snapshots.list()
for snapshot in snapshots:
    print(f"Snapshot: {snapshot.name} - {snapshot.level}")

# 快照查询
result = client.snapshot_query("daily_backup", "SELECT * FROM users")
print(f"Snapshot data: {result.fetchall()}")

# 快照上下文管理器
with client.snapshot("daily_backup") as snapshot_client:
    result = snapshot_client.execute("SELECT COUNT(*) FROM users")
    count = result.scalar()
    print(f"Snapshot user count: {count}")

# 快照查询构建器
builder = client.snapshot_query_builder("daily_backup")
result = (builder
    .select("name", "email")
    .from_table("users")
    .where("name LIKE ?", "A%")
    .execute()
)

# 快照SQLAlchemy引擎
snapshot_engine = client.get_snapshot_engine("daily_backup")
with snapshot_engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM users"))
    print(f"Snapshot data: {result.fetchall()}")
```

## 运行测试

```bash
# 基础功能测试
python test_basic.py

# 快照功能测试
python test_snapshot.py
```

## 运行示例

```bash
# 基础功能示例
python example.py

# 快照功能示例
python example_snapshot.py
```

## 项目结构

```
matrixone/
├── __init__.py          # 包初始化
├── client.py            # 主客户端类
├── exceptions.py        # 异常定义
├── snapshot.py          # 快照管理
test_basic.py            # 基础测试
test_snapshot.py         # 快照测试
example.py               # 基础使用示例
example_snapshot.py      # 快照使用示例
setup.py                 # 安装脚本
requirements.txt         # 依赖列表
```

## 下一步计划

- [x] 快照管理功能
- [ ] PITR支持
- [ ] 恢复操作
- [ ] 表克隆功能
- [ ] mo-ctl集成

## 注意事项

- 需要MatrixOne 3.0.0+版本
- 需要Python 3.10+
- 当前版本仅支持基本的CRUD操作
