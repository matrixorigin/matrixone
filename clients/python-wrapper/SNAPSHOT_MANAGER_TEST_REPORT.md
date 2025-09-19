# SnapshotManager 单元测试报告

## 概述

为 MatrixOne Python SDK 的 `SnapshotManager` 类创建了全面的单元测试，确保所有功能都能正确工作。

## 测试文件

- `test_snapshot_manager_standalone.py` - 独立的单元测试文件，不依赖外部模块

## 测试覆盖范围

### 1. Snapshot 类测试 (3个测试)
- ✅ 快照对象创建
- ✅ 快照字符串表示
- ✅ 可选参数处理

### 2. SnapshotManager 类测试 (22个测试)

#### 快照创建功能 (8个测试)
- ✅ 创建集群级别快照
- ✅ 创建账户级别快照
- ✅ 创建数据库级别快照
- ✅ 创建表级别快照
- ✅ 缺少数据库名称时的错误处理
- ✅ 缺少表参数时的错误处理
- ✅ 无效快照级别的错误处理
- ✅ 未连接时的错误处理

#### 快照列表功能 (3个测试)
- ✅ 列出所有快照
- ✅ 未连接时的错误处理
- ✅ 执行错误处理

#### 快照获取功能 (4个测试)
- ✅ 获取特定快照
- ✅ 获取不存在的快照
- ✅ 未连接时的错误处理
- ✅ 执行错误处理

#### 快照删除功能 (3个测试)
- ✅ 删除快照
- ✅ 未连接时的错误处理
- ✅ 执行错误处理

#### 快照存在性检查 (2个测试)
- ✅ 快照存在时返回 True
- ✅ 快照不存在时返回 False

#### 初始化测试 (1个测试)
- ✅ SnapshotManager 初始化

#### 快照创建执行错误 (1个测试)
- ✅ 创建快照时的执行错误处理

### 3. SnapshotQueryBuilder 类测试 (17个测试)

#### 初始化测试 (1个测试)
- ✅ SnapshotQueryBuilder 初始化

#### 查询构建方法 (12个测试)
- ✅ SELECT 列添加
- ✅ FROM 表设置
- ✅ JOIN 添加
- ✅ LEFT JOIN 添加
- ✅ RIGHT JOIN 添加
- ✅ WHERE 条件添加
- ✅ GROUP BY 列添加
- ✅ HAVING 条件添加
- ✅ ORDER BY 列添加
- ✅ LIMIT 设置
- ✅ OFFSET 设置
- ✅ 方法链式调用

#### 查询执行 (4个测试)
- ✅ 简单查询执行
- ✅ 复杂查询执行（包含所有子句）
- ✅ 缺少 SELECT 列时的错误处理
- ✅ 缺少 FROM 表时的错误处理

### 4. SnapshotManager 克隆功能测试 (17个测试)

#### 数据库克隆功能 (8个测试)
- ✅ 基本数据库克隆（无快照）
- ✅ 带 IF NOT EXISTS 的数据库克隆
- ✅ 带快照的数据库克隆
- ✅ 带快照和 IF NOT EXISTS 的数据库克隆
- ✅ 未连接时的错误处理
- ✅ 执行错误处理
- ✅ 快照验证功能
- ✅ 不存在快照的错误处理

#### 表克隆功能 (9个测试)
- ✅ 基本表克隆（无快照）
- ✅ 带数据库名的表克隆
- ✅ 带 IF NOT EXISTS 的表克隆
- ✅ 带快照的表克隆
- ✅ 带快照和 IF NOT EXISTS 的表克隆
- ✅ 未连接时的错误处理
- ✅ 执行错误处理
- ✅ 快照验证功能
- ✅ 不存在快照的错误处理

### 5. 集成测试 (3个测试)
- ✅ 完整工作流程测试
- ✅ 不同快照级别验证
- ✅ 错误传播测试

## 测试结果

```
==================================================
Tests run: 62
Failures: 0
Errors: 0
Success rate: 100.0%
==================================================
```

## 测试特点

### 1. 独立性
- 使用 Mock 对象模拟外部依赖
- 不依赖真实的数据库连接
- 可以独立运行，无需安装额外依赖

### 2. 全面性
- 覆盖所有公共方法
- 测试正常情况和异常情况
- 包含边界条件测试

### 3. 可维护性
- 清晰的测试结构
- 详细的测试文档
- 易于扩展新测试

### 4. 错误处理
- 测试所有异常情况
- 验证错误消息的正确性
- 确保错误正确传播

## 测试方法

### Mock 使用
- 使用 `unittest.mock.Mock` 模拟客户端
- 模拟数据库连接和执行结果
- 模拟异常情况

### 断言验证
- 验证方法调用参数
- 验证返回值
- 验证异常抛出

### 测试隔离
- 每个测试方法独立
- 使用 `setUp` 方法初始化测试环境
- 避免测试间的相互影响

## 运行测试

```bash
# 激活 conda 环境
conda activate ai_env

# 进入测试目录
cd /Users/xupeng/github/matrixone/clients/python-wrapper

# 运行测试
python test_snapshot_manager_standalone.py
```

## 测试覆盖的功能点

### SnapshotManager 核心功能
1. **快照创建** - 支持 cluster、account、database、table 四个级别
2. **快照列表** - 从系统表查询所有快照
3. **快照获取** - 根据名称获取特定快照
4. **快照删除** - 删除指定快照
5. **快照存在性检查** - 检查快照是否存在
6. **数据库克隆** - 支持带快照和不带快照的数据库克隆
7. **表克隆** - 支持带快照和不带快照的表克隆

### SnapshotQueryBuilder 功能
1. **查询构建** - 支持所有 SQL 子句
2. **方法链式调用** - 流畅的 API 设计
3. **参数化查询** - 支持参数绑定
4. **快照查询** - 自动添加快照提示

### 错误处理
1. **连接错误** - 未连接数据库时的处理
2. **参数验证** - 必需参数缺失时的处理
3. **执行错误** - 数据库执行失败时的处理
4. **业务逻辑错误** - 快照不存在等业务错误
5. **克隆错误** - 克隆操作失败时的处理
6. **快照验证错误** - 克隆时快照不存在时的处理

## 新增克隆功能

### 克隆功能特性
1. **数据库克隆**
   - 基本克隆：`clone_database(target_db, source_db)`
   - 带快照克隆：`clone_database(target_db, source_db, snapshot_name="snap1")`
   - 安全克隆：`clone_database_with_snapshot(target_db, source_db, snapshot_name)`
   - 支持 IF NOT EXISTS 选项

2. **表克隆**
   - 基本克隆：`clone_table(target_table, source_table)`
   - 跨数据库克隆：`clone_table("db1.table1", "db2.table2")`
   - 带快照克隆：`clone_table(target_table, source_table, snapshot_name="snap1")`
   - 安全克隆：`clone_table_with_snapshot(target_table, source_table, snapshot_name)`
   - 支持 IF NOT EXISTS 选项

3. **快照验证**
   - 自动验证快照存在性
   - 提供清晰的错误信息
   - 防止使用不存在的快照进行克隆

### 使用示例
```python
# 基本数据库克隆
client.snapshots.clone_database("backup_db", "production_db")

# 带快照的表克隆
client.snapshots.clone_table_with_snapshot(
    "backup_table", 
    "production_db.users", 
    "daily_snapshot"
)

# 安全克隆（自动验证快照）
client.snapshots.clone_database_with_snapshot(
    "test_db", 
    "production_db", 
    "test_snapshot"
)
```

## 总结

为 `SnapshotManager` 创建了 62 个全面的单元测试，覆盖了所有核心功能、克隆功能和边界情况。测试成功率达到 100%，确保了代码的可靠性和稳定性。新增的克隆功能为数据库和表的复制提供了强大的支持，特别适合开发、测试和备份场景。这些测试为后续的代码维护和功能扩展提供了坚实的基础。
