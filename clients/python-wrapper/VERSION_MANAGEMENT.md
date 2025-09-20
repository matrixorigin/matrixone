# MatrixOne Python SDK - Version Management Framework

## 概述

MatrixOne Python SDK 现在包含了一个强大的版本管理框架，用于处理后端版本兼容性检查和提供有用的错误提示。这个框架支持语义版本控制（如 3.0.1），并提供了自动版本检测、功能可用性检查和版本感知的错误消息。

## 主要特性

### 1. 版本解析和比较
- 支持语义版本格式：`major.minor.patch`（如 3.0.1）
- 版本比较：3.0.2 > 3.0.1，2.1.19 < 3.0.9
- 版本兼容性检查：>=、>、<=、<、==、!= 操作符

### 2. 自动后端版本检测
- 连接时自动检测 MatrixOne 后端版本
- 支持通过 `version()` 和 `git_version()` 函数检测
- 手动版本设置功能

### 3. 功能可用性检查
- 注册功能的最低/最高版本要求
- 实时检查功能在当前后端版本中的可用性
- 提供替代方案建议

### 4. 版本检查装饰器
- 为方法添加版本要求装饰器
- 自动版本兼容性检查
- 版本不兼容时抛出有意义的错误

### 5. 智能错误提示
- 详细的版本兼容性错误消息
- 提供替代方案和建议
- 上下文相关的错误信息

## MatrixOne 版本格式支持

MatrixOne 版本管理框架支持以下版本格式：

### 开发版本
- **格式**: `8.0.30-MatrixOne-v` (v后面为空)
- **解析结果**: `999.0.0` (最高优先级)
- **说明**: 开发版本具有最高优先级，支持所有功能

### 正式版本
- **格式**: `8.0.30-MatrixOne-v3.0.0` (v后面有版本号)
- **解析结果**: `3.0.0`
- **说明**: 正式版本，按照语义版本规则进行比较

### 兼容格式
- **格式**: `MatrixOne 3.0.1`, `Version 2.5.0`, `3.0.1`
- **解析结果**: 提取的版本号
- **说明**: 向后兼容的格式支持

## 快速开始

### 基本使用

```python
from matrixone import Client, VersionManager, VersionInfo, FeatureRequirement

# 创建客户端
client = Client()

# 连接到 MatrixOne（版本会自动检测）
client.connect('localhost', 6001, 'root', 'password', 'test')

# 获取检测到的后端版本
version = client.get_backend_version()
print(f"后端版本: {version}")

# 检查是否为开发版本
if client.is_development_version():
    print("当前是开发版本，支持所有功能")
else:
    print(f"当前是正式版本: {version}")

# 检查功能可用性
if client.is_feature_available('snapshot_creation'):
    print("快照创建功能可用")
else:
    hint = client.get_version_hint('snapshot_creation')
    print(f"快照创建不可用: {hint}")

# 检查版本兼容性
if client.check_version_compatibility('3.0.0', '>='):
    print("后端支持 3.0.0+ 版本的功能")
```

### 版本管理 API

```python
# 手动设置后端版本
client.set_backend_version('3.0.1')

# 检查版本兼容性
compatible = client.check_version_compatibility('2.5.0', '>=')
print(f"与 2.5.0+ 兼容: {compatible}")

# 获取功能信息
info = client.get_feature_info('snapshot_creation')
if info:
    print(f"功能信息: {info}")

# 获取版本提示
hint = client.get_version_hint('advanced_feature', '调用高级功能')
print(f"提示: {hint}")
```

## 版本检查装饰器

### 基本装饰器使用

```python
from matrixone.version import requires_version

class MyMatrixOneClient:
    @requires_version(
        min_version="3.0.0",
        feature_name="premium_feature",
        description="高级功能需要 3.0.0+ 版本",
        alternative="使用 basic_feature() 代替"
    )
    def premium_feature(self):
        return "高级功能执行成功！"
    
    @requires_version(
        min_version="2.0.0",
        max_version="2.9.9",
        feature_name="legacy_feature",
        description="遗留功能仅在 2.x 版本中可用",
        alternative="使用 new_feature() 代替"
    )
    def legacy_feature(self):
        return "遗留功能执行成功！"
```

### 装饰器参数说明

- `min_version`: 最低支持版本（如 "3.0.0"）
- `max_version`: 最高支持版本（如 "3.5.0"）
- `feature_name`: 功能名称（默认使用函数名）
- `description`: 功能描述
- `alternative`: 替代方案建议
- `raise_error`: 是否在版本不兼容时抛出错误（默认 True）

## 自定义功能要求

### 注册功能要求

```python
from matrixone.version import VersionManager, FeatureRequirement, VersionInfo

version_manager = VersionManager()

# 注册自定义功能要求
feature = FeatureRequirement(
    feature_name="my_custom_feature",
    min_version=VersionInfo(3, 0, 0),
    max_version=VersionInfo(3, 5, 0),
    description="我的自定义功能",
    alternative="使用 standard_feature() 代替"
)

version_manager.register_feature_requirement(feature)

# 检查功能可用性
if version_manager.is_feature_available("my_custom_feature"):
    print("自定义功能可用")
```

## 版本比较示例

```python
from matrixone.version import VersionManager

vm = VersionManager()

# 版本比较
result = vm.compare_versions("3.0.2", "3.0.1")
print(f"3.0.2 vs 3.0.1: {result}")  # GREATER

# 版本兼容性检查
vm.set_backend_version("3.0.1")

# 检查各种操作符
print(f">= 3.0.0: {vm.is_version_compatible('3.0.0', '>=')}")  # True
print(f"> 3.0.0: {vm.is_version_compatible('3.0.0', '>')}")    # True
print(f">= 3.0.1: {vm.is_version_compatible('3.0.1', '>=')}")  # True
print(f"> 3.0.1: {vm.is_version_compatible('3.0.1', '>')}")    # False
print(f"== 3.0.1: {vm.is_version_compatible('3.0.1', '==')}")  # True
```

## 错误处理

### 版本错误示例

```python
from matrixone.exceptions import VersionError

try:
    # 尝试使用需要更高版本的功能
    client.snapshots.create('my_snapshot', 'cluster')
except VersionError as e:
    print(f"版本错误: {e}")
    # 输出详细的错误信息和建议
```

### 错误消息示例

```
VersionError: Feature 'premium_feature' is not available in current backend version.
Feature 'premium_feature' requires backend version 3.0.0 or higher, but current version is 2.5.0
Alternative: 使用 basic_feature() 代替
Description: 高级功能需要 3.0.0+ 版本
Context: 调用 premium_feature() 方法
```

## 预定义功能要求

SDK 已经为以下功能预定义了版本要求：

- `snapshot_cluster_level`: 集群级快照功能
- `snapshot_account_level`: 账户级快照功能
- `pitr_cluster_level`: 集群级 PITR 功能
- `pubsub_publications`: 发布订阅功能
- `account_management`: 账户管理功能
- `database_cloning`: 数据库克隆功能

## 异步客户端支持

异步客户端也完全支持版本管理：

```python
from matrixone import AsyncClient

async def main():
    client = AsyncClient()
    
    # 连接（版本会自动检测）
    await client.connect('localhost', 6001, 'root', 'password', 'test')
    
    # 版本管理 API 与同步客户端相同
    version = client.get_backend_version()
    print(f"后端版本: {version}")
    
    # 版本感知的异步方法
    try:
        snapshot = await client.snapshots.create('my_snapshot', 'cluster')
        print(f"创建快照: {snapshot}")
    except VersionError as e:
        print(f"快照创建失败: {e}")
    
    await client.disconnect()

# 运行异步示例
import asyncio
asyncio.run(main())
```

## 最佳实践

### 1. 版本检测
- 在连接后立即检查后端版本
- 为关键功能添加版本检查
- 提供降级方案

### 2. 错误处理
- 捕获 `VersionError` 异常
- 向用户显示有意义的错误消息
- 提供替代方案

### 3. 功能检查
- 在调用新功能前检查可用性
- 为不同版本提供不同的实现
- 记录版本兼容性信息

### 4. 装饰器使用
- 为所有版本敏感的方法添加装饰器
- 提供清晰的错误消息和替代方案
- 测试不同版本的兼容性

## 测试

运行版本管理测试：

```bash
python test_version_management.py
```

运行示例：

```bash
python example_10_version_management.py
```

## 总结

MatrixOne Python SDK 的版本管理框架提供了：

1. **自动版本检测**：连接时自动检测后端版本
2. **智能兼容性检查**：检查功能在当前版本中的可用性
3. **有用的错误提示**：提供详细的错误信息和替代方案
4. **版本检查装饰器**：简化版本敏感方法的实现
5. **灵活的功能要求**：支持自定义功能版本要求
6. **完整的测试覆盖**：确保框架的可靠性

这个框架确保了 SDK 与不同版本的 MatrixOne 后端的兼容性，并提供了良好的开发体验和用户反馈。
