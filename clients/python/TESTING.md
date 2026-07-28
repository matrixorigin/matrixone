# MatrixOne Python SDK 测试指南

本指南介绍如何使用简化的测试系统来验证MatrixOne Python SDK的兼容性。

## 🎯 核心功能

我们的测试系统支持：

1. **环境检查** - 检查当前环境是否满足测试要求
2. **依赖安装** - 安装SQLAlchemy 1.4或2.0的测试依赖
3. **离线测试** - 不依赖数据库的单元测试
4. **在线测试** - 需要数据库连接的集成测试
5. **自动化测试** - 支持SQLAlchemy 1.4和2.0的矩阵测试

## 🚀 快速开始

### 1. 检查环境
```bash
# 检查当前环境是否满足测试要求
make check-env
```

### 2. 安装依赖
```bash
# 安装SQLAlchemy 1.4依赖
make install-sqlalchemy14

# 安装SQLAlchemy 2.0依赖
make install-sqlalchemy20

# 安装完整开发环境
make install
```

### 3. 运行测试
```bash
# 运行离线测试
make test-offline

# 运行在线测试（需要数据库）
make test-online

# 运行所有测试
make test
```

### 4. 自动化测试
```bash
# 运行矩阵测试（SQLAlchemy 1.4 + 2.0）
make test-matrix

# 测试特定SQLAlchemy版本
make test-sqlalchemy14
make test-sqlalchemy20
```

## 📋 可用命令

### 环境检查
- `make check-env` - 检查环境是否满足测试要求
- `make show-deps` - 显示当前依赖信息（别名）

### 依赖安装
- `make install` - 安装完整开发环境
- `make install-sqlalchemy14` - 安装SQLAlchemy 1.4依赖
- `make install-sqlalchemy20` - 安装SQLAlchemy 2.0依赖

### 测试
- `make test-offline` - 运行离线测试
- `make test-online` - 运行在线测试
- `make test` - 运行所有测试
- `make test-matrix` - 运行矩阵测试
- `make test-sqlalchemy14` - 测试SQLAlchemy 1.4
- `make test-sqlalchemy20` - 测试SQLAlchemy 2.0

### 构建和发布
- `make build` - 构建包
- `make publish-test` - 发布到测试PyPI
- `make publish` - 发布到正式PyPI

### 其他
- `make clean` - 清理构建文件
- `make help` - 显示帮助信息
- `make version` - 显示版本号

## 🔧 使用示例

### 示例1：本地开发
```bash
# 1. 检查环境
make check-env

# 2. 安装开发环境
make install

# 3. 运行离线测试
make test-offline
```

### 示例2：测试特定SQLAlchemy版本
```bash
# 1. 安装SQLAlchemy 1.4依赖
make install-sqlalchemy14

# 2. 运行测试
make test-sqlalchemy14
```

### 示例3：完整矩阵测试
```bash
# 运行所有SQLAlchemy版本的测试
make test-matrix
```

### 示例4：CI/CD环境
```bash
# 使用tox运行矩阵测试
tox

# 或使用make命令
make test-matrix
```

## 🗄️ 数据库要求

在线测试需要运行中的MatrixOne数据库：

```bash
# 检查数据库连接
python scripts/check_connection.py

# 如果数据库不可用，在线测试会被跳过
make test-online
```

## 📦 依赖文件

- `requirements.txt` - 核心依赖
- `requirements-sqlalchemy14.txt` - SQLAlchemy 1.4依赖
- `requirements-sqlalchemy20.txt` - SQLAlchemy 2.0依赖

## 🔍 故障排除

### 常见问题

1. **Python版本问题**
   ```bash
   # 检查Python版本
   make check-env
   
   # 确保使用Python 3.8+
   python --version
   ```

2. **依赖安装失败**
   ```bash
   # 重新安装依赖
   make install-sqlalchemy14
   # 或
   make install-sqlalchemy20
   ```

3. **数据库连接失败**
   ```bash
   # 检查数据库连接
   python scripts/check_connection.py
   
   # 如果数据库不可用，只运行离线测试
   make test-offline
   ```

4. **测试失败**
   ```bash
   # 检查环境
   make check-env
   
   # 运行详细测试
   python -m pytest tests/offline/ -v -s
   ```

## 🎯 最佳实践

1. **开发时**：使用 `make test-offline` 进行快速测试
2. **提交前**：运行 `make test-matrix` 确保兼容性
3. **发布前**：运行完整的在线测试
4. **CI/CD**：使用 `make test-matrix` 或 `tox` 进行自动化测试

## 📊 测试覆盖

- **离线测试**：753个测试用例，覆盖所有核心功能
- **在线测试**：集成测试，需要数据库连接
- **矩阵测试**：确保SQLAlchemy 1.4和2.0的兼容性

这个简化的测试系统提供了清晰、易用的测试工作流程，满足开发、测试和发布的所有需求。