# ReadTheDocs 配置说明

本项目已配置 ReadTheDocs 支持，可以自动构建和发布在线文档。

## 配置文件

- `.readthedocs.yaml` - ReadTheDocs v2 配置文件
- `docs/conf.py` - Sphinx 文档配置
- `docs/requirements.txt` - 文档构建依赖
- `docs/index.rst` - 文档主入口

## 本地构建文档

### 安装依赖

```bash
# 安装文档依赖
pip install -e ".[docs]"

# 或者直接安装 docs requirements
pip install -r docs/requirements.txt
```

### 构建 HTML 文档

```bash
cd docs
make html
```

文档将生成在 `docs/_build/html/` 目录下。

### 实时预览

```bash
cd docs
sphinx-autobuild . _build/html
```

然后在浏览器中打开 http://127.0.0.1:8000 即可实时预览。

## ReadTheDocs 在线配置步骤

### 1. 导入项目

1. 访问 https://readthedocs.org/
2. 登录你的账号（支持 GitHub 登录）
3. 点击 "Import a Project"
4. 选择 `matrixone` 仓库
5. 填写项目信息：
   - Name: `matrixone`（已配置）
   - Repository URL: `https://github.com/matrixorigin/matrixone`
   - Repository type: Git
   - Default branch: `main`

### 2. 高级设置

在项目设置页面（Admin -> Advanced Settings）：

- **Documentation type**: Sphinx Html
- **Language**: zh_CN 或 en
- **Programming Language**: Python
- **Default version**: latest
- **Privacy Level**: Public

### 3. 环境变量（如需要）

在 Admin -> Environment Variables 设置：

```
READTHEDOCS=True
```

### 4. 自定义域名（可选）

在 Admin -> Domains 可以设置自定义域名。

## 构建流程

ReadTheDocs 会在以下情况自动构建文档：

1. Push 到默认分支（main）
2. 创建新的 tag
3. 创建 Pull Request（需启用）

### 构建步骤

1. 检出代码仓库
2. 安装 Python 3.10（为了更好的依赖兼容性）
3. 安装项目依赖（根据仓库根目录的 `.readthedocs.yaml` 配置）
   - 安装 `clients/python/` 目录下的 Python 包及文档依赖
   - 安装 `clients/python/docs/requirements.txt` 中的依赖
4. 使用 Sphinx 从 `clients/python/docs/` 构建文档
5. 发布到 ReadTheDocs 服务器

## 版本管理

ReadTheDocs 支持多版本文档：

- **latest** - 默认分支的最新版本
- **stable** - 最新的稳定版本（通常是最新的 tag）
- **版本号** - 每个 git tag 都会生成对应版本的文档

## 文档 URL

构建成功后，文档将发布到：

- 主站: https://matrixone.readthedocs.io/
- 英文版: https://matrixone.readthedocs.io/en/latest/
- 中文版: https://matrixone.readthedocs.io/zh_CN/latest/
- 特定版本: https://matrixone.readthedocs.io/en/v1.0.0/

## 徽章（Badge）

在 README 中添加构建状态徽章（已完成）：

```markdown
[![Documentation Status](https://app.readthedocs.org/projects/matrixone/badge/?version=latest)](https://matrixone.readthedocs.io/en/latest/?badge=latest)
```

## 故障排查

### 构建失败

1. 查看构建日志：Project -> Builds -> 选择失败的构建 -> View raw
2. 检查 `clients/python/docs/requirements.txt` 中的依赖是否正确
3. 本地测试构建：`cd clients/python/docs && make html`
4. 检查仓库根目录的 `.readthedocs.yaml` 配置是否正确，确保路径都指向 `clients/python/` 子目录

### 文档不更新

1. 检查 webhook 是否正确配置（Settings -> Integrations）
2. 手动触发构建：Project -> Builds -> Build Version
3. 清除缓存：Project -> Admin -> Advanced Settings -> Wipe Environment

### 依赖安装失败

1. 确保 `docs/requirements.txt` 中的版本约束正确
2. 检查 Python 版本兼容性
3. 查看构建日志中的具体错误信息

## 更多资源

- [ReadTheDocs 官方文档](https://docs.readthedocs.io/)
- [Sphinx 文档](https://www.sphinx-doc.org/)
- [reStructuredText 语法](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html)

