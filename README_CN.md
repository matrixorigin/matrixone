<div class="column" align="middle">
  <p align="center">
   <img alt="MatrixOne All in One" height="50" src="https://github.com/matrixorigin/artwork/blob/main/docs/overview/logo.png?raw=true">
  </p>
  <a href="https://github.com/matrixorigin/matrixone/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/License-Apache%202.0-red.svg" alt="license"/>
  </a>
  <a href="https://golang.org/">
    <img src="https://img.shields.io/badge/Language-Go-blue.svg" alt="language"/>
  </a>
  <img src="https://img.shields.io/badge/platform-MacOS-white.svg" alt="macos"/>
  <img src="https://img.shields.io/badge/platform-Linux-9cf.svg" alt="linux"/>
  <a href="https://www.codefactor.io/repository/github/matrixorigin/matrixone">
    <img src="https://www.codefactor.io/repository/github/matrixorigin/matrixone/badge?s=7280f4312fca2f2e6938fb8de5b726c5252541f0" alt="codefactor"/>
  </a>
  <a href="https://docs.matrixorigin.cn/en/latest/MatrixOne/Overview/whats-new/">
   <img src="https://img.shields.io/badge/Release-latest-green.svg" alt="release"/>
  </a>
  <br>
  <img src="https://img.shields.io/badge/MySQL-Compatible-4479A1.svg?logo=mysql&logoColor=white" alt="mysql-compatible"/>
  <img src="https://img.shields.io/badge/AI-Native-FF6B6B.svg?logo=openai&logoColor=white" alt="ai-native"/>
  <img src="https://img.shields.io/badge/Cloud-Native-326CE5.svg?logo=kubernetes&logoColor=white" alt="cloud-native"/>
  <br>
  <a href="https://docs.matrixorigin.cn/latest/">
    <b>Docs</b>
  </a>
  <b>||</b>
  <a href="https://www.matrixorigin.cn/">
   <b> Official Website</b>
  </a>
  <br>
  <a href="https://github.com/matrixorigin/matrixone/blob/main/README.md">
    <b>English</b>
  </a>
  <b>||</b>
  <a href="https://github.com/matrixorigin/matrixone/blob/main/README_CN.md">
    <b>简体中文</b>
  </a>
</div>

<h3 align="center">加入 MatrixOne 社区</h3>
<p align="center">
<a href="https://26805113.s21i.faiusr.com/4/ABUIABAEGAAgv7rJjwYo977zvgQw1AY41AY!200x200.png.webp" target="blank"><img align="center" src="https://github.com/dengn/CASAUVSQ/blob/priority/wechat-logo.png" alt="matrixone16" height="30" width="30" /></a>
<a href="http://matrixoneworkspace.slack.com" target="blank"><img align="center" src="https://github.com/dengn/CASAUVSQ/blob/priority/slack_icon.png" alt="matrixone16" height="30" width="30" /></a>

</p>

<h5 align="center">如果你对 MatrixOne 项目感兴趣的话，请帮忙给 MatrixOne 点击 Star， Fork 和 Watch 三连吧，谢谢！</h5>

目录
========

* [MatrixOne 是什么？](#what-is-matrixone)
* [60秒快速上手](#️-60秒快速上手)
* [教程与示例](#-教程与示例)
* [安装与部署](#️-安装与部署)
* [架构](#architecture)
* [Python SDK](#python-sdk)
* [参与贡献](#contributing)
* [参考引用](#reference)
* [License](#license)

## <a id="what-is-matrixone">MatrixOne 是什么？</a>

**MatrixOne 是业界首个将 Git 风格版本控制引入数据库的产品**，同时具备 MySQL 兼容、AI 原生和云原生架构。

作为一款 **HTAP（混合事务/分析处理）数据库**，MatrixOne 采用超融合的 **HSTAP 引擎**，在单一系统中无缝处理事务型（OLTP）、分析型（OLAP）、全文检索和向量检索等多种工作负载——无需数据迁移、无需 ETL、无需妥协。

### 🎬 **Git for Data - 革命性创新**

正如 Git 革新了代码管理，MatrixOne 革新了数据管理。**像管理代码一样管理数据库：**

- **📸 即时快照** - 毫秒级零拷贝快照，无存储膨胀
- **⏰ 时间旅行** - 查询任意历史时刻的数据状态
- **🔀 分支与合并** - 在隔离分支中测试迁移和转换
- **↩️ 即时回滚** - 无需完整备份即可恢复到任意状态
- **🔍 完整审计追踪** - 不可变历史记录追踪每次数据变更

**为什么重要：** 数据错误代价高昂。Git for Data 为您的最关键资产（数据）提供了开发者在 Git 中享有的安全性和灵活性。

---

### 🎯 **为 AI 时代而生**

<table>
<tr>
<td width="33%" valign="top">

**🗄️ MySQL 兼容**

MySQL 8.0 的直接替代品。使用现有工具、ORM 和应用程序无需修改代码。无缝迁移路径。

</td>
<td width="33%" valign="top">

**🤖 AI 原生**

内置向量检索（IVF/HNSW）和全文检索。直接构建 RAG 应用和语义搜索——无需外部向量数据库。

</td>
<td width="33%" valign="top">

**☁️ 云原生**

存算分离。随处部署。弹性扩展。Kubernetes 原生。零停机运维。

</td>
</tr>
</table>

---

### 🚀 **一个数据库替代所有**

MatrixOne 的 **HSTAP 引擎**整合您的整个数据基础设施：

**传统方案：**
```
MySQL (OLTP) → ETL → ClickHouse (OLAP) → ETL → Elasticsearch (检索)
                                             → Vector DB (AI)
```
4 个数据库 · 多条 ETL 管道 · 数据不一致 · 运维复杂

**MatrixOne 方案：**
```
MatrixOne (OLTP + OLAP + 全文检索 + 向量检索)
```
1 个数据库 · 无需 ETL · 实时一致 · ACID 合规

**核心优势：**
- 🎯 **简单** - 一套系统部署、监控和维护
- ⚡ **实时** - 无数据延迟，即时分析
- 🔒 **一致** - 单一数据源，无同步问题
- 💰 **高效** - 共享基础设施，无数据重复

<p align="center">
  <img alt="MatrixOne" height="450" src="https://github.com/matrixorigin/artwork/blob/main/docs/overview/architecture/architeture241113_en.png?raw=true">
</p>

## ⚡️ 60秒快速上手

### 1️⃣ 启动 MatrixOne

```bash
docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:latest
```

### 2️⃣ 创建数据库

```bash
mysql -h127.0.0.1 -P6001 -p111 -uroot -e "create database demo"
```

### 3️⃣ 连接与查询

**安装 Python SDK：**
```bash
pip install matrixone-python-sdk
```

**向量检索示例：**
```python
from matrixone import Client

# 连接到 MatrixOne（使用默认配置）
client = Client()
client.connect(database='demo')

# 创建带向量列的表
client.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        id INT PRIMARY KEY,
        title VARCHAR(100),
        embedding VECF32(16)  -- 16维向量
    )
""")

# 创建 IVF 索引以加速相似度搜索
client.execute("CREATE INDEX idx_vec ON documents(embedding) USING IVF")

# 插入示例向量数据
client.execute("""
    INSERT INTO documents VALUES
    (1, 'AI 数据库', '[0.1, 0.2, 0.3, 0.15, 0.25, 0.35, 0.12, 0.22, 0.18, 0.28, 0.13, 0.23, 0.17, 0.27, 0.14, 0.24]'),
    (2, '向量检索', '[0.2, 0.3, 0.4, 0.25, 0.35, 0.45, 0.22, 0.32, 0.28, 0.38, 0.23, 0.33, 0.27, 0.37, 0.24, 0.34]'),
    (3, '时序数据', '[0.5, 0.1, 0.2, 0.45, 0.15, 0.25, 0.42, 0.12, 0.48, 0.18, 0.43, 0.13, 0.47, 0.17, 0.44, 0.14]')
""")

# 使用余弦相似度查找相似文档
query_vector = "[0.15, 0.25, 0.35, 0.2, 0.3, 0.4, 0.17, 0.27, 0.23, 0.33, 0.18, 0.28, 0.22, 0.32, 0.19, 0.29]"
results = client.query(f"""
    SELECT id, title, cosine_similarity(embedding, {query_vector}) AS similarity
    FROM documents
    ORDER BY similarity DESC
    LIMIT 3
""")
print(results)
```

**全文检索示例：**
```python
...
from matrixone.sqlalchemy_ext import boolean_match

# 使用 SDK 创建全文索引
client.fulltext_index.create(
    Article, name='ftidx_content', columns=['title', 'content']
)

# 布尔搜索，支持 must/should 操作符
results = client.query(
    Article.title,
    Article.content,
    boolean_match('title', 'content')
        .must('机器')
        .must('学习')
        .must_not('入门')
).execute()

# 结果是 ResultSet 对象
for row in results.rows:
    print(f"标题: {row[0]}, 内容: {row[1][:50]}...")
...
```

**完成！** 🎉 您现在已运行一个生产级数据库，具备类 Git 快照、向量检索和完整 ACID 合规性。

> 💡 **需要更多控制？** 查看下方的 [安装与部署](#️-安装与部署) 章节了解生产级安装选项。

📖 **[Python SDK 文档 →](clients/python/README.md)**

## 📚 教程与示例

深入了解 MatrixOne！浏览我们全面的实践教程和真实案例：

### 🎯 入门教程

| 教程 | 语言/框架 | 说明 |
|----------|-------------------|-------------|
| [Java CRUD 示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/develop-java-crud-demo/) | Java | Java 应用开发 |
| [SpringBoot 和 JPA CRUD 示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/springboot-hibernate-crud-demo/) | Java | SpringBoot + Hibernate/JPA |
| [PyMySQL CRUD 示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/develop-python-crud-demo/) | Python | Python 基础数据库操作 |
| [SQLAlchemy CRUD 示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/sqlalchemy-python-crud-demo/) | Python | Python + SQLAlchemy ORM |
| [Django CRUD 示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/django-python-crud-demo/) | Python | Django Web 框架 |
| [Golang CRUD 示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/develop-golang-crud-demo/) | Go | Go 应用开发 |
| [Gorm CRUD 示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/gorm-golang-crud-demo/) | Go | Go + Gorm ORM |
| [C# CRUD 示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/c-net-crud-demo/) | C# | .NET 应用开发 |
| [TypeScript CRUD 示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/typescript-crud-demo/) | TypeScript | TypeScript 应用开发 |

### 🚀 高级功能教程

| 教程 | 使用场景 | 相关 MatrixOne 特性 |
|----------|----------|---------------------------|
| [Pinecone 兼容向量检索](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/pinecone-vector-demo/) | AI 与搜索 | 向量检索，Pinecone 兼容 API |
| [IVF 索引健康监控](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/ivf-index-health-demo/) | AI 与搜索 | 向量检索，IVF 索引 |
| [HNSW 向量索引](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/hnsw-vector-demo/) | AI 与搜索 | 向量检索，HNSW 索引 |
| [全文自然语言搜索](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/fulltext-natural-search-demo/) | AI 与搜索 | 全文检索，自然语言 |
| [全文布尔搜索](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/fulltext-boolean-search-demo/) | AI 与搜索 | 全文检索，布尔运算符 |
| [全文 JSON 搜索](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/fulltext-json-search-demo/) | AI 与搜索 | 全文检索，JSON 数据 |
| [混合搜索](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/hybrid-search-demo/) | AI 与搜索 | 混合搜索，向量+全文+SQL |
| [RAG 应用示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/rag-demo/) | AI 与搜索 | RAG，向量检索，全文检索 |
| [图文搜索应用](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/search-picture-demo/) | AI 与搜索 | 多模态搜索，图像相似度 |
| [Dify 集成示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/dify-mo-demo/) | AI 与搜索 | AI 平台集成 |
| [HTAP 应用示例](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/htap-demo/) | 性能 | HTAP，实时分析 |
| [多团队开发即时克隆](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/efficient-clone-demo/) | 性能 | 即时克隆，Git for Data |
| [生产环境安全升级与即时回滚](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/snapshot-rollback-demo/) | 性能 | 快照，回滚，Git for Data |

📖 **[查看所有教程 →](https://docs.matrixorigin.cn/en/v25.3.0.2/MatrixOne/Tutorial/snapshot-rollback-demo/)**

## 🛠️ <a id="installation--deployment">安装与部署</a>

MatrixOne 支持多种安装方式，选择最适合您需求的方式：

### 🐳 本地多 CN 开发环境

在本地运行完整的分布式集群，包含多个 CN 节点、负载均衡和便捷的配置管理。

```bash
# 快速开始
make dev-build && make dev-up

# 通过代理连接（负载均衡）
mysql -h 127.0.0.1 -P 6001 -u root -p111

# 配置特定服务（交互式编辑器）
make dev-edit-cn1          # 编辑 CN1 配置
make dev-restart-cn1       # 仅重启 CN1（快速！）
```

📖 **[完整开发指南 →](etc/DEV_README.md)** - 涵盖单机设置、多 CN 集群、监控、指标、配置和所有 `make dev-*` 命令的完整指南

### 🎯 使用 mo_ctl 工具（推荐）

官方 [mo_ctl](https://github.com/matrixorigin/mo_ctl_standalone) 工具提供一键部署和生命周期管理。自动处理安装、升级、备份和健康监控。

📖 **[完整 mo_ctl 安装指南 →](INSTALLATION.md#using-moctl-tool)**

### ⚙️ 从源码构建

从源码构建 MatrixOne，适用于开发、定制或参与贡献。需要 Go 1.22、GCC/Clang、Git 和 Make。

📖 **[完整源码构建指南 →](BUILD.md)**

### 🐳 其他方式

Docker、Kubernetes、二进制包等更多部署选项。

📖 **[所有安装选项 →](INSTALLATION.md)**

## 🔎 <a id="architecture">架构一览</a>

MatrixOne 的架构图如下图所示：
<p align="center">
  <img alt="MatrixOne" height="500" src="https://community-shared-data-1308875761.cos.ap-beijing.myqcloud.com/artwork/docs/Release-Notes/release-notes-1.1.0.png">
</p>

关于更详细的 MatrixOne 技术架构，可以参考[MatrixOne 架构设计](https://docs.matrixorigin.cn/latest/MatrixOne/Overview/architecture/matrixone-architecture-design/)。

## 🐍 <a id="python-sdk">Python SDK</a>

MatrixOne 提供**全面的 Python SDK**，支持数据库操作、向量搜索、全文搜索以及快照、PITR、账户管理等高级功能。

**核心特性**：高性能 async/await 支持、向量相似度搜索（IVF/HNSW 索引）、全文搜索、元数据分析和完整的类型安全。

📚 **[完整文档](https://matrixone.readthedocs.io/)** [![Documentation Status](https://app.readthedocs.org/projects/matrixone/badge/?version=latest)](https://matrixone.readthedocs.io/en/latest/)

📖 **[Python SDK README](clients/python/README.md)** - 功能详情、安装和使用指南

📦 **安装**: `pip install matrixone-python-sdk`

### 👏贡献者

<!-- readme: contributors -start -->
<table>
<tr>
    <td align="center">
        <a href="https://github.com/nnsgmsone">
            <img src="https://avatars.githubusercontent.com/u/31609524?v=4" width="30;" alt="nnsgmsone"/>
            <br />
            <sub><b>Nnsgmsone</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/XuPeng-SH">
            <img src="https://avatars.githubusercontent.com/u/39627130?v=4" width="30;" alt="XuPeng-SH"/>
            <br />
            <sub><b>XuPeng-SH</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/zhangxu19830126">
            <img src="https://avatars.githubusercontent.com/u/2995754?v=4" width="30;" alt="zhangxu19830126"/>
            <br />
            <sub><b>Fagongzi</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/reusee">
            <img src="https://avatars.githubusercontent.com/u/398457?v=4" width="30;" alt="reusee"/>
            <br />
            <sub><b>Reusee</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/ouyuanning">
            <img src="https://avatars.githubusercontent.com/u/45346669?v=4" width="30;" alt="ouyuanning"/>
            <br />
            <sub><b>Ouyuanning</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/daviszhen">
            <img src="https://avatars.githubusercontent.com/u/60595215?v=4" width="30;" alt="daviszhen"/>
            <br />
            <sub><b>Daviszhen</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/aunjgr">
            <img src="https://avatars.githubusercontent.com/u/523063?v=4" width="30;" alt="aunjgr"/>
            <br />
            <sub><b>BRong Njam</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/sukki37">
            <img src="https://avatars.githubusercontent.com/u/77312370?v=4" width="30;" alt="sukki37"/>
            <br />
            <sub><b>Maomao</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/iamlinjunhong">
            <img src="https://avatars.githubusercontent.com/u/49111204?v=4" width="30;" alt="iamlinjunhong"/>
            <br />
            <sub><b>Iamlinjunhong</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/jiangxinmeng1">
            <img src="https://avatars.githubusercontent.com/u/51114574?v=4" width="30;" alt="jiangxinmeng1"/>
            <br />
            <sub><b>Jiangxinmeng1</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/jianwan0214">
            <img src="https://avatars.githubusercontent.com/u/32733096?v=4" width="30;" alt="jianwan0214"/>
            <br />
            <sub><b>Jianwan0214</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/LeftHandCold">
            <img src="https://avatars.githubusercontent.com/u/14086886?v=4" width="30;" alt="LeftHandCold"/>
            <br />
            <sub><b>GreatRiver</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/w-zr">
            <img src="https://avatars.githubusercontent.com/u/28624654?v=4" width="30;" alt="w-zr"/>
            <br />
            <sub><b>Wei Ziran</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/m-schen">
            <img src="https://avatars.githubusercontent.com/u/59043531?v=4" width="30;" alt="m-schen"/>
            <br />
            <sub><b>Chenmingsong</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/dengn">
            <img src="https://avatars.githubusercontent.com/u/4965857?v=4" width="30;" alt="dengn"/>
            <br />
            <sub><b>Dengn</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/aptend">
            <img src="https://avatars.githubusercontent.com/u/49832303?v=4" width="30;" alt="aptend"/>
            <br />
            <sub><b>Aptend</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/lni">
            <img src="https://avatars.githubusercontent.com/u/30930154?v=4" width="30;" alt="lni"/>
            <br />
            <sub><b>Lni</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/xzxiong">
            <img src="https://avatars.githubusercontent.com/u/3927687?v=4" width="30;" alt="xzxiong"/>
            <br />
            <sub><b>Jackson</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/YANGGMM">
            <img src="https://avatars.githubusercontent.com/u/26563383?v=4" width="30;" alt="YANGGMM"/>
            <br />
            <sub><b>YANGGMM</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/qingxinhome">
            <img src="https://avatars.githubusercontent.com/u/70939751?v=4" width="30;" alt="qingxinhome"/>
            <br />
            <sub><b>Qingxinhome</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/badboynt1">
            <img src="https://avatars.githubusercontent.com/u/112734932?v=4" width="30;" alt="badboynt1"/>
            <br />
            <sub><b>Nitao</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/broccoliSpicy">
            <img src="https://avatars.githubusercontent.com/u/93440049?v=4" width="30;" alt="broccoliSpicy"/>
            <br />
            <sub><b>BroccoliSpicy</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/mooleetzi">
            <img src="https://avatars.githubusercontent.com/u/42628885?v=4" width="30;" alt="mooleetzi"/>
            <br />
            <sub><b>Mooleetzi</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/fengttt">
            <img src="https://avatars.githubusercontent.com/u/169294?v=4" width="30;" alt="fengttt"/>
            <br />
            <sub><b>Fengttt</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/zzl200012">
            <img src="https://avatars.githubusercontent.com/u/57308069?v=4" width="30;" alt="zzl200012"/>
            <br />
            <sub><b>Kutori</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/lacrimosaprinz">
            <img src="https://avatars.githubusercontent.com/u/43231571?v=4" width="30;" alt="lacrimosaprinz"/>
            <br />
            <sub><b>Prinz</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/guguducken">
            <img src="https://avatars.githubusercontent.com/u/22561920?v=4" width="30;" alt="guguducken"/>
            <br />
            <sub><b>Brown</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/dongdongyang33">
            <img src="https://avatars.githubusercontent.com/u/47596332?v=4" width="30;" alt="dongdongyang33"/>
            <br />
            <sub><b>Dongdongyang</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/JackTan25">
            <img src="https://avatars.githubusercontent.com/u/60096118?v=4" width="30;" alt="JackTan25"/>
            <br />
            <sub><b>Boyu Tan</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/cnutshell">
            <img src="https://avatars.githubusercontent.com/u/20291742?v=4" width="30;" alt="cnutshell"/>
            <br />
            <sub><b>Cui Guoke</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/JinHai-CN">
            <img src="https://avatars.githubusercontent.com/u/33142505?v=4" width="30;" alt="JinHai-CN"/>
            <br />
            <sub><b>Jin Hai</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/lignay">
            <img src="https://avatars.githubusercontent.com/u/58507761?v=4" width="30;" alt="lignay"/>
            <br />
            <sub><b>Matthew</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/bbbearxyz">
            <img src="https://avatars.githubusercontent.com/u/71327518?v=4" width="30;" alt="bbbearxyz"/>
            <br />
            <sub><b>Bbbearxyz</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/tianyahui-python">
            <img src="https://avatars.githubusercontent.com/u/39303074?v=4" width="30;" alt="tianyahui-python"/>
            <br />
            <sub><b>Tianyahui-python</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/wanglei4687">
            <img src="https://avatars.githubusercontent.com/u/74483764?v=4" width="30;" alt="wanglei4687"/>
            <br />
            <sub><b>Wanglei</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/triump2020">
            <img src="https://avatars.githubusercontent.com/u/63033222?v=4" width="30;" alt="triump2020"/>
            <br />
            <sub><b>Triump2020</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/heni02">
            <img src="https://avatars.githubusercontent.com/u/113406637?v=4" width="30;" alt="heni02"/>
            <br />
            <sub><b>Heni02</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/wanhanbo">
            <img src="https://avatars.githubusercontent.com/u/97089788?v=4" width="30;" alt="wanhanbo"/>
            <br />
            <sub><b>Wanhanbo</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/iceTTTT">
            <img src="https://avatars.githubusercontent.com/u/74845916?v=4" width="30;" alt="iceTTTT"/>
            <br />
            <sub><b>IceTTTT</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/volgariver6">
            <img src="https://avatars.githubusercontent.com/u/18366608?v=4" width="30;" alt="volgariver6"/>
            <br />
            <sub><b>LiuBo</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/taofengliu">
            <img src="https://avatars.githubusercontent.com/u/81315978?v=4" width="30;" alt="taofengliu"/>
            <br />
            <sub><b>刘陶峰</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/Ariznawlll">
            <img src="https://avatars.githubusercontent.com/u/108530700?v=4" width="30;" alt="Ariznawlll"/>
            <br />
            <sub><b>Ariznawlll</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/goodMan-code">
            <img src="https://avatars.githubusercontent.com/u/74952516?v=4" width="30;" alt="goodMan-code"/>
            <br />
            <sub><b>GoodMan-code</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/yingfeng">
            <img src="https://avatars.githubusercontent.com/u/7248?v=4" width="30;" alt="yingfeng"/>
            <br />
            <sub><b>Yingfeng</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/mklzl">
            <img src="https://avatars.githubusercontent.com/u/36362816?v=4" width="30;" alt="mklzl"/>
            <br />
            <sub><b>Mklzl</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/jensenojs">
            <img src="https://avatars.githubusercontent.com/u/56761542?v=4" width="30;" alt="jensenojs"/>
            <br />
            <sub><b>Jensen</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/domingozhang">
            <img src="https://avatars.githubusercontent.com/u/88298673?v=4" width="30;" alt="domingozhang"/>
            <br />
            <sub><b>DomingoZhang</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/arjunsk">
            <img src="https://avatars.githubusercontent.com/u/9638314?v=4" width="30;" alt="arjunsk"/>
            <br />
            <sub><b>Arjun Sunil Kumar</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/chrisxu333">
            <img src="https://avatars.githubusercontent.com/u/44099579?v=4" width="30;" alt="chrisxu333"/>
            <br />
            <sub><b>Nuo Xu</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/aressu1985">
            <img src="https://avatars.githubusercontent.com/u/47846308?v=4" width="30;" alt="aressu1985"/>
            <br />
            <sub><b>Aressu1985</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/matrix-meow">
            <img src="https://avatars.githubusercontent.com/u/108789643?v=4" width="30;" alt="matrix-meow"/>
            <br />
            <sub><b>Mo-bot</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/zengyan1">
            <img src="https://avatars.githubusercontent.com/u/93656539?v=4" width="30;" alt="zengyan1"/>
            <br />
            <sub><b>Zengyan1</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/aylei">
            <img src="https://avatars.githubusercontent.com/u/18556593?v=4" width="30;" alt="aylei"/>
            <br />
            <sub><b>Aylei</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/noneback">
            <img src="https://avatars.githubusercontent.com/u/46670806?v=4" width="30;" alt="noneback"/>
            <br />
            <sub><b>NoneBack</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/WenhaoKong2001">
            <img src="https://avatars.githubusercontent.com/u/43122508?v=4" width="30;" alt="WenhaoKong2001"/>
            <br />
            <sub><b>Otter</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/richelleguice">
            <img src="https://avatars.githubusercontent.com/u/84093582?v=4" width="30;" alt="richelleguice"/>
            <br />
            <sub><b>Richelle Guice</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/yjw1268">
            <img src="https://avatars.githubusercontent.com/u/29796528?v=4" width="30;" alt="yjw1268"/>
            <br />
            <sub><b>Ryan</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/e1ijah1">
            <img src="https://avatars.githubusercontent.com/u/30852919?v=4" width="30;" alt="e1ijah1"/>
            <br />
            <sub><b>Elijah</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/MatrixAdventurer">
            <img src="https://avatars.githubusercontent.com/u/85048713?v=4" width="30;" alt="MatrixAdventurer"/>
            <br />
            <sub><b>MatrixAdventurer</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/NTH19">
            <img src="https://avatars.githubusercontent.com/u/74389817?v=4" width="30;" alt="NTH19"/>
            <br />
            <sub><b>NTH19</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/anitajjx">
            <img src="https://avatars.githubusercontent.com/u/61374486?v=4" width="30;" alt="anitajjx"/>
            <br />
            <sub><b>Anitajjx</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/whileskies">
            <img src="https://avatars.githubusercontent.com/u/20637002?v=4" width="30;" alt="whileskies"/>
            <br />
            <sub><b>Whileskies</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/BePPPower">
            <img src="https://avatars.githubusercontent.com/u/43782773?v=4" width="30;" alt="BePPPower"/>
            <br />
            <sub><b>BePPPower</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/jiajunhuang">
            <img src="https://avatars.githubusercontent.com/u/5924269?v=4" width="30;" alt="jiajunhuang"/>
            <br />
            <sub><b>Jiajun Huang</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/Morranto">
            <img src="https://avatars.githubusercontent.com/u/56924967?v=4" width="30;" alt="Morranto"/>
            <br />
            <sub><b>Morranto</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/Y7n05h">
            <img src="https://avatars.githubusercontent.com/u/69407218?v=4" width="30;" alt="Y7n05h"/>
            <br />
            <sub><b>Y7n05h</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/songjiayang">
            <img src="https://avatars.githubusercontent.com/u/1459834?v=4" width="30;" alt="songjiayang"/>
            <br />
            <sub><b> Songjiayang</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/Abirdcfly">
            <img src="https://avatars.githubusercontent.com/u/5100555?v=4" width="30;" alt="Abirdcfly"/>
            <br />
            <sub><b>Abirdcfly</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/decster">
            <img src="https://avatars.githubusercontent.com/u/193300?v=4" width="30;" alt="decster"/>
            <br />
            <sub><b>Binglin Chang</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/Charlie17Li">
            <img src="https://avatars.githubusercontent.com/u/32014420?v=4" width="30;" alt="Charlie17Li"/>
            <br />
            <sub><b>Charlie17Li</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/DanielZhangQD">
            <img src="https://avatars.githubusercontent.com/u/36026334?v=4" width="30;" alt="DanielZhangQD"/>
            <br />
            <sub><b>DanielZhangQD</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/Juneezee">
            <img src="https://avatars.githubusercontent.com/u/20135478?v=4" width="30;" alt="Juneezee"/>
            <br />
            <sub><b>Eng Zer Jun</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/ericsyh">
            <img src="https://avatars.githubusercontent.com/u/10498732?v=4" width="30;" alt="ericsyh"/>
            <br />
            <sub><b>Eric Shen</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/Fungx">
            <img src="https://avatars.githubusercontent.com/u/38498093?v=4" width="30;" alt="Fungx"/>
            <br />
            <sub><b>Fungx</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/player-kirito">
            <img src="https://avatars.githubusercontent.com/u/73377767?v=4" width="30;" alt="player-kirito"/>
            <br />
            <sub><b>Kirito</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/JasonPeng1310">
            <img src="https://avatars.githubusercontent.com/u/46837930?v=4" width="30;" alt="JasonPeng1310"/>
            <br />
            <sub><b>Jason Peng</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/ikenchina">
            <img src="https://avatars.githubusercontent.com/u/3422667?v=4" width="30;" alt="ikenchina"/>
            <br />
            <sub><b>O2</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/RinChanNOWWW">
            <img src="https://avatars.githubusercontent.com/u/33975039?v=4" width="30;" alt="RinChanNOWWW"/>
            <br />
            <sub><b>RinChanNOW!</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/TheR1sing3un">
            <img src="https://avatars.githubusercontent.com/u/87409330?v=4" width="30;" alt="TheR1sing3un"/>
            <br />
            <sub><b>TheR1sing3un</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/chaixuqing">
            <img src="https://avatars.githubusercontent.com/u/41991639?v=4" width="30;" alt="chaixuqing"/>
            <br />
            <sub><b>XuQing Chai</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/qqIsAProgrammer">
            <img src="https://avatars.githubusercontent.com/u/68439848?v=4" width="30;" alt="qqIsAProgrammer"/>
            <br />
            <sub><b>Yiliang Qiu</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/yubindy">
            <img src="https://avatars.githubusercontent.com/u/74901886?v=4" width="30;" alt="yubindy"/>
            <br />
            <sub><b>ZeYu Zhao</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/adlternative">
            <img src="https://avatars.githubusercontent.com/u/58138461?v=4" width="30;" alt="adlternative"/>
            <br />
            <sub><b>ZheNing Hu</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/TszKitLo40">
            <img src="https://avatars.githubusercontent.com/u/18443139?v=4" width="30;" alt="TszKitLo40"/>
            <br />
            <sub><b>Zijie Lu</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/ZoranPandovski">
            <img src="https://avatars.githubusercontent.com/u/7192539?v=4" width="30;" alt="ZoranPandovski"/>
            <br />
            <sub><b>Zoran Pandovski</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/yegetables">
            <img src="https://avatars.githubusercontent.com/u/37119488?v=4" width="30;" alt="yegetables"/>
            <br />
            <sub><b>Ajian</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/bxiiiiii">
            <img src="https://avatars.githubusercontent.com/u/75570810?v=4" width="30;" alt="bxiiiiii"/>
            <br />
            <sub><b>Binxxi</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/coderzc">
            <img src="https://avatars.githubusercontent.com/u/26179648?v=4" width="30;" alt="coderzc"/>
            <br />
            <sub><b>Coderzc</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/forsaken628">
            <img src="https://avatars.githubusercontent.com/u/18322364?v=4" width="30;" alt="forsaken628"/>
            <br />
            <sub><b>ColdWater</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/dr-lab">
            <img src="https://avatars.githubusercontent.com/u/815516?v=4" width="30;" alt="dr-lab"/>
            <br />
            <sub><b>Dr-lab</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/florashi181">
            <img src="https://avatars.githubusercontent.com/u/87641339?v=4" width="30;" alt="florashi181"/>
            <br />
            <sub><b>Florashi181</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/hiyoyolumi">
            <img src="https://avatars.githubusercontent.com/u/75571545?v=4" width="30;" alt="hiyoyolumi"/>
            <br />
            <sub><b>Hiyoyolumi</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/jinfuchiang">
            <img src="https://avatars.githubusercontent.com/u/92502624?v=4" width="30;" alt="jinfuchiang"/>
            <br />
            <sub><b>Jinfu</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/sourcelliu">
            <img src="https://avatars.githubusercontent.com/u/20898138?v=4" width="30;" alt="sourcelliu"/>
            <br />
            <sub><b>Liuguangliang</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/lokax">
            <img src="https://avatars.githubusercontent.com/u/57343445?v=4" width="30;" alt="lokax"/>
            <br />
            <sub><b>Lokax</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/lyfer233">
            <img src="https://avatars.githubusercontent.com/u/24930135?v=4" width="30;" alt="lyfer233"/>
            <br />
            <sub><b>Lyfer233</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/sundy-li">
            <img src="https://avatars.githubusercontent.com/u/3325189?v=4" width="30;" alt="sundy-li"/>
            <br />
            <sub><b>Sundyli</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/supermario1990">
            <img src="https://avatars.githubusercontent.com/u/8428531?v=4" width="30;" alt="supermario1990"/>
            <br />
            <sub><b>Supermario1990</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/lawrshen">
            <img src="https://avatars.githubusercontent.com/u/63652929?v=4" width="30;" alt="lawrshen"/>
            <br />
            <sub><b>Tjie</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/Toms1999">
            <img src="https://avatars.githubusercontent.com/u/94617906?v=4" width="30;" alt="Toms1999"/>
            <br />
            <sub><b>Toms</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/wuliuqii">
            <img src="https://avatars.githubusercontent.com/u/34090258?v=4" width="30;" alt="wuliuqii"/>
            <br />
            <sub><b>Wuliuqii</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/xiw5">
            <img src="https://avatars.githubusercontent.com/u/33027107?v=4" width="30;" alt="xiw5"/>
            <br />
            <sub><b>Xiyuedong</b></sub>
        </a>
    </td></tr>
<tr>
    <td align="center">
        <a href="https://github.com/yclchuxue">
            <img src="https://avatars.githubusercontent.com/u/75575291?v=4" width="30;" alt="yclchuxue"/>
            <br />
            <sub><b>Yclchuxue</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/ZtXavier">
            <img src="https://avatars.githubusercontent.com/u/75614614?v=4" width="30;" alt="ZtXavier"/>
            <br />
            <sub><b>Zt</b></sub>
        </a>
    </td></tr>
</table>
<!-- readme: contributors -end -->

## 🙌 <a id="contributing">参与贡献</a>

欢迎大家对 MatrixOne 的贡献。  
请查看[贡献指南](https://docs.matrixorigin.cn/latest/MatrixOne/Contribution-Guide/make-your-first-contribution/)来了解有关提交补丁和完成整个贡献流程的详细信息。

## <a id="reference">参考引用</a>

如果你在研究论文中使用 MatrixOne，请引用：

```bibtex
@misc{gou2026versioncontrolsystemdata,
  title={Version Control System for Data with MatrixOne},
  author={Gou, Hongshen and Tian, Feng and Wang, Long and Deng, Nan and Xu, Peng},
  year={2026},
  eprint={2604.03927},
  archivePrefix={arXiv},
  primaryClass={cs.DB},
  doi={10.48550/arXiv.2604.03927},
  url={https://arxiv.org/abs/2604.03927}
}
```

## <a id="license">License</a>

[Apache License, Version 2.0](LICENSE)。
