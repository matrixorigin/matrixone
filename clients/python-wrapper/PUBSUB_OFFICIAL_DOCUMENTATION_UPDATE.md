# MatrixOne Python SDK - 发布订阅功能官方文档更新

## 🎯 基于官方文档的优化

根据 [MatrixOne 官方文档](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/SQL-Reference/Other/SHOW-Statements/show-publications/) 和其他相关文档，我已经对发布订阅功能进行了重要的优化和更新。

## ✅ 主要更新内容

### 1. 字段结构优化

根据 [SHOW PUBLICATIONS 官方文档](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/SQL-Reference/Other/SHOW-Statements/show-publications/)，`SHOW PUBLICATIONS` 返回的字段结构为：

```
| publication | database | tables | sub_account | subscribed_accounts | create_time | update_time | comments |
```

**更新内容**：
- 更新了 `Publication` 类的字段结构以匹配官方文档
- 修改了 `_row_to_publication` 方法以正确解析字段
- 更新了所有相关的测试和示例

### 2. SQL 语法兼容性

**问题发现**：
- `SHOW PUBLICATIONS WHERE ...` 语法在某些 MatrixOne 版本中不被支持
- 需要采用更兼容的方式

**解决方案**：
- 使用 `SHOW PUBLICATIONS` 获取所有数据
- 在 Python 代码中进行过滤
- 提供更好的错误处理和兼容性

### 3. 新增功能

**SHOW CREATE PUBLICATION 支持**：
```python
# 显示创建发布语句
create_statement = client.pubsub.show_create_publication("pub_name")
print(create_statement)
```

### 4. 错误处理改进

**发现的限制**：
1. **"can't publish to self"** - 不能发布给自己（同一个账户）
2. **"unclassified statement appears in uncommitted transaction"** - 发布订阅语句不能在事务中执行
3. **账户不存在** - 需要确保目标账户存在

## 🔧 技术实现细节

### 1. 字段映射更新

```python
class Publication:
    def __init__(self, 
                 name: str,                    # publication
                 database: str,                # database  
                 tables: str,                  # tables
                 sub_account: str,             # sub_account
                 subscribed_accounts: str,     # subscribed_accounts
                 created_time: Optional[datetime] = None,  # create_time
                 update_time: Optional[datetime] = None,   # update_time
                 comments: Optional[str] = None):          # comments
```

### 2. 兼容性查询方法

```python
def get_publication(self, name: str) -> Publication:
    """获取发布 - 兼容性实现"""
    try:
        # 使用 SHOW PUBLICATIONS 获取所有数据
        sql = "SHOW PUBLICATIONS"
        result = self._client.execute(sql)
        
        if not result or not result.rows:
            raise PubSubError(f"Publication '{name}' not found")
        
        # 在 Python 中查找匹配的发布
        for row in result.rows:
            if row[0] == name:  # publication name is in first column
                return self._row_to_publication(row)
        
        raise PubSubError(f"Publication '{name}' not found")
        
    except Exception as e:
        raise PubSubError(f"Failed to get publication '{name}': {e}")
```

### 3. 过滤实现

```python
def list_publications(self, 
                     account: Optional[str] = None,
                     database: Optional[str] = None) -> List[Publication]:
    """列出发布 - 兼容性实现"""
    try:
        # 获取所有发布
        sql = "SHOW PUBLICATIONS"
        result = self._client.execute(sql)
        
        if not result or not result.rows:
            return []
        
        publications = [self._row_to_publication(row) for row in result.rows]
        
        # 在 Python 中应用过滤
        if account:
            publications = [pub for pub in publications if account in pub.sub_account]
        if database:
            publications = [pub for pub in publications if pub.database == database]
        
        return publications
        
    except Exception as e:
        raise PubSubError(f"Failed to list publications: {e}")
```

## 📋 使用限制和最佳实践

### 1. 账户限制

```python
# ❌ 错误：不能发布给自己
client.pubsub.create_database_publication("pub1", "db1", "sys")  # 当前用户是 sys

# ✅ 正确：发布给其他账户
client.pubsub.create_database_publication("pub1", "db1", "acc1")  # 发布给 acc1
```

### 2. 事务限制

```python
# ❌ 错误：发布订阅语句不能在事务中执行
with client.transaction() as tx:
    tx.pubsub.create_database_publication("pub1", "db1", "acc1")  # 会失败

# ✅ 正确：在事务外执行发布订阅操作
pub = client.pubsub.create_database_publication("pub1", "db1", "acc1")
with client.transaction() as tx:
    tx.execute("INSERT INTO users (name) VALUES ('Alice')")  # 其他操作
```

### 3. 权限要求

根据 [MatrixOne 权限文档](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/access-control-type/#publish-and-subscribe-permission)：

- 发布者必须是 ACCOUNTADMIN 或 MOADMIN 角色
- 订阅者需要访问订阅数据的权限
- 发布者控制订阅者访问权限

## 🎯 实际使用示例

### 1. 基本发布订阅流程

```python
from matrixone import Client

client = Client()
client.connect(host="localhost", port=6001, user="root", password="111", database="test")

# 1. 创建发布（发布给其他账户）
pub = client.pubsub.create_database_publication(
    name="warehouse_pub",
    database="central_warehouse_db", 
    account="acc1"  # 发布给 acc1 账户
)

# 2. 列出发布
pubs = client.pubsub.list_publications()
for pub in pubs:
    print(f"Publication: {pub.name}, Database: {pub.database}, Sub Account: {pub.sub_account}")

# 3. 创建订阅（从发布者订阅）
sub = client.pubsub.create_subscription(
    subscription_name="warehouse_sub",
    publication_name="warehouse_pub",
    publisher_account="sys"  # 从 sys 账户订阅
)

# 4. 查看创建语句
create_statement = client.pubsub.show_create_publication("warehouse_pub")
print(f"CREATE statement: {create_statement}")

client.disconnect()
```

### 2. 错误处理示例

```python
try:
    # 尝试创建发布
    pub = client.pubsub.create_database_publication("pub1", "db1", "acc1")
    print(f"✅ Created publication: {pub}")
    
except PubSubError as e:
    if "can't publish to self" in str(e):
        print("❌ 不能发布给自己，请使用其他账户")
    elif "not existed account" in str(e):
        print("❌ 目标账户不存在，请先创建账户")
    else:
        print(f"❌ 发布创建失败: {e}")
```

## 🧪 测试更新

所有测试都已更新以反映新的字段结构：

```python
def test_create_database_publication_success(self):
    """测试数据库发布创建成功"""
    # Mock 成功执行
    mock_result = Mock()
    mock_result.rows = [('db_pub1', 'central_db', '*', 'acc1', '', datetime.now(), None, None)]
    self.client.execute = Mock(return_value=mock_result)
    
    # 测试创建数据库发布
    pub = self.pubsub_manager.create_database_publication("db_pub1", "central_db", "acc1")
    
    # 验证
    self.assertIsInstance(pub, Publication)
    self.assertEqual(pub.name, "db_pub1")
    self.assertEqual(pub.database, "central_db")
    self.assertEqual(pub.tables, "*")
    self.assertEqual(pub.sub_account, "acc1")
```

## 📊 测试结果

```
Ran 29 tests in 0.097s
OK
```

所有测试都通过，确保功能的正确性和稳定性。

## 🎉 总结

通过基于 MatrixOne 官方文档的优化，我们的发布订阅功能现在：

1. **完全兼容官方文档** - 字段结构和 SQL 语法完全匹配
2. **更好的错误处理** - 提供清晰的错误信息和限制说明
3. **增强的兼容性** - 支持不同版本的 MatrixOne
4. **新增功能** - 支持 SHOW CREATE PUBLICATION
5. **完善的测试** - 29 个测试用例全部通过
6. **详细的文档** - 包含使用限制和最佳实践

这个实现为 MatrixOne 用户提供了强大、稳定且完全符合官方规范的发布订阅功能！🎉

## 📚 参考文档

- [SHOW PUBLICATIONS](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/SQL-Reference/Other/SHOW-Statements/show-publications/)
- [CREATE PUBLICATION](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/SQL-Reference/Data-Definition-Language/create-publication/)
- [ALTER PUBLICATION](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/SQL-Reference/Data-Definition-Language/alter-publication/)
- [DROP PUBLICATION](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/SQL-Reference/Data-Definition-Language/drop-publication/)
- [SHOW CREATE PUBLICATION](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/SQL-Reference/Other/SHOW-Statements/show-create-publication/)
- [SHOW SUBSCRIPTIONS](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/SQL-Reference/Other/SHOW-Statements/show-subscriptions/)
- [Publish-Subscribe Permissions](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/access-control-type/#publish-and-subscribe-permission)
- [Publish-Subscribe Overview](https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Develop/Publish-Subscribe/pub-sub-overview/)
