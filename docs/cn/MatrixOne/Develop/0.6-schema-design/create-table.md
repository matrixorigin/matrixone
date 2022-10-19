# 创建表

本篇文档中介绍如何使用 SQL 来创建表。上一篇文档中介绍了创建一个名为 *modatabase* 的数据库，本篇文档我们介绍在这个数据库中创建一个表。

!!! note
    此处仅对 `CREATE TABLE` 语句进行简单描述。更多信息，参见 [CREATE TABLE](../../Reference/SQL-Reference/Data-Definition-Statements/create-table.md)。

## 开始前准备

在阅读本页面之前，你需要准备以下事项：

- 了解并已经完成构建 MatrixOne 集群。
- 了解什么是[数据库模式](overview.md)。
- 创建了一个数据库。

## 什么是表

表是 MatrixOne 数据库集群中的一种逻辑对象，它从属于某个数据库，用于保存数据。

表以行和列的形式组织数据记录，一张表至少有一列。若在表中定义了 n 个列，那么每一行数据都将拥有与这 n 个列中数据格式完全一致的字段。

## 命名表

创建一个有实际意义的表名称，含有关键词或编号规范的表名称，遵循命名规范，方便查找和使用。

`CREATE TABLE` 语句通常采用以下形式：

```sql
CREATE TABLE {table_name} ({elements});
```

**参数描述**

- {table_name}：表名。
- {elements}：以逗号分隔的表元素列表，比如列定义，主键定义等。

## 定义列

列从属于表，每张表都至少有一列。列通过将每行中的值分成一个个单一数据类型的小单元来为表提供结构。

列定义通常使用以下形式：

```
{column_name} {data_type} {column_qualification}
```

**参数描述**

- {column_name}：列名。
- {data_type}：列的数据类型。
- {column_qualification}：列的限定条件。

这里介绍创建一个命名为 *user* 的表来存储 *modatabase* 库中的用户信息。

可以为 *users* 表添加一些列，如他们的唯一标识 *id*，余额 *balance* 及昵称 nickname。

```sql
CREATE TABLE `modatabase`.`users` (
  `id` bigint,
  `nickname` varchar(100),
  `balance` decimal(15,2)
);
```

**示例解释**

下表将解释上述示例中的字段：

|字段名|数据类型|作用|解释|
|---|---|---|---|
|id|bigint|用以表示用户唯一标识|所有的用户标识都应该是 bigint 类型的|
|nickname|varchar|用以表示用户的昵称|所用用户的昵称都是 varchar 类型，且不超过 100 字符的|
|balance|decimal|用以表示用户的余额|精度为 15，比例为 2，即精度代表字段数值的总位数，而比例代表小数点后有多少位，例如: decimal(5,2)，即精度为 5，比例为 2 时，其取值范围为 -999.99 到 999.99。decimal(6,1) ，即精度为 6，比例为 1 时，其取值范围为 -99999.9 到 99999.9。|

MatrixOne 支持许多其他的列数据类型，包含 整数、浮点数、时间等，参见[数据类型](../../Reference/Data-Types/data-types.md)。

**创建一个复杂表**

创建一张 *books* 表，这张表将是 *modatabase* 数据的核心。它包含书的 唯一标识、名称、库存、价格、出版时间 字段。

```sql
CREATE TABLE `modatabase`.`books` (
  `id` bigint NOT NULL,
  `title` varchar(100),
  `published_at` datetime,
  `stock` int,
  `price` decimal(15,2)
);
```

这张表比 *users* 表包含更多的数据类型：

|字段名|数据类型|作用|解释|
|---|---|---|---|
|stock|int|推荐使用合适大小的类型|防止使用过量的硬盘甚至影响性能(类型范围过大)或数据溢出(类型范围过小)|
|published_at|datetime|时间值|可以使用 datetime 类型保存时间值|

## 选择主键

主键是一个或一组列，这个由所有主键列组合起来的值是数据行的唯一标识。

主键在 `CREATE TABLE` 语句中定义。主键约束要求所有受约束的列仅包含非 `NULL` 值。

一个表可以没有主键，主键也可以是非整数类型。

## 添加列约束

除主键约束外，MatrixOne 还支持其他的列约束，如：非空约束 NOT NULL、默认值 DEFAULT 等。

### **填充默认值**

如需在列上设置默认值，请使用 `DEFAULT` 约束。默认值将可以使你无需指定每一列的值，就可以插入数据。

你可以将 `DEFAULT` 与支持的 SQL 函数结合使用，将默认值的计算移出应用层，从而节省应用层的资源（当然，计算所消耗的资源并不会凭空消失，只是被转移到了 MatrixOne 集群中）。常见的，希望实现数据插入时，可默认填充默认的时间。还是使用 *ratings* 作为示例，可使用以下语句：

```sql
CREATE TABLE `modatabase`.`ratings` (
  `book_id` bigint,
  `user_id` bigint,
  `score` tinyint,
  `rated_at` datetime DEFAULT NOW(),
  PRIMARY KEY (`book_id`,`user_id`)
);
```

### **防止重复**

如果你需要防止列中出现重复值，那你可以使用 `UNIQUE` 约束。

例如，你需要确保用户的昵称唯一，可以这样改写 *users* 表的创建 SQL：

```sql
CREATE TABLE `modatabase`.`users` (
  `id` bigint,
  `balance` decimal(15,2),
  `nickname` varchar(100) UNIQUE,
  PRIMARY KEY (`id`)
);
```

如果你在 *users* 表中尝试插入相同的 *nickname*，将返回错误。

### **防止空值**

如果你需要防止列中出现空值，那就可以使用 `NOT NULL` 约束。

还是使用用户昵称来举例子，除了昵称唯一，还希望昵称不可为空，于是此处可以这样改写 *users* 表的创建 SQL：

```sql
CREATE TABLE `modatabase`.`users` (
  `id` bigint,
  `balance` decimal(15,2),
  `nickname` varchar(100) UNIQUE NOT NULL,
  PRIMARY KEY (`id`)
);
```

<!--## 使用 HTAP 能力: 缺少数据分析引擎等文档-->

## 执行 `CREATE TABLE` 语句

需查看 *modatabase* 数据库下的所有表，可使用 `SHOW TABLES` 语句：

```sql
SHOW TABLES IN `modatabase`;
```

运行结果为：

```
+----------------------+
| tables_in_modatabase |
+----------------------+
| books                |
| ratings              |
| users                |
+----------------------+
```

## 创建表时应遵守的规则

### 命名表时应遵守的规则

- 使用完全限定的表名称（例如：`CREATE TABLE {database_name}.{table_name}`）。这是因为你在不指定数据库名称时，MatrixOne 将使用你 SQL 会话中的当前数据库。若你未在 SQL 会话中使用 `USE {databasename};` 来指定数据库，MatrixOne 将会返回错误。

- 请使用有意义的表名，例如，若你需要创建一个用户表，你可以使用名称：*user*, *t_user*, *users* 等，或遵循你公司或组织的命名规范。如果你的公司或组织没有相应的命名规范，可参考表命名规范。

- 多个单词以下划线分隔，不推荐超过 32 个字符。

- 不同业务模块的表单独建立 *DATABASE*，并增加相应注释。

### 定义列时应遵守的规则

- 查看支持的列的数据类型。
- 查看选择主键时应遵守的规则，决定是否使用主键列。
- 查看添加列约束，决定是否添加约束到列中。
- 请使用有意义的列名，推荐你遵循公司或组织的表命名规范。如果你的公司或组织没有相应的命名规范，可参考列命名规范。

### 选择主键时应遵守的规则

- 在表内定义一个主键或唯一索引。
- 尽量选择有意义的列作为主键。
- 出于为性能考虑，尽量避免存储超宽表，表字段数不建议超过 60 个，建议单行的总数据大小不要超过 64K，数据长度过大字段最好拆到另外的表。
- 不推荐使用复杂的数据类型。
- 需要 `JOIN` 的字段，数据类型保障绝对一致，避免隐式转换。
- 避免在单个单调数据列上定义主键。如果你使用单个单调数据列（例如：AUTO_INCREMENT 的列）来定义主键，有可能会对写性能产生负面影响。
