# BLOB 和 TEXT 数据类型

**BLOB**

- `BLOB` 是可以存储可变数量的大数据二进制对象。BLOB 有四种类型，分别是 `TINYBLOB`、`BLOB`、`MEDIUMBLOB` 和 `LONGBLOB`，它们保存值的长度不同。
- `BLOB` 值为二进制字符串（字节字符串），对应二进制字符集和排序规则，比较和排序基于列值中字节的数值。

**TEXT**

- 四种 `TEXT` 类型分别是 `TINYTEXT`、`TEXT`、`MEDIUMTEXT` 和 `LONGTEXT`。更多信息，参见[数据类型](data-types.md)

- TEXT 为非二进制字符串（字符串）。它们具有二进制以外的字符集，并且基于字符集的排序规则对值进行排序和比较。

<!--If strict SQL mode is not enabled and you assign a value to a BLOB or TEXT column that exceeds the column's maximum length, the value is truncated to fit and a warning is generated. For truncation of nonspace characters, you can cause an error to occur (rather than a warning) and suppress insertion of the value by using strict SQL mode. See Section 5.1.11, “Server SQL Modes”.-->

**关于 BLOB 和 TEXT**

从要插入 `TEXT` 列的值中截断多余的尾随空格会产生警告。

对于 `TEXT` 和 `BLOB` 数据类型的列，插入时没有字节填充，则选择时也就没有字节被剥离。

如果将 `BINARY` 属性与 `TEXT` 数据类型一起使用，则会为该列分配列字符集的二进制 `(_bin)` 排序规则。

`LONG` 和 `LONG VARCHAR` 可以被兼容映射到 `MEDIUMTEXT` 数据类型。

每个 `BLOB` 或 `TEXT` 值在内部由一个单独分配的对象表示。这与所有其他数据类型相反，其他数据类型在打开表时需要为每列分配一次存储。

在某些情况下，可能需要将二进制数据（例如媒体文件）存储在 `BLOB` 或 `TEXT` 列中。
