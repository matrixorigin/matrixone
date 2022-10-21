# The BLOB and TEXT Types

**BLOB**

- A `BLOB` is a binary large object that can hold a variable amount of data. The four `BLOB` types are `TINYBLOB`, `BLOB`, `MEDIUMBLOB`, and `LONGBLOB`. These differ only in the maximum length of the values they can hold.

- `BLOB` values are treated as binary strings (byte strings). They have the binary character set and collation, and comparison and sorting are based on the numeric values of the bytes in column values.

**TEXT**

The four TEXT types are TINYTEXT, TEXT, MEDIUMTEXT, and LONGTEXT. For more information, see [Data Types Overview](data-types.md)

- `TEXT` values are treated as nonbinary strings (character strings). They have a character set other than binary, and values are sorted and compared based on the collation of the character set.

<!--If strict SQL mode is not enabled and you assign a value to a BLOB or TEXT column that exceeds the column's maximum length, the value is truncated to fit and a warning is generated. For truncation of nonspace characters, you can cause an error to occur (rather than a warning) and suppress insertion of the value by using strict SQL mode. See Section 5.1.11, “Server SQL Modes”.-->

**About `BLOB` and `TEXT`**

Truncation of excess trailing spaces from values to be inserted into `TEXT` columns always generates a warning.

For `TEXT` and `BLOB` columns, there is no padding on insert and no bytes are stripped on select.

If you use the `BINARY` attribute with a `TEXT` data type, the column is assigned the binary (_bin) collation of the column character set.

`LONG` and `LONG VARCHAR` map to the `MEDIUMTEXT` data type. This is a compatibility feature.

Each `BLOB` or `TEXT` value is represented internally by a separately allocated object. This is in contrast to all other data types, for which storage is allocated once per column when the table is opened.

In some cases, it may be desirable to store binary data such as media files in `BLOB` or `TEXT` columns.
