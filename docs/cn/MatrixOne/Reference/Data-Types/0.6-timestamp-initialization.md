# TIMESTAMP 和 DATETIME 的自动初始化和更新

`TIMESTAMP` 和 `DATETIME` 列可以自动初始化并更新为当前日期和时间（即当前时间戳）。

对于表中的任何 `TIMESTAMP` 或 `DATETIME` 列，你可以将当前时间戳指定为默认值、自动更新值或两者均可：

- 对于未为列指定值的插入行，将自动初始化的列设置为当前时间戳。

- 当行中任何其他列的值从当前值进行更改时，自动更新列将自动更新为当前时间戳。如果所有其他列都设置为当前值，则自动更新的列保持不变。若要防止自动更新的列在其他列更改时更新，请显式将其设置为当前值。要更新自动更新的列，即使其他列没有更改，也要显式地将其设置为它应该具有的值(例如，将其设置为 `CURRENT_TIMESTAMP`)。

要指定自动属性，请在列定义中使用 `DEFAULT CURRENT_TIMESTAMP` 和 `ON UPDATE CURRENT_TIMESTAMP` 子句。两个子句如果同时定义一个列，它们的顺序可互换，不影响逻辑计算。另外，`CURRENT_TIMESTAMP` 与 `CURRENT_TIMESTAMP()` 或者 `NOW()` 意义一致。

`DEFAULT CURRENT_TIMESTAMP` 和 `ON UPDATE CURRENT_TIMESTAMP` 的使用是特定于 `TIMESTAMP` 和 `DATETIME` 的。 `DEFAULT` 子句还可用于指定常量（非自动）默认值（例如，`DEFAULT 0` 或 `DEFAULT '2000-01-01 00:00:00'`）。

`TIMESTAMP` 或 `DATETIME` 列定义可以为默认值和自动更新值指定当前时间戳，仅指定其中一个，或者两者都不指定。不同的列可以有不同的自动属性组合。以下规则描述了这些可能性：

- 同时使用 `DEFAULT CURRENT_TIMESTAMP` 和 `ON UPDATE CURRENT_TIMESTAMP` 子句时，列的默认值是当前时间戳，并自动更新为当前时间戳。

```
CREATE TABLE t1 (
  ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

- 仅使用 `DEFAULT` 子句，不使用 `ON UPDATE CURRENT_TIMESTAMP` 子句，该列具有给定的默认值，但不会自动更新为当前时间戳。

   默认值取决于 `DEFAULT` 子句是指定 `CURRENT_TIMESTAMP` 还是常量值。使用 `CURRENT_TIMESTAMP`，默认为当前时间戳。

```
CREATE TABLE t1 (
  ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  dt DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

   对于常量，默认值是给定值。在这种情况下，该列没有自动属性。

```
CREATE TABLE t1 (
  ts TIMESTAMP DEFAULT 0,
  dt DATETIME DEFAULT 0
);
```

- 使用 `ON UPDATE CURRENT_TIMESTAMP` 子句和常量 `DEFAULT` 子句，该列会自动更新为当前时间戳并使用给定的常量默认值。

```
CREATE TABLE t1 (
  ts TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
  dt DATETIME DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP
);
```

- 仅使用 `ON UPDATE CURRENT_TIMESTAMP` 子句，但不使用 `DEFAULT` 子句时，列会自动更新为当前时间戳，但其默认值没有当前时间戳。

   `TIMESTAMP` 的默认值为 0；若使用 `NULL` 属性定义，则默认值为 `NULL`。

```
CREATE TABLE t1 (
  ts1 TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,     -- default 0
  ts2 TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP -- default NULL
);
```

`DATETIME` 的默认值为 `NULL`；若使用 `NOT NULL` 属性定义，则默认值为 0。

```
CREATE TABLE t1 (
  dt1 DATETIME ON UPDATE CURRENT_TIMESTAMP,         -- default NULL
  dt2 DATETIME NOT NULL ON UPDATE CURRENT_TIMESTAMP -- default 0
);
```
