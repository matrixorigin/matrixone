# **数据类型转换**

MatrixOne 支持不同数据类型之间的转换，下表列出了数据类型转换支持情况：

* **可转换**：使用 `cast` 函数，进行显式转换。
* **强制转换**：不使用 `cast` 函数，进行隐式转换，即强制转换。

| 源数据类型             | 目标数据类型 | **显式转换** | **强制转换** |
| ---------------------------- | ---------------- | ------------ | ------------- |
| BOOLEAN                      | INTEGER          | ❌           | ❌            |
|                              | DECIMAL          | ❌           | ❌            |
|                              | VARCHAR          | ✔            | ✔             |
| DATE                         | TIMESTAMP        | ✔            | ✔             |
|                              | DATETIME         | ✔            | ✔             |
|                              | VARCHAR          | ✔            | ✔             |
| DATETIME                     | TIMESTAMP        | ✔            | ✔             |
|                              | DATE             | ✔            | ✔             |
|                              | VARCHAR          | ✔            | ✔             |
| FLOAT(Floating-point number) | INTEGER          | ❌            | ❌             |
|                              | DECIMAL          | ✔            | ✔             |
|                              | VARCHAR          | ✔            | ✔             |
| INTEGER                      | BOOLEAN          | ❌            | ❌             |
|                              | FLOAT            | ✔️            | ✔             |
|                              | TIMESTAMP        | ✔            | ✔             |
|                              | VARCHAR          | ✔            | ✔             |
|                              | DECIMAL          | ✔            | ✔             |
| TIMESTAMP                    | DATE             | ✔            | ✔             |
|                              | DATETIME         | ✔            | ✔             |
|                              | VARCHAR          | ✔            | ✔             |
| VARCHAR                      | BOOLEAN          | ✔            | ✔             |
|                              | DATE             | ✔            | ✔             |
|                              | FLOAT            | ✔            | ✔             |
|                              | INTEGER          | ✔            | ✔             |
|                              | DECIMAL          | ✔            | ✔             |
|                              | TIMESTAMP        | ✔            | ✔             |
|                              | DATETIME         | ✔            | ✔             |
