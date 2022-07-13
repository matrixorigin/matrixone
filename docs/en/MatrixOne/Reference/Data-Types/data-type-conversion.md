## **Data Type Conversion**

MatrixOne supports the conversion between different data types, the supported and unsupported conversions are listed in the following table.

* **Castable**: explicit conversion with  `cast` function.
* **Coercible**: implicit conversion without `cast` function. 

| Source Data Type             | Target Data Type | **Castable** | **Coercible** |
| ---------------------------- | ---------------- | ------------ | ------------- |
| BOOLEAN                      | INTEGER          | ❌            | ❌             |
|                              | DECIMAL          | ❌            | ❌             |
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
|                              | FLOAT            | ✔            | ✔             |
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
