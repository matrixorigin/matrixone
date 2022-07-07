# **Operator Precedence**

Operator precedences are shown in the following list, from highest precedence to the lowest. Operators that are shown together on a line have the same precedence.

| From highest precedence to the lowest | Operators|
|---|-----|
| 1 |  INTERVAL   |
| 2 |   BINARY, COLLATE  |
| 3 |   !  |
| 4 |   - (unary minus), ~ (unary bit inversion)  |
| 5 |  ^   |
| 6 |   *, /, DIV, %, MOD|
| 7 |   -, +  |
| 8 |  <<, >>|
| 9 | & |
| 10 | \| |
| 11 |= (comparison), <=>, >=, >, <=, <, <>, !=, IS, LIKE, <!--REGEXP-->, IN, MEMBER OF |
| 12 | BETWEEN, CASE, WHEN, THEN, ELSE |
| 13 | NOT |
| 14 | AND, && |
| 15 | XOR |
| 16 | OR, \|\| |
|17|= (assignment)|

<!--:= 位于assignment后，暂时不支持-->

The precedence of = depends on whether it is used as a comparison operator (=) or as an assignment operator (=). When used as a comparison operator, it has the same precedence as <!--<=>-->, >=, >, <=, <, <>, !=, IS, LIKE, and IN().

For operators that occur at the same precedence level within an expression, evaluation proceeds left to right, with the exception that assignments evaluate right to left.
