# **Arithmetic Operators Overview**

| Name | Description|
|---|-----|
| [%,MOD](mod.md) | Modulo operator |
| [*](multiplication.md) | Multiplication operator |
| [+](addition.md) | Addition operator |
| [-](minus.md) | Minus operator |
| [-](unary-minus.md) | Change the sign of the argument |
| [/](division.md) | Division operator |
| [DIV](div.md) | Integer division |

The usual arithmetic operators are available. The result is determined according to the following rules:

- In the case of `-`, `+`, and `*`, the result is calculated with `BIGINT` (64-bit) precision if both operands are integers.

- If both operands are integers and any of them are unsigned, the result is an unsigned integer.

- If any of the operands of a `+`,`-`, `/`, `*`, `%` is a real or string value, the precision of the result is the precision of the operand with the maximum precision.

In division performed with `/`, the scale of the result when using two exact-value operands is the scale of the first operand plus the value of the div_precision_increment system variable. For example, the result of the expression 5.05 / 0.014 has a scale of 13 decimal places (360.7142857142857).

These rules are applied for each operation, such that nested calculations imply the precision of each component. Hence, (14620 / 9432456) / (24250 / 9432456), resolves first to (0.0014) / (0.0026), with the final result having 16 decimal places (0.6028865979381443).

## **Constraints**

- Arithmetic operators only apply to numbers.
- To ensure that components and subcomponents of a calculation use the appropriate level of precision. See [Cast Functions and Operators](../cast-functions-and-operators/cast-functions-and-operators-overview.md).
