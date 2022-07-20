# **Comparison Functions and Operators Overview**

| Name | Description|
|---|-----|
| [>](greater-than.md) | Greater than operator |
| [>=](greater-than-or-equal.md) | Greater than or equal operator |
| [<](less-than.md) | Less than operator |
| [<>,!=](not-equal.md) | Not equal operator |
| [<=](less-than-or-equal.md) | Less than or equal operator |
| [=](assign-equal.md) | Equal operator|
| [BETWEEN ... AND ...](between.md) | Whether a value is within a range of values |
| [IN()](in.md) | Whether a value is within a set of values |
| [IS](is.md) | Test a value against a boolean |
| [IS NOT](is-not.md) | Test a value against a boolean |
| [IS NOT NULL](is-not-null.md) | NOT NULL value test |
| [IS NULL](is-null.md) | NULL value test |
| [LIKE](like.md) | Simple pattern matching |
| [NOT BETWEEN ... AND ...](not-between.md) | Whether a value is not within a range of values |
| [NOT LIKE](not-like.md) | Negation of simple pattern matching |

Comparison operations result in a value of `TRUE`, `FALSE`, or `NULL`. These operations work for both numbers and strings. Strings are automatically converted to numbers and numbers to strings as necessary.

The following relational comparison operators can be used to compare not only scalar operands, but row operands:

```
=  >  <  >=  <=  <>  !=
```
