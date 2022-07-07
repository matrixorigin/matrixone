# **Bit Functions and Operators Overview**

| Name | Description|
|---|-----|
| [&](bitwise-and.md) | Bitwise AND |
| [<<](left-shift.md) | Left shift |
| [^](bitwise-xor.md) | Bitwise XOR |
| [|](bitwise-or.md) | Bitwise OR |

## **Bitwise AND, OR, and XOR Operations**

For `&`, `|`, and `^` bit operations, the result type depends on whether the arguments are evaluated as binary strings or numbers:

- Binary-string evaluation occurs when the arguments have a binary string type, and at least one of them is not a hexadecimal literal, bit literal, or NULL literal. Numeric evaluation occurs otherwise, with argument conversion to unsigned 64-bit integers as necessary.

- Binary-string evaluation produces a binary string of the same length as the arguments. If the arguments have unequal lengths, an `ER_INVALID_BITWISE_OPERANDS_SIZE` error occurs. Numeric evaluation produces an unsigned 64-bit integer.

<!--这三段需要检视-->

## **Bitwise Complement and Shift Operations**

For <!--`~`, , and `>>`-->`<<` bit operation, the result type depends on whether the bit argument is evaluated as a binary string or number:

- Binary-string evaluation occurs when the bit argument has a binary string type, and is not a hexadecimal literal, bit literal, or NULL literal. Numeric evaluation occurs otherwise, with argument conversion to an unsigned 64-bit integer as necessary.

- Binary-string evaluation produces a binary string of the same length as the bit argument. Numeric evaluation produces an unsigned 64-bit integer.

For shift operations, bits shifted off the end of the value are lost without warning, regardless of the argument type. In particular, if the shift count is greater or equal to the number of bits in the bit argument, all bits in the result are 0.
<!--这三段也需要检视-->
