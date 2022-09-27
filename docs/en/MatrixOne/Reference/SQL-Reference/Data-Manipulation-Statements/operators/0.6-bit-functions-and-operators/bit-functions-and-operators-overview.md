# **Bit Functions and Operators**

| Name | Description|
|---|-----|
| [&](bitwise-and.md) | Bitwise AND |
| [>>](right-shift.md) | Right shift |
| [<<](left-shift.md) |Left shift|
| [^](bitwise-xor.md) |Bitwise XOR|
| [|](bitwise-or.md) |Bitwise OR|
| [~](bitwise-inversion.md) |Bitwise inversion|

Bit functions and operators required BIGINT (64-bit integer) arguments and returned BIGINT values, so they had a maximum range of 64 bits. Non-BIGINT arguments were converted to BIGINT prior to performing the operation and truncation could occur.

Bit functions and operators permit binary string type arguments (BINARY, VARBINARY, and the BLOB types) and return a value of like type, which enables them to take arguments and produce return values larger than 64 bits. Nonbinary string arguments are converted to BIGINT and processed as such, as before.
