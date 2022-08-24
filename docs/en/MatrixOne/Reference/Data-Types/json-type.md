# **JSON Types Overview**
MatrixOne JSON types conforms with MySQL JSON types definition.

Reference: <https://dev.mysql.com/doc/refman/8.0/en/json.html>

## **JSON Binary Format**
Reference: <https://dev.mysql.com/doc/dev/mysql-server/8.0.28/json__binary_8h.html>

```
JSON doc ::= type value
   type ::=
       0x01 |       // object
       0x02 |       // array
       0x03 |       // literal (true/false/null)
       0x04 |       // int64
       0x05 |       // uint64
       0x06 |       // float64
       0x07 |       // string

   value ::=
       object  |
       array   |
       literal |
       number  |
       string  |

   object ::= element-count size key-entry* value-entry* key* value*

   array ::= element-count size value-entry* value*

   // number of members in object or number of elements in array
   element-count ::= uint32

   // number of bytes in the binary representation of the object or array
   size ::= uint32

   key-entry ::= key-offset key-length

   key-offset ::= uint32

   key-length ::= uint16    // key length must be less than 64KB

   value-entry ::= type offset-or-inlined-value

   // This field holds either the offset to where the value is stored,
   // or the value itself if it is small enough to be inlined (that is,
   // if it is a JSON literal)
   offset-or-inlined-value ::= uint32

   key ::= utf8-data

   literal ::=
       0x00 |   // JSON null literal
       0x01 |   // JSON true literal
       0x02 |   // JSON false literal

   number ::=  ....    // little-endian format for [u]int64 and float64

   string ::= data-length utf8-data

   data-length ::= uint8*    // Variable size encoding, if the high bit of a byte is 1, the length
                             // field is continued in the next byte,
                             // otherwise it is the last byte of the length
                             // field. So we need 1 byte to represent
                             // lengths up to 127, 2 bytes to represent
                             // lengths up to 16383, and so on...
```

## **JSON Path Syntax**
Reference: <https://dev.mysql.com/doc/refman/8.0/en/json.html#json-path-syntax>

```
pathExpression:
    scope[(pathLeg)*]

pathLeg:
    member | arrayLocation | doubleAsterisk

member:
    period ( keyName | asterisk )

arrayLocation:
    leftBracket ( nonNegativeInteger | asterisk ) rightBracket

keyName:
    ESIdentifier | doubleQuotedString

doubleAsterisk:
    '**'

period:
    '.'

asterisk:
    '*'

leftBracket:
    '['

rightBracket:
    ']'
```
### notes of asterisk
1. `.*` represents the values of all members in the object.
2. `[*]` represents the values of all elements in the array.
3. `prefix**suffix` represents all paths beginning with prefix and ending with suffix. prefix is optional, while suffix is required; in other words, a path may not end in `**`.