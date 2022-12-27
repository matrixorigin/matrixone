# **JSON Types Overview**

MatrixOne JSON types conforms with MySQL JSON types definition.

Reference: <https://dev.mysql.com/doc/refman/8.0/en/json.html>

## **JSON Binary Format**

Reference: <https://dev.mysql.com/doc/dev/mysql-server/8.0.28/json__binary_8h.html>

### **Description**

JSON binary format is a binary format for storing JSON data.

### **Detail**

 ```
 JSON doc ::= type value
    type ::=
        0x01 |       // object
        0x03 |       // array
        0x04 |       // literal (true/false/null)
        0x09 |       // int64
        0x0a |       // uint64
        0x0b |       // float64
        0x0c |       // string

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

### **Implementations**

1. All the values are wrapped in a JSON doc. A JSON doc consists of a type and a value. The type field is a single byte
   that indicates the type of the value. The value field is a binary representation of the value.
2. The value of object type is stored as a sequence of element-count, size, key-entry list, value-entry list, key list
   and value list, where element-count is the number of members in the object, size is the number of bytes in the
   binary. The key-entry consists of key-offset and key-length, key-offset is the offset to the key in the key list, and
   key-length is the length of the key. The value-entry consists of type and offset-or-inlined-value, type is the type
   of the value, offset-or-inlined-value is the offset to the value in the value list or the value itself if it is small
   enough to be inlined. The key is a string and the value is a binary representation of the value type described above.
3. The value of array type is stored as a sequence of element-count, size, value-entry list, value list, where
   element-count
   is the number of elements in the array, size is the number of bytes in the binary. The value-entry consists of type
   and
   offset-or-inlined-value, type is the type of the value, offset-or-inlined-value is the offset to the value in the
   value list
   or the value itself if it is small enough to be inlined. The value is a binary representation of the value type
   described
   above.
4. The value of literal type is stored as a single byte that indicates the literal type.
5. The value of number type is stored as a binary representation of the number whose format is little-endian.
6. The value of string type is stored as a sequence of data-length and utf8-data, data-length which is stored as a
   variable size encoding is the length of the utf8-data, utf8-data is the utf8-encoded string.

### **Example**

```sql
drop table if exists t;
create table t
(
    a json,
    b int
);

insert into t(a, b)
values ('{"a": [1, "2", {"aa": "bb"}]}', 1),
       ('[1, 2, 3]', 2),
       ('null', 3),
       ('true', 4),
       ('false', 5),
       ('1', 6),
       ('1.1', 7),
       ('"a"', 8);

select *
from t;
+-------------------------------------+---+
| a                                   | b |
+-------------------------------------+---+
| {"a": [1, "2", {"aa": "bb"}]}       | 1 |
| [1, 2, 3]                           | 2 |
| null                                | 3 |
| true                                | 4 |
| false                               | 5 |
| 1                                   | 6 |
| 1.1                                 | 7 |
| "a"                                 | 8 |
+-------------------------------------+---+

delete
from t
where b = 3;

update t
set a = '{"a": 1}'
where b = 1;

select *
from t;
+-------------------------------------+---+
| a                                   | b |
+-------------------------------------+---+
| {"a": 1}                            | 1 |
| [1, 2, 3]                           | 2 |
| true                                | 4 |
| false                               | 5 |
| 1                                   | 6 |
| 1.1                                 | 7 |
| "a"                                 | 8 |
```

## **JSON Path Syntax**

Reference: <https://dev.mysql.com/doc/refman/8.0/en/json.html#json-path-syntax>

### **Description**

JSON Path is a path expression that can be used to access a value in a JSON document.

### **Detail**

 ```
 pathExpression:
     scope[(pathLeg)*]

 pathLeg:
     member | arrayLocation | arrayRange | doubleAsterisk

 member:
     period ( keyName | asterisk )

 arrayLocation:
     leftBracket ( nonNegativeInteger | asterisk ) rightBracket
 
 arrayRange:
     leftBracket (nonNegativeInteger| lastExpression) colon (nonNegativeInteger| lastExpression) rightBracket
     
 lastExpression:
     lastToken substract nonNegativeInteger

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
     
 colon:
     'to'
     
 lastToken:
     'last'
     
 substract:
     '-'
 ```

*In MatrixOne, the scope of the path is always the document being operated on, represented as $. You can use '$' as a
synonym for the document in JSON path expressions.*

### **Notes**

0. The path expression must start with `$`.
1. The array location is a left bracket followed by a non-negative integer or an asterisk, followed by a right bracket,
   if user gives a negative number, it will return an error.
2. The key name is an identifier or a double-quoted string.
3. `prefix**suffix` represents all paths beginning with prefix and ending with suffix. prefix is optional, while suffix
   is required; in other words, a path may not end in `**`.

### **Examples**

```
$ -> represents the whole document
$[0] -> represents second element of the array document
$.* -> represents all values of the object document
$.store -> represents the store object
$.store.* -> represents all values of the store object
$.store.book[0] -> represents the first book element in the store object
$**.a -> represents values of the document keys which ends with 'a', such as 'a', 'b.a', 'c.b.a', etc.
$.a**.b -> represents values of the document keys which starts with 'a' and ends with 'b', such as 'a.b', 'a.x.b', 'a.x.y.b', etc.
$[last] -> represents the last element of the array document
$[last-1] -> represents the second last element of the array document
$[0 to 2] -> represents the first three elements of the array document
$[0 to last-1] -> represents all elements of the array document except the last one
$[0 to last-2] -> represents all elements of the array document except the last two
$[last - 5 to last] -> represents the last five elements of the array document
```

## **JSON EXTREACT**

### **Description**

json_extract is a JSON query function that can be used to query JSON documents.

### **Syntax**

```sql
select json_extract(jsonDoc, pathExpression);
```

*jsonDoc is the JSON document to be queried,which can be a JSON text string or a JSON column in a table.*

### **Implementation**

1. The implementation is based on the JSONPath syntax.
2. If the path expression is not valid, return an error.
3. If the path expression is valid, return the value of the path expression. Rules for query:
    1. If the path expression is empty, return the query result.
    2. Use *restPath* to represent the *current* path expression without the front path leg.
    3. If the front leg is a member, query the value of the member if current doc is an object, or return null if
       current doc is an array.
    4. If the front leg is an array location, query the value of the array location if current doc is an array, or
       return null if current doc is an object.
    5. If the front leg is a double asterisk:
        1. First, use the *restPath* expression to query the value of the current doc.
        2. Second, use the *current* path expression to query the value of each sub doc in the current doc.
        3. Return the union of the two results.
    6. If the front leg is an asterisk, use the *rest* path expression to query the value of each sub doc in the current
       doc, and return the union of the results.
    7. If the result is not a single value, return the result as a JSON array.
4. see `matrixone.pkg.container.bytejson.Query` for more details.

### **Examples**

```sql
select json_extract('[1,2,3]', '$[*]');
+-----------------------+
| json_extract('[1,2,3]', '$[*]') |
+-----------------------+
| [1,2,3]               |
+-----------------------+

select json_extract('[1,2,3]', '$[0]');
+------------------+
| json_extract([1,2,3],$[0]) |
+------------------+
| 1                |
+------------------+

select json_extract('{"a":1,"b":2,"c":3}', '$.*');
+-----------------------------+
| json_extract({"a":1,"b":2,"c":3},$.*) |
+-----------------------------+
| [1, 2, 3]                   |
+-----------------------------+

select json_extract('{"a":1,"b":2,"c":3}', '$.a');
+-----------------------------+
| json_extract({"a":1,"b":2,"c":3},$.a) |
+-----------------------------+
| 1                           |
+-----------------------------+

select json_extract('{"a":1,"b":2,"c":3,"d":{"a":"x"}}', '$**.a');
+---------------------------------------------+
| json_extract({"a":1,"b":2,"c":3,"d":{"a":"x"}},$**.a) |
+---------------------------------------------+
| [1, "x"]                                    |
+---------------------------------------------+

drop table if exists t;
create table t
(
    a json
);

insert into t
values ('{"a":1,"b":2,"c":3}');

select json_extract(a, '$.a')
from t;
+----------------------+
| json_extract(a,$.a)            |
+----------------------+
| 1                    |
+----------------------+

insert into t
values ('{"a":5,"b":6,"c":7}');
select json_extract(a, '$.a')
from t;
+----------------------+
| json_extract(a,$.a)            |
+----------------------+
| 1                    |
| 5                    |
+----------------------+
```


