# **CAST**

## **函数说明**
CAST()函数可以将任何类型的一个值转化为另一个特定类型 。

## **语法语法**
```
> CAST(value AS datatype)

```

## **相关参数**
|  参数  | 说明 |
|  ----  | ----  |
| value  | 必要参数，待转化的值 |
| datatype  | 必要参数，目标数据类型 |


`datatype ` 可以是如下任何一种数据类型：
|  取值  | 说明  |
|  ----  | ----  |
| DATE  | 转化为日期类型，格式为: "YYYY-MM-DD" |
| DATETIME  | 转化为日期时间类型. 格式为: "YYYY-MM-DD HH:MM:SS" |
| DECIMAL  | 转化为定点数，使用可选参数`M`与`D`分别指定数值的最大位数与小数位数|
| TIME  | 转化为时间类型. 格式为: "HH:MM:SS" |
| CHAR  | 转化为定长字符串CHAR|
| NCHAR  | 转化为 NCHAR (与CHAR类似，但使用国家字符集来生成字符串（双字节编码 |
| SIGNED  | 转化为带符号的64位整数 |
| UNSIGNED  | 转化为无符号的64位整数 |
| BINARY  | 转化二进制字符串  |


## *示例**
```

#Convert a value to a DATE datatype:

> SELECT CAST("2017-08-29" AS DATE);

#Convert a value to a CHAR datatype:

> SELECT CAST(150 AS CHAR);

#Convert a value to a SIGNED datatype:

>SELECT CAST(5-10 AS SIGNED);

```

## **限制**

