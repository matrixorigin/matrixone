# **LENGTH()**

## **Description**

The length() function returns the length of the string.  


## **Syntax**

```
> LENGTH(str)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| str | Required. String you want to calculate. |


## **Examples**


```
> select a,length(a) from t1;

a	length(a)
a       1 
ab      2 
abc     3 
```

## Constraints
Currently, MatrixOne doesn't support select function() without from tables.

