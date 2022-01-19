# **YEAR()**

## **Description**

The YEAR() or TOYEAR() function returns the year part for a given date (a number from 1000 to 9999).


## **Syntax**

```
> YEAR(date)
> TOYEAR(date)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| date  | Required.  The date/datetime to extract the year from |



## **Examples**


```
> SELECT YEAR("2017-06-15");
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmddd` for now. 