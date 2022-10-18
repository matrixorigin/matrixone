# **UUID()**

## **Description**

Returns a Universal Unique Identifier (UUID) generated according to [RFC 4122](http://www.ietf.org/rfc/rfc4122.txt), “A Universally Unique IDentifier (UUID) URN Namespace”.

A UUID is designed as a number that is globally unique in space and time. Two calls to UUID() are expected to generate two different values, even if these calls are performed on two separate devices not connected to each other.

!!! info
    Although UUID() values are intended to be unique, they are not necessarily unguessable or unpredictable. If unpredictability is required, UUID values should be generated some other way.

`UUID()` returns a value that conforms to UUID version 1 as described in [RFC 4122](http://www.ietf.org/rfc/rfc4122.txt). The value is a 128-bit number represented as a utf8mb3 string of five hexadecimal numbers in aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee format:

- The first three numbers are generated from the low, middle, and high parts of a timestamp. The high part also includes the UUID version number.

- The fourth number preserves temporal uniqueness in case the timestamp value loses monotonicity (for example, due to daylight saving time).

- The fifth number is an IEEE 802 node number that provides spatial uniqueness. A random number is substituted if the latter is not available (for example, because the host device has no Ethernet card, or it is unknown how to find the hardware address of an interface on the host operating system). In this case, spatial uniqueness cannot be guaranteed. Nevertheless, a collision should have very low probability.

## **Syntax**

```
> UUID()
```

## **Examples**

```sql
> create table t1(a INT,  b float);
> insert into t1 values(12124, -4213.413), (12124, -42413.409);
> SELECT length(uuid()) FROM t1;
+----------------+
| length(uuid()) |
+----------------+
|             36 |
|             36 |
+----------------+
2 rows in set (0.01 sec)

> SELECT UUID();
+--------------------------------------+
| uuid()                               |
+--------------------------------------+
| 4b64e482-47a3-11ed-8df5-5ad2460dea4f |
+--------------------------------------+
1 row in set (0.00 sec)
```

## **Constraints**

`UUID()` does not support optional argument for now, which means it doesn't support `UUID([number])`.
