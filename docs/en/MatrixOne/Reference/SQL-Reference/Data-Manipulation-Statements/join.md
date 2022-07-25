# **JOIN**

## **Description**

The ``JOIN`` statement is used to combine rows from two or more tables.

The following figure shows seven usages of ``LEFT JOIN``, ``RIGHT JOIN``, ``INNER JOIN``, and ``OUTER JOIN``.

- ``LEFT JOIN``

|SELECT /<select_list> FROM TableA A LEFT JOIN TableB B ON A.Key=B.Key|![leftjoin](https://github.com/matrixorigin/artwork/blob/main/docs/reference/left_join.png?raw=true)|
|---|---|
|SELECT /<select_list> FROM TableA A LEFT JOIN TableB B ON A.Key=B.Key WHERE B.Key IS NULL|![leftjoinwhere](https://github.com/matrixorigin/artwork/blob/main/docs/reference/left_join_where.png?raw=true)|

- ``RIGHT JOIN``

|SELECT /<select_list> FROM TableA A RIGHT JOIN TableB B ON A.Key=B.Key|![leftjoinwhere](https://github.com/matrixorigin/artwork/blob/main/docs/reference/right_join.png?raw=true)|
|---|---|
|SELECT /<select_list> FROM TableA A RIGHT JOIN TableB B ON A.Key=B.Key WHERE A.Key IS NULL|![leftjoinwhere](https://github.com/matrixorigin/artwork/blob/main/docs/reference/right_join_where.png?raw=true)|

- ``INNER JOIN``

|SELECT /<select_list> FROM TableA A INNER JOIN TableB B ON A.Key=B.Key|![innerjoin](https://github.com/matrixorigin/artwork/blob/main/docs/reference/inner_join.png?raw=true)|
|---|---|

- ``FULL JOIN``

|SELECT /<select_list> FROM TableA A FULL OUTER JOIN TableB B ON A.Key=B.Key|![leftjoin](https://github.com/matrixorigin/artwork/blob/main/docs/reference/full_join.png?raw=true)|
|---|---|
|SELECT /<select_list> FROM TableA A FULL OUTER JOIN TableB B ON A.Key=B.Key WHERE A.Key IS NULL OR B.Key IS NULL|![fulljoinwhere](https://github.com/matrixorigin/artwork/blob/main/docs/reference/full_join_where.png?raw=true)|

For more information, see the reference below:

- [LEFT JOIN](join/left-join.md)
- [RIGHT JOIN](join/right-join.md)
- [INNER JOIN](join/inner-join.md)
- [FULL JOIN](join/full-join.md)
- [OUTER JOIN](join/outer-join.md)
- [NATURAL JOIN](join/natural-join.md)
