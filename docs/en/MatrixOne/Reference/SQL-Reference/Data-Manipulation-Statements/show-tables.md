# **SHOW TABLES**

## **Description**
Shows the list of tables in the currently selected database.

## **Syntax**
```
$ SHOW TABLES  [LIKE 'pattern' | WHERE expr | FROM 'pattern' | IN 'pattern']
```
## **Examples**
```
$ SHOW TABLES;
+---------------+
| name          |
+---------------+
| clusters      |
| contributors  |
| databases     |
| functions     |
| numbers       |
| numbers_local |
| numbers_mt    |
| one           |
| processes     |
| settings      |
| tables        |
| tracing       |
+---------------+
```
