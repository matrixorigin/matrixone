From the 0.5.0 version, MatrixOne introduces an automatic testing framework [MO-Tester](https://github.com/matrixorigin/mo-tester).

This tester is designed to test MatrixOne or other database functionalities with SQL.

# What's in MO-Tester?

MO-Tester is a java-based tester suite for MatrixOne. It has built a whole toolchain to run automatic SQL tests. It contains the test cases and results. Once launched, MO-Tester runs all SQL test cases with MatrixOne, and compares all output SQL results with expected results. All successful and failed cases will be logged into reports.

MO-Tester content locations:

* *Cases*: <https://github.com/matrixorigin/mo-tester/tree/main/cases>

* *Result*: <https://github.com/matrixorigin/mo-tester/tree/main/result>

* *Report*: once finished running, a `mo-tester/report` will be generated in the local directory.

The Cases and Results are 1-1 correspondence, and they are actually `git submodules` from MatrixOne repository. Adding new cases and results should be in MatrixOne repo: <https://github.com/matrixorigin/matrixone/tree/main/test>

MO-Tester includes testing cases in the following table.

| Test cases     | Description                                                  |
| -------------- | ------------------------------------------------------------ |
| Benchmark/TPCH | DDL and 22 Queries of TPCH Benchmark                         |
| Database       | DDL Statements, creation/drop databases                      |
| Table          | DDL Statements, creation/drop tables                         |
| DML            | DML Statements, including insert, select, show statements    |
| dtype          | Data Types and type conversion test cases                    |
| Expression     | Case when, With(CTE), Temporal Interval                      |
| Function       | Aggregate function, built-in function                        |
| Explain        | Explain statement                                            |
| Join           | Join statement, including Left/Right/Inner/Outer/Natural Join |
| Operator       | Including +,-,*,/,MOD,%,=, >, <, IS, LIKE etc                |
| Subquery       | Including Select/From/Where subquery                         |
| Transaction    | Test of isolation level, atomicity                           |

# How to use MO-Tester?

## 1. Prepare the testing environment

* Make sure you have installed jdk8.

* Launch MatrixOne or other database instance. Please refer to more information about [how to install and launch MatrixOne](https://docs.matrixorigin.io/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/).

* Clone `mo-tester` repository.

  ```
  git clone https://github.com/matrixorigin/mo-tester.git
  ```

## 2. Configure `mo-tester`

* In `mo.yml` file, configure the server address, default database name, username&password, etc. MO-tester is based on java, so these parameters are required for the JDBC driver. Below is a default example for a local standalone version MatrixOne.

  ```
  #jdbc
  jdbc:
    driver: "com.mysql.cj.jdbc.Driver"
    server:
    - addr: "127.0.0.1:6001"
    database:
      default: "test"
    paremeter:
      characterSetResults: "utf8"
      continueBatchOnError: "false"
      useServerPrepStmts: "true"
      alwaysSendSetIsolation: "false"
      useLocalSessionState: "true"
      zeroDateTimeBehavior: "CONVERT_TO_NULL"
      failoverReadOnly: "false"
      serverTimezone: "Asia/Shanghai"

  #users
  user:
    name: "dump"
    passwrod: "111"
  ```

## 3. Run mo-tester

* With the simple below command, all the SQL test cases will automatically run and generate reports and error messages to `report/report.txt` and `report/error.txt`.

```
> ./run.sh
```

If you'd like to adjust the test range, you can just change the `path` parameter of `run.yml`. And you can also specify some 			parameters when executing the command `run.sh`, parameters are as followings:

```
-p  set the path of test cases needed to be executed by mo-tester, the default value is configured by the `path` in `run.yaml`
-m  set the method that mo-tester will run with, the default value is configured by the `method` in `run.yaml`
-t  set the type of the format that mo-tester executes the SQL command in, the default value is configured by the `type` in `run.yaml`
-r  set The success rate that test cases should reach, the default value is configured by the `rate` in `run.yaml`
-i  set the including list, and only script files in the path whose name contains one of the lists will be executed, if more than one, separated by `,`, if not specified, refers to all cases included
-e  set the excluding list, and script files in the path whose name contains one of the lists will not be executed, if more than one, separated by `,`, if not specified, refers to none of the cases excluded
-g  means SQL commands which is marked with [bvt:issue] flag will not be executed,this flag starts with [-- @bvt:issue#{issueNO.}],and ends with [-- @bvt:issue],eg:
    -- @bvt:issue#3236
    select date_add("1997-12-31 23:59:59",INTERVAL "-10000:1" HOUR_MINUTE);
    select date_add("1997-12-31 23:59:59",INTERVAL "-100 1" YEAR_MONTH);
    -- @bvt:issue
    Those two sql commands are associated with issue#3236, and they will not be executed in bvt test, until the flag is removed when issue#3236 is fixed.

-n  means the metadata of the resultset will be ignored when comparing the result
Examples:
bash run.sh -p case -m run -t script -r 100 -i select,subquery -e substring -g
```

If you want to automatically generate SQL results for the new SQL cases, you can just change the `method` parameter of `run.yml` to `genrs`. Running the `run.sh` scripts will directly record test results in the `result/` path with their original filenames.

Note: every time running `run.sh` will overwrite the `error`, `report`, and `success` reports.

## 4. Check the report

* Once the test is finished, `mo-tester` generates `error`, `report` and `success` reports in `txt` format.

* An example of `report.txt`  looks like this:

```
[SUMMARY] TOTAL : 486, SUCCESS : 486, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
[SUMMARY] TOTAL : 486, SUCCESS : 485, ERROR :1, NOEXE :0, SUCCESS RATE : 99%
[cases/transaction/atomicity.sql] TOTAL : 67, SUCCESS : 66, ERROR :1, NOEXE :0, SUCCESS RATE : 98%
[cases/transaction/isolation.sql] TOTAL : 202, SUCCESS : 202, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
[cases/transaction/isolation_1.sql] TOTAL : 217, SUCCESS : 217, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
```

* An example of `error.txt` looks like this:

```
[ERROR]
[SCRIPT   FILE]: cases/transaction/atomicity.sql
[ROW    NUMBER]: 14
[SQL STATEMENT]: select * from test_11 ;
[EXPECT RESULT]:
c	d
1	1
2 2
[ACTUAL RESULT]:
c	d
1	1
```
