从 0.5.0 版本开始，MatrixOne 引入了一个自动测试框架 [MO-Tester](https://github.com/matrixorigin/mo-tester)。

MO-Tester 测试框架，也可以称作为测试器，是通过 SQL 测试 MatrixOne 或其他数据库功能的。

# MO-Tester 简介

MO-Tester 是基于 Java 语言进行开发，用于 MatrixOne 的测试套件。MO-Tester 构建了一整套完整的工具链来进行 SQL 自动测试。它包含测试用例和运行结果。MO-Tester 启动后，MO-Tester 将使用 MatrixOne 运行所有 SQL 测试用例，并将所有输出 SQL 测试结果与预期结果进行比较。所有案例的结果无论成功或者失败，都将记录在报告中。

MO-Tester 相关用例、结果和报告的链接如下：

* *Cases*: <https://github.com/matrixorigin/mo-tester/tree/main/cases>

* *Result*: <https://github.com/matrixorigin/mo-tester/tree/main/result>

* *Report*: 运行结束后，本地目录自动生成 `mo-tester/report`。

测试用例和测试结果一一对应。如需添加新的测试用例和测试结果请进入右侧所示 MatrixOne 仓库路径中进行添加：<https://github.com/matrixorigin/matrixone/tree/main/test>

MO-Tester 测试用例如下表所示：

| 测试用例     | 描述                                                  |
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

# 使用 MO-Tester

## 1. 准备测试环境

* 请先确认已安装 jdk8。

* 启动 MatrixOne 或其他数据库用例。参见更多信息 >>[安装单机版 MatrixOne](https://docs.matrixorigin.io/0.5.0/MatrixOne/Get-Started/install-standalone-matrixone/).

* 克隆 MO-Tester 仓库.

  ```
  git clone https://github.com/matrixorigin/mo-tester.git
  ```

## 2. 配置 MO-Tester

MO-tester 基于 Java 语言进行开发，因此 JDBC 驱动程序需要配置参数信息：打开 `mo.yml` 文件，配置服务器地址、默认的数据库名称、用户名和密码等。

以下是本地独立版本 MatrixOne 的默认示例。

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

## 3. 运行 MO-Tester

运行以下所示命令行，SQL 所有测试用例将自动运行，并将报告和错误消息生成至 `report/report.txt` 和 `report/error.txt` 文件中。

```
> ./run.sh
```

如果你想调整测试范围，你可以修改 `run.yml` 文件中的 `path` 参数。或者，在执行 `run.sh` 命令时，你也可以指定一些参数，参数如下：

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

如果你想测试新的 SQL 用例并自动生成 SQL 结果，你只需要将 `run` 中的 `method` 参数 `yml` 修改为 `genrs`。运行 `run.sh` 后，在`result/` 路径下将直接记录测试结果及其原始文件名。

注意：每次运行 `run.sh` 都会覆盖 `error.txt`、`report.txt` 和 `success.txt` 报告文件。

## 4. 查看测试报告

测试完成后，MO-Tester 生成 `error.txt`，`report.txt` 和  `success.txt` 报告文件。

* `report.txt` 示例如下：

```
[SUMMARY] TOTAL : 486, SUCCESS : 486, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
[SUMMARY] TOTAL : 486, SUCCESS : 485, ERROR :1, NOEXE :0, SUCCESS RATE : 99%
[cases/transaction/atomicity.sql] TOTAL : 67, SUCCESS : 66, ERROR :1, NOEXE :0, SUCCESS RATE : 98%
[cases/transaction/isolation.sql] TOTAL : 202, SUCCESS : 202, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
[cases/transaction/isolation_1.sql] TOTAL : 217, SUCCESS : 217, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
```

* `error.txt` 示例如下：

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
