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

* 启动 MatrixOne 或其他数据库用例。参见更多信息 >>[安装单机版 MatrixOne](../Get-Started/install-standalone-matrixone.md).

* 克隆 MO-Tester 仓库.

  ```
  git clone https://github.com/matrixorigin/mo-tester.git
  ```

## 2. 配置 MO-Tester

MO-tester 基于 Java 语言进行开发，因此 Mo-tester 所依赖的 Java 数据库连接（JDBC，Java Database Connectivity） 驱动程序需要配置 *mo.yml* 文件里的参数信息：进入到 *mo-tester* 本地仓库，打开 *mo.yml* 文件，配置服务器地址、默认的数据库名称、用户名和密码等。

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

运行以下所示命令行，SQL 所有测试用例将自动运行，并将报告和错误消息生成至 *report/report.txt* 和 *report/error.txt* 文件中。

```
> ./run.sh
```

如果你想调整测试范围，你可以修改 `run.yml` 文件中的 `path` 参数。或者，在执行 `run.sh` 命令时，你也可以指定一些参数，参数解释如下：

|参数|参数释义|
|---|---|
|-p|设置由 MO-tester 执行的测试用例的路径。默认值可以参见 *run.yml* 文件中 `path` 的配置参数|
|-m|设置 MO-tester 测试的方法，即直接运行或者生成新的测试结果。默认值可以参见 *run.yaml* 文件中 `method` 的配置参数|
|-t| 设置 MO-tester 执行 SQL 命令的格式类型。默认值可以参见 *run.yml* 文件中 `type` 的配置参数。|
|-r| 设置测试用例应该达到的成功率。默认值可以参见 *run.yml* 文件中 `rate` 的配置参数。|
|-i|设置包含列表，只有路径中名称包含其中一个列表的脚本文件将被执行，如果有多个，如果没有指定，用'，'分隔，指的是包含的所有情况set the including list, and only script files in the path whose name contains one of the lists will be executed, if more than one, separated by `,`, if not specified, refers to all cases included|
|-e|设置排除列表，如果路径下的脚本文件的名称包含一个排除列表，则不会被执行，如果有多个，用'，'分隔，如果没有指定，表示不排除任何情况set the excluding list, and script files in the path whose name contains one of the lists will not be executed, if more than one, separated by `,`, if not specified, refers to none of the cases excluded|
|-g|表示带有[-- @bvt:issue#{issueNO.}]标志的 SQL 命令将不会被执行，该标志以 [-- @bvt:issue#{issueNO.}]开始，以 [-- @bvt:issue]结束。例如，<br>-- @bvt:issue#3236<br/><br>select date_add("1997-12-31 23:59:59",INTERVAL "-10000:1" HOUR_MINUTE);<br/><br>select date_add("1997-12-31 23:59:59",INTERVAL "-100 1" YEAR_MONTH);<br/><br>-- @bvt:issue<br/><br>这两个 SQL 命令与问题 #3236 相关联，它们将不会在 MO-tester 测试中执行，直到问题 #3236 修复后标签移除才可以在测试中执行。<br/>|
|-n|表示在比较结果时将忽略结果集的元数据|

**示例**：

```
./run.sh -p case -m run -t script -r 100 -i select,subquery -e substring -g
```

如果你想测试新的 SQL 用例并自动生成 SQL 结果，运行命令中可以将 `-m run` 更改为 `-m genrs`，或者将 *run.yml* 文件里的 `method` 参数修改为 `genrs`，相关示例参见<p><a href="#new_test_scenario">示例 4</a></p>

!!! note
    每次运行 `./run.sh` 都会覆盖 *report/* 路径下 *error.txt*、*report.txt* 和 *success.txt* 报告文件。

## 4. 查看测试报告

测试完成后，MO-Tester 生成 *error.txt*、*report.txt* 和 *success.txt*  报告文件。

* *report.txt* 示例如下：

```
[SUMMARY] TOTAL : 486, SUCCESS : 486, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
[SUMMARY] TOTAL : 486, SUCCESS : 485, ERROR :1, NOEXE :0, SUCCESS RATE : 99%
[cases/transaction/atomicity.sql] TOTAL : 67, SUCCESS : 66, ERROR :1, NOEXE :0, SUCCESS RATE : 98%
[cases/transaction/isolation.sql] TOTAL : 202, SUCCESS : 202, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
[cases/transaction/isolation_1.sql] TOTAL : 217, SUCCESS : 217, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
```

* *error.txt* 示例如下：

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

## 5. 测试示例

### 示例 1

**示例描述**：运行 *mo-tester* 仓库内的 */cases* 路径下的所有测试用例。

**步骤**：

1. 拉取最新的 *mo-tester* 远端仓库。

   ```
   cd mo-tester
   git pull https://github.com/matrixorigin/mo-tester.git
   ```

2. 运行如下命令，即运行 *mo-tester* 仓库内所有的测试用例：

   ```
   ./run.sh
   ```

3. 在 *report/* 路径下的 *error.txt*、*report.txt* 和 *success.txt* 报告文件中查看运行结果。

### 示例 2

**示例描述**：运行 *mo-tester* 仓库内 */cases/transaction/* 路径下的测试用例。

**步骤**：

1. 拉取最新的 *mo-tester* 远端仓库。

   ```
   cd mo-tester
   git pull https://github.com/matrixorigin/mo-tester.git
   ```

2. 运行如下命令，即运行 *mo-tester* 仓库内 *cases/transaction/* 路径的所有测试用例：

   ```
   ./run.sh -p cases/transaction/
   ```

3. 在 *report/* 路径下的 *error.txt*、*report.txt* 和 *success.txt* 报告文件中查看运行结果。例如，预期 *report.txt* 报告如下所示：

   ```
   [SUMMARY] TOTAL : 486, SUCCESS : 486, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
   [cases/transaction/atomicity.sql] TOTAL : 67, SUCCESS : 67, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
   [cases/transaction/isolation.sql] TOTAL : 202, SUCCESS : 202, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
   [cases/transaction/isolation_1.sql] TOTAL : 217, SUCCESS : 217, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
   ```

### 示例 3

**示例描述**：运行 *mo-tester* 仓库内 *cases/transaction/atomicity.sql* 单个测试用例。

**步骤**：

1. 拉取最新的 *mo-tester* 远端仓库。

   ```
   cd mo-tester
   git pull https://github.com/matrixorigin/mo-tester.git
   ```

2. 运行如下命令，即运行 *mo-tester* 仓库内 *cases/transaction/atomicity.sql* 测试用例：

   ```
   ./run.sh -p cases/transaction/atomicity.sql
   ```

3. 在 *report/* 路径下的 *error.txt*、*report.txt* 和 *success.txt* 报告文件中查看运行结果。例如，预期 *report.txt* 报告如下所示：

   ```
   [SUMMARY] TOTAL : 67, SUCCESS : 67, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
   [cases/transaction/atomicity.sql] TOTAL : 67, SUCCESS : 67, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
   ```

### <h3><a name="new_test_scenario">示例 4</a></h3>

**示例描述**：

- 新建一个命名为 *local_test* 的文件夹，放在 */cases* 目录下
- 本地新增一个命名为测试文件 *new_test.sql*，放在 *cases/local_test/* 路径下
- 仅想要运行 *new_test.sql** 测试用例

**步骤**

1. 拉取最新的 *mo-tester* 远端仓库。

   ```
   cd mo-tester
   git pull https://github.com/matrixorigin/mo-tester.git
   ```

2. 生成测试结果：

   - 方式 1：运行如下命令，生成测试结果。

   ```
   ./run.sh -p cases/local_test/new_test.sql -m genrs -g
   ```

   - 方式 2：打开 *run.yml* 文件，先将 *method* 参数由默认的 `run` 修改为 `genrs`，再运行如下命令，生成测试结果。

   ```
   ./run.sh -p cases/local_test/new_test.sql
   ```

3. 在 *result/* 路径下查看 *local_test/new_test.result* 结果。

4. 运行如下命令，即运行 *mo-tester* 仓库内 *cases/local_test/new_test.sql* 测试用例：

   ```
   ./run.sh -p cases/local_test/new_test.sql -m run -g
   ```

4. 在 *report/* 路径下的 *error.txt*、*report.txt* 和 *success.txt* 报告文件中查看运行结果。例如，*report.txt* 中结果如下所示：

   ```
   [SUMMARY] TOTAL : 67, SUCCESS : 67, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
   [cases/local_test/new_test.sql] TOTAL : 67, SUCCESS : 67, ERROR :0, NOEXE :0, SUCCESS RATE : 100%
   ```
