**欢迎大家来到MatrixOne，这里就是你进入矩阵世界的电话亭了～叮铃~~~**

> 英文版本活动说明[传送门](https://github.com/matrixorigin/matrixone/issues/1997)

## MatrixCamp2022活动

MatrixCamp是一个由MatrixOne社区举办的开发者活动，欢迎对数据库技术感兴趣的开发者们来参与这次为期2周的开发挑战任务。
MatrixOne社区一共准备了4个类别的56个任务，有不同的难度级别和功能类型，开发者可以挑选自己感兴趣的进行挑战。参与挑战仅需要一些基础的Golang语言编程经验就够了，另外我们也有详尽的开发指南的耐心的mentor给大家进行服务。

这次的任务大家将要挑战的是MatrixOne的系统函数和聚合函数，对于刚入门数据库的同学来讲是相对基础但是又不乏挑战的任务。

- 基础任务-系统函数（Built-in function）: 所谓的系统函数就是数据库自带的针对一些基础数据类型进行操作的函数，比如常见的round(), time(), substring()等等。第一周将有25个系统函数作为基础任务发布给大家进行挑战，包含数学类函数，时间日期类函数，字符串类函数，有9个任务非常容易，16个任务稍微有一些难度，只要你有一定的go语言基础，看得懂英文文档，就能快速上手解决哦。

- 挑战任务-聚合函数（Aggregate function）：所谓的聚合函数就是需要聚集一部分数据进行运算返回结果的函数，比如常见的sum(),count(),avg()等等。在MatrixOne中，实现聚合函数是要用到我们的大杀器因子化加速能力的，需要对因子化中的“环”数据结构理论有一定理解，实现会有一定复杂度，所以我们将5个聚合函数列为了挑战任务。

## 参与流程

在开始之前，先给MatrixOne项目Star, Fork, Watch一下吧。

1. 选择很重要！加小助手微信“MatrixOrigin001”填写活动注册表<https://www.wjx.top/vm/Ys1rz1I.aspx> 选择你想要完成的函数任务，并加入MatrixOne社区群。
2. 登录Github，在你选择的函数issue留下你的comment，比如“I'd like to work on this issue”, 小助手会将相关issue分配给你。
3. 开始只属于你的函数体验任务。如有任何问题，MatrixOne社区群里的技术大牛全程在线支持。

> 请注意：1个开发者可以选择多个挑战任务，但是1个任务只能由1个开发者完成

来看下我们的任务列表吧，其中已经被assign的任务请查看[英文版活动说明](https://github.com/matrixorigin/matrixone/issues/1997)：

### 1. 基础任务-数学类系统函数

- [ ] <https://github.com/matrixorigin/matrixone/issues/1966> Mathematical Built-in function sin() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1967> Mathematical Built-in function cos() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1968> Mathematical Built-in function tan() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1969> Mathematical Built-in function cot() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1970> Mathematical Built-in function asin() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1971> Mathematical Built-in function acos() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1973> Mathematical Built-in function atan() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2094> Mathematical Built-in function sinh() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2095> Mathematical Built-in function cosh() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2096> Mathematical Built-in function crc32() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2097> Mathematical Built-in function sqrt() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2098> Mathematical Built-in function cbrt() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2099> Mathematical Built-in function rand() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2100> Mathematical Built-in function degrees() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2101> Mathematical Built-in function radians() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2102> Mathematical Built-in function sign() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2103> Mathematical Built-in function hypot() **[中等]**

### 2. 基础任务-日期时间类系统函数

- [ ]  <https://github.com/matrixorigin/matrixone/issues/1974> Datetime Built-in function date() **[容易]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/1976> Datetime Built-in function utc_timestamp() **[容易]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/1977> Datetime Built-in function datediff() **[中等]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/1978> Datetime Built-in function dayofmonth() **[中等]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/1979> Datetime Built-in function dayofweek() **[中等]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/1980> Datetime Built-in function dayofyear() **[中等]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/2049> Datetime Built-in function month() **[中等]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/2046> Datetime Built-in function weekday() **[中等]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/2047> Datetime Built-in function yearweek() **[中等]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/2048> Datetime Built-in function week() **[中等]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/1830> Datetime Built-in function now() **[中等]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/2104> Datetime Built-in function quarter() **[中等]**
- [ ]  <https://github.com/matrixorigin/matrixone/issues/2105> Datetime Built-in function timestamp() **[中等]**

### 3. 基础任务-字符串类系统函数

- [ ] <https://github.com/matrixorigin/matrixone/issues/1984> String function lpad() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1985> String function ltrim() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1986> String function rpad() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1987> String function rtrim() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1988> String function repeat() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1989> String function reverse() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1990> String function space() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1991> String function replace() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2106> String function ascii() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2107> String function bit_length() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2108> String function empty() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2109> String function notEmpty() **[容易]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2110> String function bin() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2111> String function concat() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2112> String function hex() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2113> String function insert() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2114> String function locate() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2115> String function oct() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2116> String function lengthUTF8() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2117> String function startsWith() **[中等]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/2118> String function endsWith() **[中等]**

### 4. 挑战任务-聚合函数

- [ ] <https://github.com/matrixorigin/matrixone/issues/1992> Aggregate function any() **[有挑战]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1993> Aggregate function bit_and() **[有挑战]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1994> Aggregate function bit_or() **[有挑战]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1995> Aggregate function bit_xor() **[有挑战]**
- [ ] <https://github.com/matrixorigin/matrixone/issues/1996> Aggregate function stddev_pop() **[有挑战]**

## 在开始之前

1. 仔细阅读下MatrixOne社区的[贡献者指南](https://docs.matrixorigin.io/0.3.0/MatrixOne/Contribution-Guide/make-your-first-contribution/) 了解如何向MatrixOne提交代码.
2. 了解整个MatrixOne数据库项目, 参考[MatrixOne项目文档](https://docs.matrixorigin.io/).
3. 详细查看 [**系统函数构建指南**](https://docs.matrixorigin.io/0.3.0/MatrixOne/Contribution-Guide/Tutorial/develop_builtin_functions/) 与 [**聚合函数构建指南**](https://docs.matrixorigin.io/0.3.0/MatrixOne/Contribution-Guide/Tutorial/develop_aggregate_functions/)，其中详细描述了如何在MatrixOne中开发系统函数和聚合函数，同时给出了很多样例代码。

## 提交代码(Pull Request)要求

1. 注意在实现功能完成之后一定要写单元测试哦，否则你的PR是不会被社区采纳的。
2. 完成代码编写之后，按以下格式向MatrixOne提交PR: 

* PR格式: [MatrixCamp] + function name + PR title
* 标签 ：[MatrixCamp]
* PR内容: 遵循[MatrixOne的PR模版] (<https://github.com/matrixorigin/matrixone/blob/main/.github/PULL_REQUEST_TEMPLATE.md>)  

3. 提交PR完成后，在你的PR下面按以下格式评论：
评论格式: "I have finished Issue #" + PR link id

## 常见问题

**Q: 必须先领任务才能开始么，可以直接提PR吗？**
A:  是的，参与开发者都需要在issue下面评论认领后再开始任务，不建议直接提PR。这样是为了避免多名开发者针对同一问题重复劳动。

**Q: 为什么`select abs(-1);`和`select d, abs(-1) from t;`这样的语句会出问题？**
A: 目前MatrixOne还不支持无表及常数参数的SQL语句，因此`select abs(-1);`和`select abs(-1) from table1;`这样的语句均会出错。正确的SQL语句是需要创建一张表，导入一些数据，然后再将列名作为参数运行built-in函数。如以下例子：

```sql
select abs(a) from t; 
```

**Q: 为什么每次PR完成之后，我的仓库总是会比MatrixOne领先？**
A: MatrixOne对所有pr的合并都要求是squash and merge，因此pr合并后跟主干会不一致。这里推荐采用的git流程，可以保持本地git，远端git仓库与MatrixOne仓库保持一致的方法：

```
1. 首先Fork MatrixOne的仓库。
2. clone自己的仓库到本地: git clone https://github.com/YOUR_NAME/matrixone.git
3. 将MatrixOne主仓库添加为remote仓库: git remote add upstream https://github.com/matrixorigin/matrixone.git
4. 在本地修改代码，合并冲突，etc.
5. push到一个新的分支: git push origin main:NEW_BRANCH
6. 到自己仓库的NEW_BRANCH分支提PR，等待合并
7. PR合并之后，覆盖本地提交历史: git pull --force upstream main:main
8. 再次提交到自己的远端仓库：git push --force origin main:main
```

**Q: 我提交的PR在自动测试阶段失败了，这个是什么原因？**
A：MatrixOne会在PR提交之后自动进行一系列CI及BVT测试，开发者在提交之后可以看到测试的进展及日志，如果是与函数功能相关的测试没有通过，请修改code后重新提交。如果是其他与函数无关的测试问题，可以在PR中进行评论，社区将会查询问题所在，不会影响本次函数任务的判定结果。目前MatrixOne的CI测试有一定的不稳定程度，因此可能有一些与函数无关的CI测试会fail。

**Q: MatrixOne服务启动之后的日志信息有点太多了，怎么能关掉?**
A: 目前MatrixOne的默认日志级别是`DEBUG`级，所以信息是会比较多的。控制这个日志级别的是系统配置文件`system_vars_config.toml`，可以在调试的时候将该文件的`cubeLogLevel`与`level`两个参数设置成`ERROR`级别，这样打印的信息会少很多。

```
cubeLogLevel = "error"
level = "error"
```

**Q: 如果我对认领的函数实现有一些疑问，我应该向谁询问?**
A：如果是针对MatrixOne本身的测试和行为发现bug，欢迎向MatrixOne社区提出issue，我们有相应的模版帮助说明如何写issue，社区会对issue进行处理。如果是针对函数本身的实现有问题，可以直接在认领的issue下面评论你的想法和疑问，MatrixOne社区的mentor会及时回复。

**Q: 我启动了MatrixOne服务，并且用MySQL进行连接，但是好像没有获得正确的结果，如何才知道是否成功连接?**
A: 如果你的MySQL客户端连接MatrixOne后打印如下信息，就说明已经成功连接： 

```
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1002
Server version: 0.3.0 MatrixOne

Copyright (c) 2000, 2020, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement. 
```

Q: 我的MatrixOne服务启动了，但是我的MySQL客户端始终连接不上，这个应该怎么解决？
A: 这个可能由各种原因导致，首先请关注错误的日志信息来定位问题，目前在MatrixOne中有两种常见错误：

1. 端口冲突：有的时候因为端口被其他程序占用会有冲突，比如50000端口经常被占用。可以通过一下`lsof -i:50000`命令来查找是哪个进程在占用这个端口。从这个命令中可以获得这个进程的PID，再通过`kill -9 PIDNAME`关闭这个进程。
2. 存储不兼容：有的时候如果你从新的代码库拉取了最新的代码，但是存储数据是之前版本导入的数据，你可能也会面临连接失败问题。你可以尝试先将MatrixOne目录下的`Store`目录清空并重启MatrixOne服务。

**Q: Mentor要求我对提交的代码格式进行缩进，这个如何做？**
A：可以通过Golang语言自带的gofmt工具在命令行中进行操作，或者在一些常见的IDE如VS Code和GoLand中也均有相应的设置。

**Q: 如何理解MatrixOne的Vector数据结构？**
A：Vector是MatrixOne中表示一列数据的结构，是MatrixOne最重要的数据结构体之一。它其中的不同变量含义：

```
// Vector.Or: 表示这个Vector是否是从磁盘读取的原始Vector
// Link: 这个Vector拥有的软链接个数
// Data: 内存中保存的原始数据
// Typ: Vector的类型
// Col: 内存中经过序列化编码的数据，它与Vector.Data共享内存位置，但是会被转换到某个专门的类型，比如int32
// Nsp: 存储一列中所有NULL的位图
```
