**欢迎大家来到MatrixOne，这里就是你进入矩阵世界的电话亭了～叮铃~~~**

> 英文版本活动说明[传送门](https://github.com/matrixorigin/matrixone/issues/1997)

## MatrixCamp2022活动

MatrixCamp是一个由MatrixOne社区举办的开发者活动，欢迎对数据库技术感兴趣的开发者们来参与这次为期2周的开发挑战任务。
MatrixOne社区一共准备了4个类别的55个任务，有不同的难度级别和功能类型，开发者可以挑选自己感兴趣的进行挑战。参与挑战仅需要一些基础的Golang语言编程经验就够了，另外我们也有详尽的开发指南的耐心的mentor给大家进行服务。

这次的任务大家将要挑战的是MatrixOne的系统函数和聚合函数，对于刚入门数据库的同学来讲是相对基础但是又不乏挑战的任务。


- 基础任务-系统函数（Built-in function）: 所谓的系统函数就是数据库自带的针对一些基础数据类型进行操作的函数，比如常见的round(), time(), substring()等等。第一周将有25个系统函数作为基础任务发布给大家进行挑战，包含数学类函数，时间日期类函数，字符串类函数，有9个任务非常容易，16个任务稍微有一些难度，只要你有一定的go语言基础，看得懂英文文档，就能快速上手解决哦。

- 挑战任务-聚合函数（Aggregate function）：所谓的聚合函数就是需要聚集一部分数据进行运算返回结果的函数，比如常见的sum(),count(),avg()等等。在MatrixOne中，实现聚合函数是要用到我们的大杀器因子化加速能力的，需要对因子化中的“环”数据结构理论有一定理解，实现会有一定复杂度，所以我们将5个聚合函数列为了挑战任务。


## 参与流程
1. 选择很重要！加小助手微信“MatrixOrigin001”填写活动注册表，选择你想要完成的函数任务，并加入MatrixOne社区群。
2. 登陆Github，在你选择的函数issue留下你的comment，小助手会将相关issue分配给你。
3. 开始只属于你的函数体验任务。如有任何问题，MatrixOne社区群里的技术大牛全程在线支持。

> 请注意：1个开发者可以选择多个挑战任务，但是1个任务只能由1个开发者完成


来看下我们的任务列表吧：

### 1. 基础任务-数学类系统函数


- [ ] https://github.com/matrixorigin/matrixone/issues/1966 Mathematical Built-in function sin() **[容易]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1967 Mathematical Built-in function cos() **[容易]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1968 Mathematical Built-in function tan() **[容易]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1969 Mathematical Built-in function cot() **[容易]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1970 Mathematical Built-in function asin() **[容易]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1971 Mathematical Built-in function acos() **[容易]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1973 Mathematical Built-in function atan() **[容易]**

### 2. 基础任务-日期时间类系统函数



- [ ]  https://github.com/matrixorigin/matrixone/issues/1974 Datetime Built-in function date() **[容易]**
- [ ]  https://github.com/matrixorigin/matrixone/issues/1975 Datetime Built-in function utc_time() **[容易]**
- [ ]  https://github.com/matrixorigin/matrixone/issues/1976 Datetime Built-in function utc_timestamp() **[容易]**
- [ ]  https://github.com/matrixorigin/matrixone/issues/1977 Datetime Built-in function datediff() **[中等]**
- [ ]  https://github.com/matrixorigin/matrixone/issues/1978 Datetime Built-in function dayofmonth() **[中等]**
- [ ]  https://github.com/matrixorigin/matrixone/issues/1979 Datetime Built-in function dayofweek() **[中等]**
- [ ]  https://github.com/matrixorigin/matrixone/issues/1980 Datetime Built-in function dayofyear() **[中等]**
- [ ]  https://github.com/matrixorigin/matrixone/issues/1981 Datetime Built-in function hour() **[中等]**
- [ ]  https://github.com/matrixorigin/matrixone/issues/1982 Datetime Built-in function minute() **[中等]**
- [ ]  https://github.com/matrixorigin/matrixone/issues/1983 Datetime Built-in function second() **[中等]**


### 3. 基础任务-字符串类系统函数

- [ ] https://github.com/matrixorigin/matrixone/issues/1984 String function lpad() **[中等]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1985 String function ltrim() **[中等]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1986 String function rpad() **[中等]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1987 String function rtrim() **[中等]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1988 String function repeat() **[中等]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1990 String function space() **[中等]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1991 String function replace() **[中等]**

### 4. 挑战任务-聚合函数

- [ ] https://github.com/matrixorigin/matrixone/issues/1992 Aggregate function any() **[有挑战]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1993 Aggregate function bit_and() **[有挑战]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1994 Aggregate function bit_or() **[有挑战]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1995 Aggregate function bit_xor() **[有挑战]**
- [ ] https://github.com/matrixorigin/matrixone/issues/1996 Aggregate function stddev_pop() **[有挑战]**


## 在开始之前
1. 仔细阅读下MatrixOne社区的[贡献者指南](https://docs.matrixorigin.io/0.3.0/MatrixOne/Contribution-Guide/make-your-first-contribution/) 了解如何向MatrixOne提交代码.
2. 了解整个MatrixOne数据库项目, 参考[MatrixOne项目文档](https://docs.matrixorigin.io/).
3. 详细查看 [**系统函数构建指南**](https://docs.matrixorigin.io/0.3.0/MatrixOne/Contribution-Guide/Tutorial/develop_builtin_functions/) 与 [**聚合函数构建指南**](https://docs.matrixorigin.io/0.3.0/MatrixOne/Contribution-Guide/Tutorial/develop_aggregate_functions/)，其中详细描述了如何在MatrixOne中开发系统函数和聚合函数，同时给出了很多样例代码。

## 提交代码(Pull Request)要求
1. 注意在实现功能完成之后一定要写单元测试哦，否则你的PR是不会被社区采纳的。
2. 完成代码编写之后，按以下格式向MatrixOne提交PR: 
* PR格式: [MatrixCamp] + function name + PR title
* 标签 ：[MatrixCamp]
* PR内容: 遵循[MatrixOne的PR模版] (https://github.com/matrixorigin/matrixone/blob/main/.github/PULL_REQUEST_TEMPLATE.md)  
3. 提交PR完成后，在你的PR下面按以下格式评论：
评论格式: "I have finished Issue #" + PR link id
