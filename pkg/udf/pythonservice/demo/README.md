# MO Python UDF Demo

本文简述了 MO 中 python udf 的使用方式，展示了 python udf 的灵活与便捷。

## 单机快速部署

当前部署方式（仅支持 linux 系统）：

1. 在部署之前，我们需要确保本地拥有 python3 的环境和 mysql 客户端，并在 python 环境下安装 grpc 相关的 package：

```shell
pip install protobuf
pip install grpcio
```

1. 下载 MO 二进制包并解压：

```shell
wget https://github.com/joker-star-l/matrixone/releases/download/python_udf_test/mo-linux-x86_64.zip
unzip mo-linux-x86_64.zip
```

3. 启动 MO（需要20到30秒的时间）：

```shell
cd mo-linux-x86_64
chmod +x ./mo-service
nohup ./mo-service -launch ./etc/launch-with-python-udf-server/launch.toml > mo.log 2>&1 &
```

4. 使用 mysql 客户端连接 MO：

```shell
mysql -h 127.0.0.1 -P 6001 -uroot -p111 --local-infile
```

后续发布后可能的部署方式：

1. 在部署之前，我们需要确保本地拥有 python3 的环境。

2. 按照官方文档[快速开始](https://docs.matrixorigin.cn/1.0.0-rc1/MatrixOne/Get-Started/install-standalone-matrixone/)中的步骤完成相关安装，在启动 MO 之前，执行以下命令修改 mo_ctl 的配置：

```shell
mo_ctl set_conf MO_CONF_FILE="${MO_PATH}/matrixone/etc/launch-with-python-udf-server/launch.toml"
```

3. 启动并连接 MO。

## "Hello world"

我们从一个简单的函数开始说起：用 python udf 实现两数之和。

在客户端中输入以下 sql 来创建函数：

```sql
create or replace function py_add(a int, b int) returns int language python as 
$$
def add(a, b):
  return a + b
$$
handler 'add';
```

这段 sql 定义了一个名为 `py_add` 的函数，接收两个类型为 `int` 的参数，其功能是返回两个参数的和，具体逻辑在 `as` 后的 python 代码中，`handler` 表示调用的 python 函数名称。

现在便可以使用该函数了：

```sql
select py_add(1,2);
```

输出：

```
+--------------+
| py_add(1, 2) |
+--------------+
|            3 |
+--------------+
```

当我们不再需要该函数时，可以将其删除：

```sql
drop function py_add(int, int);
```

至此，我们已经掌握了 python udf 的基本用法。

## 进阶

本节主要介绍 python udf 的一些进阶用法。

### 导入式 python udf

在上一节中，py_add 函数是直接写在 sql 中的，python 代码写在 `as` 关键字之后，我们称这种形式为**嵌入式 udf**。然而，如果函数逻辑十分复杂，直接写在 sql 中并不是一个好的选择，这会让 sql 语句膨胀而且不利于代码的维护。这时，我们可以选择另一种创建 python udf 的方式：从外部文件中导入 python 代码，即**导入式 udf**。支持导入的文件有两种：(1) 单个 python 文件，(2) wheel 包。我们仍旧以上一节的 py_add 函数为例，把 python 代码写到文件中。

./add_func.py 代码如下：

```python
def add(a, b):
  return a + b
```

在客户端中输入以下 sql 来创建函数：

```sql
create or replace function py_add(a int, b int) returns int language python import 
'./add_func.py' 
handler 'add';
```

其中，我们使用 `import` 关键字来导入当前工作目录下的 add_func.py 文件。

调用函数：

```sql
select py_add(1,2);
```

仍旧可以得到与上一节相同的输出：

```
+--------------+
| py_add(1, 2) |
+--------------+
|            3 |
+--------------+ 
```

导入 wheel 包与单个 python 文件具有相同的语法，这里不再赘述。在最后的案例中，我们将会以导入 wheel 包的形式来创建 python udf。

### Vector 选项

在某些场景下，我们会希望 python 函数一次性接收多个元组来提高运行效率。如在模型推理时，我们通常是以一个 batch 为单位进行的，这里的 batch 即为元组的 vector。而 vector 选项正是用来处理这种情况的。我们仍旧以 py_add 函数为例，展示 vector 选项的用法。

在客户端中输入以下 sql 来创建函数：

```sql
create or replace function py_add(a int, b int) returns int language python as 
$$
def add(a, b):  # a, b are list
  return [a[i] + b[i] for i in range(len(a))]
add.vector = True
$$
handler 'add';
```

其中，我们使用 `add.vector = True` 来标记 python 函数 add 接收两个 int 列表（vector）而不是 int 值。

调用函数：

```sql
select py_add(1,2);
```

仍旧可以得到与上一节相同的输出：

```
+--------------+
| py_add(1, 2) |
+--------------+
|            3 |
+--------------+ 
```

有了 vector 选项，我们就可以自由地选择函数的处理形式：一次一元组，或者一次多元组。

### 类型映射

MO 类型与 python 类型的对应关系如下：

| mo 类型                                                  | python 类型                 |
| -------------------------------------------------------- | --------------------------- |
| bool                                                     | bool                        |
| int8, int16, int32, int64, uint8, uint16, uint32, uint64 | int                         |
| float32, float64                                         | float                       |
| char, varchar, text, uuid                                | str                         |
| json                                                     | str, int, float, list, dict |
| time                                                     | datetime.timedelta          |
| date                                                     | datetime.date               |
| datetime, timestamp                                      | datetime.datetime           |
| decimal64, decimal128                                    | decimal.Decimal             |
| binary, varbinary, blob                                  | bytes                       |

### 函数重载与匹配

MO udf 支持重载，我们可以创建多个名称相同但参数类型或数量不同的函数。

在进行函数匹配时，MO 采取的策略如下：

1. 确定候选函数：匹配所有相同名称的函数。
2. 确定可行函数：从候选函数中选出参数数量相同、类型相同或可以隐式转换的函数，计算隐式类型转换的代价，如果无需转换则代价为零。
3. 确定最佳匹配：在所有可行函数中确定类型转换代价最低的函数。

值得注意的是，虽然 MO 会尽可能地去找到最佳匹配，但我们不应当依赖于这种机制，因为隐式类型转换往往会产生意想不到的结果。

## 案例：信用卡欺诈检测

本节以“信用卡欺诈检测”为例，讲述了 python udf 在机器学习推理流水线中的应用。（相关代码详见 [github](https://github.com/joker-star-l/matrixone/tree/dev/pkg/udf/pythonservice/demo)）

在本节中，我们需要确保本地 python 环境已安装了 numpy 和 scikit-learn。

### 背景

信用卡公司需要识别欺诈交易，以防止客户的信用卡被他人恶意使用。（详见 kaggle [Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud)）

### 数据

数据集包含了2013年9月欧洲持卡人使用信用卡进行的交易记录。数据格式如下：

| 列名     | 类型   | 含义                                            |
| -------- | ------ | ----------------------------------------------- |
| Time     | int    | 此交易与数据集中的第一个交易之间经过的秒数      |
| V1 ~ V28 | double | 使用 PCA 提取的特征（以保护用户身份和敏感特征） |
| Amount   | double | 交易金额                                        |
| Class    | int    | 1：欺诈交易，0：非欺诈交易                      |

我们把数据按照8: 1: 1的比例划分为训练集、验证集和测试集。由于训练过程不是本文的重点，不在此处进行过多的介绍。我们把测试集当作生产过程中新出现的数据存储到 MO 中，DDL 如下：

```sql
create table credit_card_transaction(
    id int auto_increment primary key,
    time int,
    features json, -- 对应 V1 ~ V28，使用 json list 存储
    amount decimal(20,2)
);
```

### 推理

训练好模型之后，我们就可以编写推理函数了。其核心代码如下：

```python
import decimal
import os
from typing import List

import joblib
import numpy as np

# 模型路径
model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'model_with_scaler')


def detect(featuresList: List[List[int]], amountList: List[decimal.Decimal]) -> List[bool]:
    # 加载模型
    model_with_scaler = joblib.load(model_path)

    columns_features = np.array(featuresList)
    column_amount = np.array(amountList, dtype='float').reshape(-1, 1)
    # 对金额进行归一化
    column_amount = model_with_scaler['amount_scaler'].transform(column_amount)
    data = np.concatenate((columns_features, column_amount), axis=1)
    # 模型推理
    predictions = model_with_scaler['model'].predict(data)
    return [pred == 1 for pred in predictions.tolist()]

# 开启 vector 选项
detect.vector = True
```

然后编写 setup.py 用于构建 wheel 包：

```python
from setuptools import setup, find_packages

setup(
    name="detect",
    version="1.0.0",
    packages=find_packages(),
    package_data={
        'credit': ['model_with_scaler']  # 模型
    },
)
```

整个项目的目录结构如下：

```
|-- demo/
	|-- credit/
		|-- __init__.py
		|-- detection.py		# 推理函数
		|-- model_with_scaler	# 模型
	|-- setup.py
```

使用命令 `python setup.py bdist_wheel` 构建 wheel 包 detect-1.0.0-py3-none-any.whl。

创建函数：

```sql
create or replace function py_detect(features json, amount decimal) returns bool language python import 
'your_code_path/detect-1.0.0-py3-none-any.whl' -- 替换为 wheel 包所在目录
handler 'credit.detect'; -- credit module 下的 detect 函数
```

调用函数：

```sql
select id, py_detect(features, amount) as is_fraud from credit_card_transaction limit 10;
```

输出：

```
+---------+----------+
| id      | is_fraud |
+---------+----------+
|       1 | false    |
|       2 | false    |
|       3 | true     |
|       4 | false    |
|       5 | false    |
|       6 | false    |
|       7 | false    |
|       8 | true     |
|       9 | false    |
|      10 | false    |
+---------+----------+
```

至此，我们已经在 MO 中完成了信用卡欺诈检测任务的推理。

通过该案例可以看出，有了 python udf，我们可以方便地通过自定义 udf 来处理 sql 解决不了的任务。Python udf 既扩展了 sql 的语义，又避免了我们手动编写数据移动和转换的程序，极大地提高了开发效率。
