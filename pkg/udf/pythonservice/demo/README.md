# MO Python UDF Demo

This document briefly describes the use of python UDFs in MO, showing the flexibility and convenience of python UDFs.

## Standalone Deployment

1. Before the deployment, we need to make sure we have [python3](https://www.python.org/downloads/) locally and install the grpc package in python:

```
pip install protobuf
pip install grpcio
```

2. Follow the steps in [Getting Started](https://docs.matrixorigin.cn/en/1.0.0-rc1/MatrixOne/Get-Started/install-standalone-matrixone/) to complete the installation, and modify the configuration of mo_ctl by running the following command before starting MO:

```shell
mo_ctl set_conf MO_CONF_FILE="${MO_PATH}/matrixone/etc/launch-with-python-udf-server/launch.toml"
```

3. Launch and connect to MO.

## "Hello world"

Let's start with a simple function: a python UDF that adds two numbers together.

Input the following SQL in the client to create the function:

```sql
create or replace function py_add(a int, b int) returns int language python as 
$$
def add(a, b):
  return a + b
$$
handler 'add';
```

This SQL defines a function called py_add that takes two parameters of type `int` and returns the sum of the two parameters. Python code comes after `as`. `handler` represents the name of the python function that will be called.

Now we can use the function:

```sql
select py_add(1,2);
```

Output:

```
+--------------+
| py_add(1, 2) |
+--------------+
|            3 |
+--------------+
```

We can remove the function when we no longer need it:

```sql
drop function py_add(int, int);
```

We now have a basic understanding of how to use python UDFs.

## Advancement

In this section, we'll look at some more advanced uses of python UDFs.

### Imported Python UDF

In the previous section, the py_add function is written directly in SQL, and the python code is written after the keyword `as`. We call this form **embedded UDF**. However, if the function logic is very complex, writing it directly in SQL is not a good choice. It bloats the SQL statements and makes the code less maintainable. In this case, we can choose another way to create python UDFs: importing python code from an external file, known as **imported UDF**. There are two types of files that are supported: (1) a single python file, and (2) the wheel package. Let's continue with the example of the py_add function from the previous section, and write the python code to a file.

The code for ./add func.py is as follows:

```python
def add(a, b):
  return a + b
```

Input the following SQL in the client to create the function:

```sql
create or replace function py_add(a int, b int) returns int language python import 
'./add_func.py' 
handler 'add';
```

We use the keyword `import` to import the flie add_func.py from the current working directory.

Call the function:

```sql
select py_add(1,2);
```

We still get the same output as in the previous section:

```
+--------------+
| py_add(1, 2) |
+--------------+
|            3 |
+--------------+ 
```

Importing a wheel package and a single python file share the same syntax, so we won't go into detail here. In the final example, we will create a python UDF by importing a wheel package.

### Vector Option

In some scenarios, we may want a python function to receive multiple tuples at once to improve the efficiency. For example, in model inference, we typically process data in batches, where each batch is a vector of tuples. The vector option is designed to handle this situation. We'll continue to use the py_add function as an example to demonstrate the usage of the vector option.

Input the following SQL in the client to create the function:

```sql
create or replace function py_add(a int, b int) returns int language python as 
$$
def add(a, b):  # a, b are list
  return [a[i] + b[i] for i in range(len(a))]
add.vector = True
$$
handler 'add';
```

We use `add.vector = True` to indicate that the python function add should receive two integer lists (vectors) as input, rather than individual integer values.

Call the function:

```sql
select py_add(1,2);
```

We still get the same output as in the previous section:

```
+--------------+
| py_add(1, 2) |
+--------------+
|            3 |
+--------------+ 
```

With the vector option, we have the freedom to choose how the function processes its input, whether it's one tuple at a time or multiple tuples at once.

### Type Mapping

The correspondence between MO types and python types is as follows:

| MO Types                                                 | Python Types                |
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

### Function Overloading and Matching

MO UDF supports overloading. We can create multiple functions with the same name but different types or numbers of parameters.

MO's strategy for function matching is as follows:

1. Determine candidate functions: Match all functions with the same name.
2. Determine viable functions: Select functions from the candidate functions with the same number of parameters, the same types, or those that can undergo implicit type conversion. Calculate the cost of implicit type conversion, with a cost of zero if no conversion is required.
3. Determine the best match: Choose the function with the lowest type conversion cost from all viable functions.

It's worth noting that while MO strives to find the best match, we should not rely on this mechanism, as implicit type conversion can often lead to unexpected results.

## Example: Credit Card Fraud Detection

In this section, we use the example "Credit card fraud detection" to illustrate how python UDFs can be used in a machine learning inference pipeline. (Detailed code in [github](https://github.com/matrixorigin/matrixone/tree/main/pkg/udf/pythonservice/demo))

We need to ensure that the local python environment has numpy and scikit-learn installed.

### Background

It is important that credit card companies are able to recognize fraudulent credit card transactions so that customers are not charged for items that they did not purchase. (Details in kaggle [Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud))

### Data

The dataset contains transactions made by credit cards in September 2013 by European cardholders. The data format is as follows:

| Column Name | Type   | Explanation                                                  |
| ----------- | ------ | ------------------------------------------------------------ |
| Time        | int    | Number of seconds elapsed between this transaction and the first transaction in the dataset |
| V1 ~ V28    | double | May be result of a PCA Dimensionality reduction to protect user identities and sensitive features |
| Amount      | double | Transaction amount                                           |
| Class       | int    | 1 for fraudulent transactions, 0 otherwise                   |

We split the data into training, validation, and test sets in an 8:1:1 ratio. Since the training process is not the focus. we won't go into much detail here. We treat the test set as new data appearing in the production process, and store it in MO. The DDL is as follows:

```sql
create table credit_card_transaction(
    id int auto_increment primary key,
    time int,
    features json, -- store V1 ~ V28, using json list
    amount decimal(20,2)
);
```

### Inference

After the model is trained, we can write the inference function. The core code is as follows:

```python
import decimal
import os
from typing import List

import joblib
import numpy as np

# model path
model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'model_with_scaler')


def detect(featuresList: List[List[int]], amountList: List[decimal.Decimal]) -> List[bool]:
    # load model
    model_with_scaler = joblib.load(model_path)

    columns_features = np.array(featuresList)
    column_amount = np.array(amountList, dtype='float').reshape(-1, 1)
    # normalize the amount
    column_amount = model_with_scaler['amount_scaler'].transform(column_amount)
    data = np.concatenate((columns_features, column_amount), axis=1)
    # model inference
    predictions = model_with_scaler['model'].predict(data)
    return [pred == 1 for pred in predictions.tolist()]

# enable vector option
detect.vector = True
```

Then write setup.py to build the wheel package:

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

The project directory structure is as follows:

```
|-- demo/
	|-- credit/
		|-- __init__.py
		|-- detection.py		# inference function
		|-- model_with_scaler	# model
	|-- setup.py
```

Build the wheel package detect-1.0.0-py3-none-any.whl with the command `python setup.py bdist_wheel`.

Create the function:

```sql
create or replace function py_detect(features json, amount decimal) returns bool language python import 
-- replace with the directory where the wheel package is located
'your_code_path/detect-1.0.0-py3-none-any.whl' 
-- the function detect in the module credit
handler 'credit.detect'; 
```

Call the function:

```sql
select id, py_detect(features, amount) as is_fraud from credit_card_transaction limit 10;
```

Output：

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

Now, we have completed the inference of task credit card fraud detection in MO.

From this example, it's clear that we can easily use Python UDFs to tackle tasks that SQL alone cannot handle. Python UDFs not only extend the semantics of SQL but also spare us from writing data movement and transformation programs manually, greatly enhancing the development efficiency.
