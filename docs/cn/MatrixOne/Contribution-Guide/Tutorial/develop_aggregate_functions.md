# **开发聚合函数**

## **前提条件**

为 MatrixOne 开发聚合函数，你需要具备 Golang 编程的基本知识。你可以通过[Golang 教程](https://www.educative.io/blog/golang-tutorial)来学习一些基本的 Golang 概念。

## **开始前准备**

在你开始之前，请确保你已经安装了 Golang，并将 MatrixOne 代码库克隆到你的本地。

更多信息，参见[准备工作](../How-to-Contribute/preparation.md) 和[代码贡献](../How-to-Contribute/contribute-code.md)。

## **什么是聚合函数**

在数据库系统中，聚合函数是将多行的值组合在一起以形成单个汇总值的函数。

常用的聚合函数包括:

* `COUNT` 计算在一个特定列中有多少行。

* `SUM` 指将特定列中的所有值相加。

* `MIN` 和 `MAX` 分别返回特定列中的最低值和最高值。

* `AVG` 计算一组选定值的平均值。

## **MatrixOne 中的聚合函数**

与其他先进的数据库相比，MatrixOne 数据库中的 `JOIN` 函数非常高效。聚合函数就是提高执行 `JOIN` 时的效率中一个重要特性。

为了在 MatrixOne 中实现聚合函数，我们设计了一个名为 `Ring` 的数据结构。每个聚合函数都需要执行 `Ring` 接口，以便在 `join` 发生时进行因数分解。

以常见的聚合函数 `AVG` 为例，我们需要计算组的数量和它们的总数，然后得到一个平均结果。

这是数据库设计中常见的实践。然而，当在两个表之间进行 `join` 查询时，通常的方法是通过先连接表来获得一个笛卡尔积，然后用笛卡尔积进行 `AVG` 运算，因此，得到的笛卡尔积可能非常大，计算成本非常高。

在 MatrixOne 中，执行 `join` 操作之前， MatrixOne 采用因数分解方法下推组统计和，以及求和计算。这种方法大大降低了计算和存储成本。因式分解是通过 `Ring` 接口及其内部函数实现的。

更多关于因式分解理论和因式分解数据库，参见[分解数据库原理](https://fdbresearch.github.io/principles.html)。

## **什么是 `Ring`**

`Ring` is an important data structure for MatrixOne factorisation, as well as a mathematical algebraic concept with a clear [definition](https://en.wikipedia.org/wiki/Ring_(mathematics)).
An algebraic `Ring` is a set equipped with two binary operations  `+` (addition) and `⋅` (multiplication) satisfying several axioms.

A `Ring` in MatrixOne is an interface with several functions similar to the algebraic `Ring` structure. We use `Ring` interface to implement aggragate functions.
The `+`(addition) is defined as merging two `Ring`s groups and the `⋅`(multiplication) operation is defined as the computation of a grouped aggregate value combined with its grouping key frequency information.

`Ring` 是 MatrixOne 因式分解的重要数据结构，也是一个具有明确[定义](https://en.wikipedia.org/wiki/Ring_(mathematics))的数学代数概念。
代数 `Ring` 是一个包含两个二元运算 “+”（加法）和 “⋅”（乘法）组成的集合，并且满足多个公理。

MatrixOne 中的 `Ring` 是一个接口，具有类似于代数 `Ring` 结构的多个功能。我们使用 `Ring` 接口来实现聚合函数。

`+`（加法）表示合并两个 `Ring` 组；`⋅`（乘法）表示定义为计算分组聚合值及其分组关键频率信息。

| `Ring` 接口的方法 | 它能做什么                                                |
| ------------------------ | ------------------------------------------------------ |
| Count                    | Return the number of groups                                       |
| Size                     | Return the memory size of Ring                                   |
| Dup                      | Duplicate a Ring of same type                                    |
| Type                     | Return the type of a Ring                                         |
| String                   | Return some basic information of Ring for execution plan log   |
| Free                     | Free the Ring memory                                         |
| Grow                     | Add a group for the Ring                                    |
| Grows                    | Add multiple groups for the Ring                            |
| SetLength                | Shrink the size of Ring, keep the first N groups                       |
| Shrink                   | Shrink the size of Ring, keep the designated groups             |
| Eval                     | Return the eventual result of the aggregate function |
| Fill                     | Update the data of Ring by a row                                 |
| BulkFill                 | Update the ring data by a whole vector                                   |
| BatchFill                | Update the ring data by a part of vector                                    |
| Add                      | Merge a couple of groups for two Rings                             |
| BatchAdd                 | Merge several couples of groups for two Rings                           |
| Mul                      | Multiplication between groups for two Rings, called when join occurs      |

注：6.0版本 ring暂时被删除，不在/pkg/container/ring/下
`Ring` 数据结构在路径 `/pkg/container/ring/` 下实现。

## **`Ring` 如何进行查询**

为了更好地理解 `Ring` 接口，这里以聚合函数 `sum()` 为例，讲述整个 `Ring` 的执行过程。

`Ring` 数据结构下的聚合函数，有两种不同的场景，参见下面的章节。

### 1. 单表查询

在单表查询场景中，运行下面的查询时，根据表 T1 存储的存储块的情况，它会生成一个或多个 `Ring`。存储块的数量取决于存储策略，每个 `Ring` 将存储几组 `sums`，而组的数量取决于有多少重复的行在执行 `Ring`。

```sql
T1 (id, class, age)
+------+-------+------+
| id   | class | age  |
+------+-------+------+
|    1 | one   |   23 |
|    2 | one   |   20 |
|    3 | one   |   22 |
|    4 | two   |   20 |
|    5 | two   |   19 |
|    6 | three |   18 |
|    7 | three |   20 |
|    8 | three |   21 |
|    9 | three |   24 |
|   10 | three |   19 |
+------+-------+------+

select class, sum(age) from T1 group by class;
```

例如，如果生成了两个 `Ring` ，第一个 `Ring` 包含前4行的总和，如下所示：

```sql
|  one   |  23+20+22 |
|  two   |  20       |
```

第二个 `Ring` 包含了最后6行的总和，如下所示：

```sql
|  two   |  19       |
|  three |  18+20+21+24+19 |
```

先调用 `Ring` 的 `Add` 方法将两个组合并到一起，再调用 `Eval` 方法将整体结果返回。

```sql
|  one   |  23+20+22       |
|  two   |  20+19       |
|  three |  18+20+21+24+19 |
```

### 2. 连接多个表的查询

在多表连接查询场景中，假设已建好两个表 `Tc` 和 `Ts`，查询示例如下所示：

```sql
Tc (id, class)
+------+-------+
| id   | class |
+------+-------+
|    1 | one   |
|    2 | one   |
|    3 | one   |
|    4 | two   |
|    5 | two   |
|    6 | three |
|    7 | three |
|    8 | three |
|    9 | three |
|   10 | three |
+------+-------+

Ts (id, age)
+------+------+
| id   | age  |
+------+------+
|    1 |   23 |
|    2 |   20 |
|    3 |   22 |
|    4 |   20 |
|    5 |   19 |
|    6 |   18 |
|    7 |   20 |
|    8 |   24 |
|    9 |   24 |
|   10 |   19 |
+------+------+

select class, sum(age) from Tc join Ts on Tc.id = Ts.id group by class;
```

执行这个查询时，这个查询首先是对 `age` 这列执行聚合，那么它将首先为 `Ts` 表生成 `Ring`。

它也可能生成一个或多个 `Ring` 相同的单表。为了易理解，这里假设每个表只创建一个 `Ring`。

`Ring-Ts` 将开始计算 *id* 组的和。然后创建一个哈希表来执行 `join` 操作。

在执行 `join` 的同时创建`Ring-Tc`，`Ring-Tc` 将计算 *id* 出现的频率 `f`，然后调用 `Ring-Ts` 的 `Mul` 方法来计算从 `Ring-Ts` 计算的总和和从 `Ring-Tc` 计算出的频率。

```
sum[i] = sum[i] * f[i]
```

首先得到 `[class, sum(age)]` 的值，然后执行 `group by class`，得到最终结果。

## **详解因式分解**

从上面的例子中，你可以看到 `Ring` 执行一些预计算，只有结果(如 `sum`)存储在它的结构中。当执行 `join` 这样的运算时，只需要简单的 `Add` 或 `Multiplication` 就可以得到结果，这在**因式分解**中被称为下推运算。在下推运算的帮助下，数据库将无需处理高成本的笛卡尔积，并且随着连接表数量的增加，因式分解执行仅仅线性增长，而不是指数增长。

本章节以“方差”函数的实现为例，详细讲解因式分解，方差公式如下：

```
Variance = Σ [(xi - x̅)^2]/n

Example: xi = 10,8,6,12,14, x̅ = 10
Calculation: ((10-10)^2+(8-10)^2+(6-10)^2+(12-10)^2+(14-10)^2)/5 = 8
```

If we proceed with implementing this formula, we have to record all values of each group, and also maintain these values with `Add` and `Mul` operations of `Ring`. Eventually the result is calculated in an `Eval()` function. This implementation has a drawback of high memory cost since we have to store all the values during processing.

In the `Avg` implementation, it doesn't store all values in the `Ring`. Instead it stores only the `sum` of each group and the null numbers.  It returns the final result with a simple division. This method saves a lot of memory space.

Now let's turn the `Variance` formula a bit into a different form:

执行这个公式，必须记录每组的值，并通过 `Ring` 的 `Add` 和 `Mul` 操作来操作这些值，最终结果将在 `Eval()` 函数中进行计算。在处理计算过程中，需要存储所有值，这种实现方法的缺点是内存成本高

在 `Avg` 实现过程中，它没有将所有值存储在 `Ring` 中。相反，它只存储每个组的 `sum` 和空值。返回结果为只含有简单除法的结果，这种方法节省了大量的内存空间。

下面示例，将“方差”公式转换成另一种形式：

```
Variance = Σ (xi^2)/n-x̅^2

Example: xi = 10,8,6,12,14, x̅ = 10
Calculation: (10^2+8^2+6^2+12^2+14^2)/5-10^2 = 8
```

This formula's result is exactly the same as the previous one, but we only have to record the values sum of `xi^2` and the sum of `xi`. We can largely reduce the memory space with this kind of reformulation.

To conclude, every aggregate function needs to find a way to record as little values as possible in order to reduce memory cost.
Below are two different implementations for `Variance` (the second one has a better performance):

这个公式的结果和上一条公式的计算结果完全一样，在计算过程中，只需要记录 *xi^2* 和 *xi* 的和。通过这种重构，大大减少了内存空间。

总之，每个聚合函数都可以找到一种方法来记录尽可能少的值，以降低使用内存空间。

下面是“方差”的两种不同实现过程，且第二个实现过程具有更好的性能：

```go
   //Implementation1

   type VarRing struct {
    // Typ is vector's value type
    Typ types.Type

    // attributes for computing the variance
    Data []byte    // store all the Sums' bytes
    Sums  []float64 // sums of each group, its memory address is same to Data

    Values [][]float64  // values of each group
    NullCounts []int64   // group to record number of the null value
   }

   //Implementation2
   type VarRing struct {
   	// Typ is vector's value type
   	Typ types.Type

   	// attributes for computing the variance
   	Data  []byte
   	SumX  []float64 // sum of x, its memory address is same to Data, because we will use it to store result finally.
   	SumX2 []float64 // sum of x^2

   	NullCounts []int64 // group to record number of the null value
   }
```

## **开发 `var()` 函数**

在本教程中，我们将以两种不同的方法为例，介绍 Variance(获得标准总体方差值)聚合函数的完整实现过程。

### 步骤 1：注册函数

MatrixOne 不区分运算符和函数。

在代码存储库中，文件 `pkg/sql/viewexec/transformer/types.go` 将聚合函数注册为操作符，并为每个操作符赋值一个不同的整数。

新增一个新函数 `var()`，首先在常量声明中添加一个新的常量 `Variance`，在名称声明中添加一个 `var`。

```go
   const (
   	Sum = iota
   	Avg
   	Max
   	Min
   	Count
   	StarCount
   	ApproxCountDistinct
   	Variance
   )

   var TransformerNames = [...]string{
   	Sum:                 "sum",
   	Avg:                 "avg",
   	Max:                 "max",
   	Min:                 "min",
   	Count:               "count",
   	StarCount:           "starcount",
   	ApproxCountDistinct: "approx_count_distinct",
   	Variance:			 "var",
   }
```

### 步骤 2：执行 `Ring` 接口

1. 定义 `Ring` 结构：在 `pkg/container/ring` 下创建 `variance.go`，定义 `VarRing` 的结构。

   当我们计算总体方差时，我们需要计算：

   - 计算数值 `Sums` 和每组的空值的平均值。
   - 计算各组值的方差。

```go
   //Implementation1

   type VarRing struct {
    // Typ is vector's value type
    Typ types.Type

    // attributes for computing the variance
    Data []byte    // store all the Sums' bytes
    Sums  []float64 // sums of each group, its memory address is same to Data

    Values [][]float64  // values of each group
    NullCounts []int64   // group to record number of the null value
   }

   //Implementation2
   type VarRing struct {
   	// Typ is vector's value type
   	Typ types.Type

   	// attributes for computing the variance
   	Data  []byte
   	SumX  []float64 // sum of x, its memory address is same to Data, because we will use it to store result finally.
   	SumX2 []float64 // sum of x^2

   	NullCounts []int64 // group to record number of the null value
   }
```

2. 实现 `Ring` 接口的功能

有关完整实现过程，参见[variance.go](https://github.com/matrixorigin/matrixone/blob/main/pkg/container/ring/variance/variance.go)。

* `Fill` 函数

```go
      //Implementation1

    func (v *VarRing) Fill(i, j int64, z int64, vec *vector.Vector) {
       	var value float64 = 0

       	switch vec.Typ.Oid {
       	case types.T_int8:
       		value = float64(vec.Col.([]int8)[j])
       	case ...
       	}
       	for k := z; k > 0; k-- {
       		v.Values[i] = append(v.Values[i], value)
       	}

       	v.Sums[i] += value * float64(z)

       	if nulls.Contains(vec.Nsp, uint64(z)) {
       		v.NullCounts[i] += z
       	}
       }

       //Implementation2

    func (v *VarRing) Fill(i, j int64, z int64, vec *vector.Vector) {
      	var value float64 = 0

      	switch vec.Typ.Oid {
      	case types.T_int8:
      		value = float64(vec.Col.([]int8)[j])
      	case ...
      	}

      	v.SumX[i] += value * float64(z)
      	v.SumX2[i] += math.Pow(value, 2) * float64(z)

      	if nulls.Contains(vec.Nsp, uint64(z)) {
      		v.NullCounts[i] += z
      	}
      }
```

* `Add` 函数

```go

      //Implementation1
      func (v *VarRing) Add(a interface{}, x, y int64) {
          v2 := a.(*VarRing)
          v.Sums[x] += v2.Sums[y]
          v.NullCounts[x] += v2.NullCounts[y]
          v.Values[x] = append(v.Values[x], v2.Values[y]...)
      }

      //Implementation2
      func (v *VarRing) Add(a interface{}, x, y int64) {
      	v2 := a.(*VarRing)
      	v.SumX[x] += v2.SumX[y]
      	v.SumX2[x] += v2.SumX2[y]
      	v.NullCounts[x] += v2.NullCounts[y]
      }

```

* `Mul` 函数  

```go

      //Implementation1
      func (v *VarRing) Mul(a interface{}, x, y, z int64) {
          v2 := a.(*VarRing)
          {
              v.Sums[x] += v2.Sums[y] * float64(z)
              v.NullCounts[x] += v2.NullCounts[y] * z
              for k := z; k > 0; k-- {
                  v.Values[x] = append(v.Values[x], v2.Values[y]...)
              }
          }
      }

      //Implementation2
      func (v *VarRing) Mul(a interface{}, x, y, z int64) {
      	v2 := a.(*VarRing)
      	{
      		v.SumX[x] += v2.SumX[y] * float64(z)
      		v.SumX2[x] += v2.SumX2[y] * float64(z)
      		v.NullCounts[x] += v2.NullCounts[y] * z
      	}
      }

```

* `Eval` 函数

```go
         //Implementation1
      func (v *VarRing) Eval(zs []int64) *vector.Vector {
          defer func() {
              ...
          }()

          nsp := new(nulls.Nulls)
          for i, z := range zs {
              if n := z - v.NullCounts[i]; n == 0 {
                  nulls.Add(nsp, uint64(i))
              } else {
                  v.Sums[i] /= float64(n)

                  var variance float64 = 0
                  avg := v.Sums[i]
                  for _, value := range v.Values[i] {
                      variance += math.Pow(value-avg, 2.0) / float64(n)
                  }
                  v.Sums[i] = variance
              }
          }

          return ...
      }

       //Implementation2
      func (v *VarRing) Eval(zs []int64) *vector.Vector {
      	defer func() {
      		...
      	}()

      	nsp := new(nulls.Nulls)
      	for i, z := range zs {
      		if n := z - v.NullCounts[i]; n == 0 {
      			nulls.Add(nsp, uint64(i))
      		} else {
      			v.SumX[i] /= float64(n)  // compute E(x)
      			v.SumX2[i] /= float64(n) // compute E(x^2)

      			variance := v.SumX2[i] - math.Pow(v.SumX[i], 2)

      			v.SumX[i] = variance // using v.SumX to record the result and return.
      		}
      	}

      	return ...
      }
```

3. 实现 `VarRing` 的编码和解码：在`pkg/sql/protocol/protocol.go`文件中，实现`VarRing`的序列化和反序列化代码。

   | 序列化函数 | 反序列化函数   |
   | ------------------ | ----------------------------------- |
   | EncodeRing         | DecodeRing<br>DecodeRingWithProcess |

   - 序列化：

```go
   case *variance.VarRing:
   		buf.WriteByte(VarianceRing)
   		// NullCounts
   		n := len(v.NullCounts)
   		buf.Write(encoding.EncodeUint32(uint32(n)))
   		if n > 0 {
   			buf.Write(encoding.EncodeInt64Slice(v.NullCounts))
   		}
   		// Sumx2
   		n = len(v.SumX2)
   		buf.Write(encoding.EncodeUint32(uint32(n)))
   		if n > 0 {
   			buf.Write(encoding.EncodeFloat64Slice(v.SumX2))
   		}
   		// Sumx
   		da := encoding.EncodeFloat64Slice(v.SumX)
   		n = len(da)
   		buf.Write(encoding.EncodeUint32(uint32(n)))
   		if n > 0 {
   			buf.Write(da)
   		}
   		// Typ
   		buf.Write(encoding.EncodeType(v.Typ))
   		return nil
```

   - 反序列化：

```go
  case VarianceRing:
   		r := new(variance.VarRing)
   		data = data[1:]

   		// decode NullCounts
   		n := encoding.DecodeUint32(data[:4])
   		data = data[4:]
   		if n > 0 {
   			r.NullCounts = make([]int64, n)
   			copy(r.NullCounts, encoding.DecodeInt64Slice(data[:n*8]))
   			data = data[n*8:]
   		}
   		// decode Sumx2
   		n = encoding.DecodeUint32(data[:4])
   		data = data[4:]
   		if n > 0 {
   			r.SumX2 = make([]float64, n)
   			copy(r.SumX2, encoding.DecodeFloat64Slice(data[:n*8]))
   			data = data[n*8:]
   		}
   		// decode Sumx
   		n = encoding.DecodeUint32(data[:4])
   		data = data[4:]
   		if n > 0 {
   			r.Data = data[:n]
   			data = data[n:]
   		}
   		r.SumX = encoding.DecodeFloat64Slice(r.Data)
   		// decode typ
   		typ := encoding.DecodeType(data[:encoding.TypeSize])
   		data = data[encoding.TypeSize:]
   		r.Typ = typ
   		// return
   		return r, data, nil
```

现在我们可以启动 MatrixOne 并尝试使用 `var()` 函数了。

## **编译并运行 MatrixOne**

本章节讲述编译并运行 MatrixOne 来查看函数的行为。

### 步骤 1：运行 `make config` 和 `make build` 来编译 MatrixOne 并构建二进制文件

```
make build
```

!!! info
    `make config` 运行完成将生成一个新的配置文件。在本教程中，你只需要运行一次。如果你修改了一些代码并想重新编译，你只需要运行 `make build`。

### 步骤 2：运行 `./mo-service -cfg ./etc/cn-standalone-test.toml` 启动 MatrixOne，MatrixOne 服务将开始监听客户端连接

```
./mo-service -cfg ./etc/cn-standalone-test.toml
```

!!! info

   `system_vars_config.toml` 的记录器打印级别设置为默认为 `DEBUG`，它将为您打印很多信息。如果你只想获得函数的打印信息，你可以修改 `system_vars_config.toml` 并将 `cubeLogLevel` 和 `level` 设置为 `ERROR` 级别。

	 cubeLogLevel = "error"

	 level = "error"

!!! info
    如果端口 50000 产生 `port is in use` 错误。你可以通过 `lsof -i:50000` 来查看占用 50000 端口的进程。`lsof -i:50000` 命令帮助你获取该进程的 PIDNAME，然后你可以通过 `kill -9 PIDNAME` 关闭该进程。

### 步骤 3：通过 MySQL Client 连接 MatrixOne

使用内置账户：

- user: dump
- password: 111

```
mysql -h 127.0.0.1 -P 6001 -udump -p
Enter password:
```

### 步骤 4：使用数据测试你的函数行为

参考下面的示例，检查是否得到正确的方差结果。

你也可以使用 `inner join` 并检查结果，如果结果正确，则表示因数分解是有效的。

```sql
mysql>select * from variance;
+------+------+
| a    | b    |
+------+------+
|    1 |    4 |
|   10 |    3 |
|   19 |   12 |
|  239 |  114 |
|   49 |  149 |
|   10 |  159 |
|    1 |    3 |
|   34 |   35 |
+------+------+
8 rows in set (0.04 sec)

mysql> select * from variance2;
+------+------+
| a    | b    |
+------+------+
|   14 | 3514 |
|   10 | 3514 |
|    1 |   61 |
+------+------+
3 rows in set (0.02 sec)

mysql> select var(variance.a), var(variance.b) from variance;
+-----------------+-----------------+
| var(variance.a) | var(variance.b) |
+-----------------+-----------------+
|       5596.2344 |       4150.1094 |
+-----------------+-----------------+
1 row in set (0.06 sec)

mysql> select variance.a, var(variance.b) from variance inner join variance2 on variance.a =  variance2.a group by variance.a;
+------------+-----------------+
| variance.a | var(variance.b) |
+------------+-----------------+
|         10 |       6084.0000 |
|          1 |          0.2500 |
+------------+-----------------+
2 rows in set (0.04 sec)
```

!!! info
    除了 `var()` 之外，MatrixOne 已经有一些聚合函数的简洁示例，例如 `sum()`、`count()`、`max()`、`min()` 和 `avg()`。稍作相应改动后，过程与其他功能基本相同。

## **为你的函数编写测试单元**

本章节将指导你为新函数编写一个单元测试。

Go 有一个名为 `go test` 的内置测试命令和一个名为 *testing* 的测试包，它们结合起来可以提供最小但完整的测试体验。它自动执行表单的任何函数。

```
func TestXxx(*testing.T)
```

编写一个新的测试套件，先创建一个以 *_test.go* 结尾的文件，命名需要描述函数名称，例如 *variance_test.go*；将 *variance_test.go* 文件与测试文件放在同一个包中，打包构建时，不包含 *variance_test.go* 文件，但在运行 `go test` 命令时会包含 *variance_test.go* 文件，详细步骤，参见下述步骤。

### 步骤 1：在 `pkg/container/ring/variance/` 目录下新建一个名为 `variance_test.go` 的文件，并导入用于测试的 `testing` 框架和 `reflect` 框架

```go
package variance

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"reflect"
	"testing"
)

// TestVariance just for verify varRing related process
func TestVariance(t *testing.T) {

}
```

### 步骤 2：通过一些预定义值执行 `TestVariance` 函数

```go
  func TestVariance(t *testing.T) {
   	// verify that if we can calculate
   	// the variance of {1, 2, null, 0, 3, 4} and {2, 3, null, null, 4, 5} correctly

   	// 1. make the test case
   	v1 := NewVarianceRing(types.Type{Oid: types.T_float64})
   	v2 := v1.Dup().(*VarRing)
   	{
   		// first 3 rows.
   		// column1: {1, 2, null}, column2: {2, 3, null}
   		v1.SumX = []float64{1+2, 2+3}
   		v1.SumX2 = []float64{1*1+2*2, 2*2+3*3}
   		v1.NullCounts = []int64{1, 1}
   	}
   	{
   		// last 3 rows.
   		// column1: {0, 3, 4}, column2: {null, 4, 5}
   		v2.SumX = []float64{0+3+4, 4+5}
   		v2.SumX2 = []float64{3*3+4*4, 4*4+5*5}
   		v2.NullCounts = []int64{0, 1}
   	}
   	v1.Add(v2, 0, 0)
   	v1.Add(v2, 1, 1)

   	result := v1.Eval([]int64{6, 6})

   	expected := []float64{2.0, 1.25}
   	if !reflect.DeepEqual(result.Col, expected) {
   		t.Errorf(fmt.Sprintf("TestVariance wrong, expected %v, but got %v", expected, result.Col))
   	}
   }
```

### 步骤 3：在 `pkg/sql/protocol/protocol_test.go` 文件内的函数 `TestRing` 中完成序列化和反序列化的单元测试

你可以检查 `VarRing` 的完整测试代码。

```go
  &variance.VarRing{
   			NullCounts: []int64{1, 2, 3},
   			SumX:       []float64{4, 9, 13},
   			SumX2: []float64{16, 81, 169},
   			Typ: types.Type{Oid: types.T(types.T_float64), Size: 8},
   }

   case *variance.VarRing:
   			oriRing := r.(*variance.VarRing)
   			// Sumx
   			if string(ExpectRing.Data) != string(encoding.EncodeFloat64Slice(oriRing.SumX)) {
   				t.Errorf("Decode varRing Sums failed.")
   				return
   			}
   			// NullCounts
   			for i, n := range oriRing.NullCounts {
   				if ExpectRing.NullCounts[i] != n {
   					t.Errorf("Decode varRing NullCounts failed. \nExpected/Got:\n%v\n%v", n, ExpectRing.NullCounts[i])
   					return
   				}
   			}
   			// Sumx2
   			for i, v := range oriRing.SumX2 {
   				if !reflect.DeepEqual(ExpectRing.SumX2[i], v) {
   					t.Errorf("Decode varRing Values failed. \nExpected/Got:\n%v\n%v", v, ExpectRing.SumX2[i])
   					return
   				}
   			}

```

### 步骤 4：启动测试

1. 在与步骤 3 同一个目录下进行测试：

   ```
   go test
   ```

   这条命令会选取任何与 *packagename_test.go* 匹配的文件，在本示例中，执行 `go test` 将会选取测试 *variance_test.go* 文件。

2. 运行结果为 `PASS`，表示通过了单元测试。

   在 MatrixOne 中，我们有一个 `bvt` 测试框架，它将运行整个包中定义的所有单元测试，并且每次你向代码库发出拉取请求时，测试将自动运行。

3. `bvt` 测试通过，你的代码将被合并。

## **进行性能测试**

聚合函数是数据库系统的一个重要特性，当查询数以亿计的数据行时，聚合函数的时间消耗是相当大的。

因此，你可以试运行一个性能测试。

### 步骤 1：载标准测试数据集

本章节为你准备了一个包含1000万行数据的单表SSB查询数据集。原始数据文件大小约为4GB，压缩后为500MB。点击下面的链接，直接获取数据文件：

```
https://community-shared-data-1308875761.cos.ap-beijing.myqcloud.com/lineorder_flat.tar.bz2
```

### 步骤 2：解压文件，将数据加载到 MatrixOne

使用下面的 SQL 命令，可以创建数据库和表，加载数据 `lineorder_flat.tbl` 到 MatrixOne。

```sql
create database if not exists ssb;
use ssb;
drop table if exists lineorder_flat;
CREATE TABLE lineorder_flat(
  LO_ORDERKEY bigint key,
  LO_LINENUMBER int,
  LO_CUSTKEY int,
  LO_PARTKEY int,
  LO_SUPPKEY int,
  LO_ORDERDATE date,
  LO_ORDERPRIORITY char(15),
  LO_SHIPPRIORITY tinyint,
  LO_QUANTITY double,
  LO_EXTENDEDPRICE double,
  LO_ORDTOTALPRICE double,
  LO_DISCOUNT double,
  LO_REVENUE int unsigned,
  LO_SUPPLYCOST int unsigned,
  LO_TAX double,
  LO_COMMITDATE date,
  LO_SHIPMODE char(10),
  C_NAME varchar(25),
  C_ADDRESS varchar(25),
  C_CITY char(10),
  C_NATION char(15),
  C_REGION char(12),
  C_PHONE char(15),
  C_MKTSEGMENT char(10),
  S_NAME char(25),
  S_ADDRESS varchar(25),
  S_CITY char(10),
  S_NATION char(15),
  S_REGION char(12),
  S_PHONE char(15),
  P_NAME varchar(22),
  P_MFGR char(6),
  P_CATEGORY char(7),
  P_BRAND char(9),
  P_COLOR varchar(11),
  P_TYPE varchar(25),
  P_SIZE int,
  P_CONTAINER char(10)
);

load data infile '/Users/YOURPATH/lineorder_flat.tbl' into table lineorder_flat FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';

```

加载数据集成功，则代码示例如下：

```
Query OK, 10272594 rows affected (1 min 7.09 sec)
```

### 步骤 3：分别在 `LO_SUPPKEY` 列上运行聚合函数 `sum()` 和 `avg()` 来验证性能

```sql
select avg(LO_SUPPKEY) from lineorder_flat;
select sum(LO_SUPPKEY) from lineorder_flat;
select yourfunction(LO_SUPPKEY) from lineorder_flat;
```

### 步骤 4：提交你的 PR，并且在你的 PR 评论中提交测试结果
