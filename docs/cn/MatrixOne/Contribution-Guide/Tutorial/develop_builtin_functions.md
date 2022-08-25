# **开发系统函数**

## **前提条件**

为 MatrixOne 开发系统函数，你需要具备 Golang 编程的基本知识。你可以通过[Golang 教程](https://www.educative.io/blog/golang-tutorial)来学习一些基本的 Golang 概念。

## **开始前准备**

在你开始之前，请确保你已经安装了 Golang，并将 MatrixOne 代码库克隆到你的本地。

更多信息，参见[准备工作](../How-to-Contribute/preparation.md) 和[代码贡献](../How-to-Contribute/contribute-code.md)。

## **什么是系统函数**

数据库中通常有两种类型的函数，即内置函数（也就是本篇文章介绍的系统函数，以下皆称为系统函数）和用户定义函数。系统函数是数据库附带的函数。而用户自定义函数则由用户自定义。系统函数可以根据其操作的数据类型进行分类，如字符串、日期和数学类函数。

本篇文章介绍的示例 `abs()` 也属于系统函数，它用于执行计算，它计算给定数字的绝对(非负)值。

通常系统函数可以分为以下几类：

* 转换功能

* 逻辑函数

* 数学函数

* 字符串函数

* 日期函数

## **开发 `abs()` 函数**

在本教程中，将以 `abs()` 函数为例指导你完成开发流程。

### 步骤 1：注册函数

**步骤介绍**：

MatrixOne 不区分运算符和函数。

在代码存储库中，文件 `pkg/builtin/types.go` 将系统函数注册为操作符，并为每个操作符赋值一个不同的整数。

新增一个新函数 `abs()`，首先在常量声明中添加一个新的 `abs`。

```go
const (
	Length = iota + overload.NE + 1
	Year
	Round
	Floor
	Abs
)
```

在 `pkg/builtin/unary` 目录下新建一个名为 `abs.go` 的文件.

!!! info
    `unary` 目录下的函数只能输入一个值。`binary` 目录下的函数可以输入两个值。`multi` 目录下存放其他类型的函数。

`abs.go` 文件有以下功能：

- 函数名注册

- 声明此函数接受的所有不同形参类型，以及给定不同形参类型时的不同返回类型。

- 此函数的字符串化方法。

- 函数调用和函数调用的准备工作。

```go
package unary

func init() {

}

```

**(1)在 Golang 中，初始化包时会调用 `init` 函数。所有 `ABS()` 函数都包含在这个 `init` 函数中，因此无需显式调用它。**

#### 函数名注册

1. 注册函数名，参见下面的代码：

   ```go
   func init() {
   	extend.FunctionRegistry["abs"] = builtin.Abs // register function name
   }
   ```

   !!! note
       在 MatrixOn e中，函数名中的所有字母在解析过程中都会小写，所以注册函数名只能使用小写字母，否则函数将无法识别。

2. 声明函数参数类型和返回类型。

   所有数字类型都可以作为函数 `abs()` 的参数（uint8、int8、float32...）。那么，可以设置返回一个涵盖所有不同参数类型的 64 位值。为了优化函数的性能，你也可以根据参数类型返回不同的类型。

   a. 在 `init` 函数之外，需要为每对参数类型和返回类型声明这些变量。

```go
var argAndRets = []argsAndRet{
	{[]types.T{types.T_uint8}, types.T_uint8},
	{[]types.T{types.T_uint16}, types.T_uint16},
	{[]types.T{types.T_uint32}, types.T_uint32},
	{[]types.T{types.T_uint64}, types.T_uint64},
	{[]types.T{types.T_int8}, types.T_int8},
	{[]types.T{types.T_int16}, types.T_int16},
	{[]types.T{types.T_int32}, types.T_int32},
	{[]types.T{types.T_int64}, types.T_int64},
	{[]types.T{types.T_float32}, types.T_float32},
	{[]types.T{types.T_float64}, types.T_float64},
}

func init() {
	extend.FunctionRegistry["abs"] = builtin.Abs // register function name
}
```

   b. 为 `abs()` 函数注册参数类型和返回类型：

```go
func init() {
	extend.FunctionRegistry["abs"] = builtin.Abs // register function name
	for _, item := range argAndRets {
		overload.AppendFunctionRets(builtin.Abs, item.args, item.ret) // append function parameter types and return types
	}
	extend.UnaryReturnTypes[builtin.Abs] = func(extend extend.Extend) types.T { // define a get return type function for abs function
		return getUnaryReturnType(builtin.Abs, extend)
	}
}
```

   c. 定义一个 `stringify` 函数并注册 `abs()` 函数类型：

```go
func init() {
	extend.FunctionRegistry["abs"] = builtin.Abs // register function name
	for _, item := range argAndRets {
		overload.AppendFunctionRets(builtin.Abs, item.args, item.ret) // append function parameter types and return types
	}
	extend.UnaryReturnTypes[builtin.Abs] = func(extend extend.Extend) types.T { // define a get return type function for abs function
		return getUnaryReturnType(builtin.Abs, extend)
	}
	extend.UnaryStrings[builtin.Abs] = func(e extend.Extend) string { // define a stringify function for abs
		return fmt.Sprintf("abs(%s)", e)
	}
	overload.OpTypes[builtin.Abs] = overload.Unary // register abs function type
}
```

   为更简单的展示示例，本章节仅演示 `abs()` 函数的参数类型为 `float32` 和 `float64` 的两种情况。

   d. 函数调用准备：

```go
func init() {
	extend.FunctionRegistry["abs"] = builtin.Abs // register function name
	for _, item := range argAndRets {
		overload.AppendFunctionRets(builtin.Abs, item.args, item.ret) // append function parameter types and return types
	}
	extend.UnaryReturnTypes[builtin.Abs] = func(extend extend.Extend) types.T { // define a get return type function for abs function
		return getUnaryReturnType(builtin.Abs, extend)
	}
	extend.UnaryStrings[builtin.Abs] = func(e extend.Extend) string { // define a stringify function for abs
		return fmt.Sprintf("abs(%s)", e)
	}
	overload.OpTypes[builtin.Abs] = overload.Unary // register abs function type
	overload.UnaryOps[builtin.Abs] = []*overload.UnaryOp{
		{
			Typ:        types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float32)
				resultVector, err := process.Get(proc, 4*int64(len(origVecCol)), types.Type{Oid: types.T_float32, Size: 4}) // get a new types.T_float32 vector to store the result vector
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat32Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)                         // the new vector's nulls are the same as the original vector
				vector.SetCol(resultVector, abs.AbsFloat32(origVecCol, results)) // set the vector col with the return value from abs.AbsFloat32 function
				return resultVector, nil
			},
		},
	}
}

```

上面的代码中的关键注释如下：

1. process.Get：MatrixOne 为每个查询分配一个“虚拟进程”，在执行查询期间，我需要生成新的 Vector，并为它分配内存，参见下面的命令：

```go
// proc: the process for this query, size: the memory allocation size  we are asking for, type: the new Vector's type.
func Get(proc *Process, size int64, typ types.Type) (*vector.Vector, error)
```

   `abs()` 函数的参数类型为 `float32`，它的大小应该是 4*len(origVecCol)，每个 `float32` 对应4个字节。

2. encoding.DecodeFloat32Slice：对参数类型做类型转换。

3. Vector.Nsp：MatrixOne 使用 bigmap 将 NULL 值存储在列中，*Vector.Nsp* 是 bigmap 的包装结构。

4. the boolean parameter of the Fn：这个布尔值通常表示传入的向量是否为常量(长度为1)。有时我们可以在函数实现中使用这种情况，例如，*pkg/sql/colexec/extend/overload/plus.go*。

由于结果向量与原始向量具有相同的类型，当你在执行计划中不再需要原始向量时，那么可以使用原始向量来存储结果（即原始向量的引用计数为0或1)。

重用原始向量的情况如下：

```go
func init() {
	extend.FunctionRegistry["abs"] = builtin.Abs // register function name
	for _, item := range argAndRets {
		overload.AppendFunctionRets(builtin.Abs, item.args, item.ret) // append function parameter types and return types
	}
	extend.UnaryReturnTypes[builtin.Abs] = func(extend extend.Extend) types.T { // define a get return type function for abs function
		return getUnaryReturnType(builtin.Abs, extend)
	}
	extend.UnaryStrings[builtin.Abs] = func(e extend.Extend) string { // define a stringify function for abs
		return fmt.Sprintf("abs(%s)", e)
	}
	overload.OpTypes[builtin.Abs] = overload.Unary // register abs function type
	overload.UnaryOps[builtin.Abs] = []*overload.UnaryOp{
		{
			Typ:        types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float32)
				if origVec.Ref == 1 || origVec.Ref == 0 { // reuse the original vector when we don't need the original one anymore
					origVec.Ref = 0
					abs.AbsFloat32(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 4*int64(len(origVecCol)), types.Type{Oid: types.T_float32, Size: 4}) // get a new types.T_float32 vector to store the result vector
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat32Slice(resultVector.Data)				// decode the vector's data to float32 type
				results = results[:len(origVecCol)]													
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)                         // the new vector's nulls are the same as the original vector
				vector.SetCol(resultVector, abs.AbsFloat32(origVecCol, results)) // set the vector col with the return value from abs.AbsFloat32 function
				return resultVector, nil
			},
		},
	}
}

```

 `float64` 类型参数代码示例如下：

```go
func init() {
	extend.FunctionRegistry["abs"] = builtin.Abs // register function name
	for _, item := range argAndRets {
		overload.AppendFunctionRets(builtin.Abs, item.args, item.ret) // append function parameter types and return types
	}
	extend.UnaryReturnTypes[builtin.Abs] = func(extend extend.Extend) types.T { // define a get return type function for abs function
		return getUnaryReturnType(builtin.Abs, extend)
	}
	extend.UnaryStrings[builtin.Abs] = func(e extend.Extend) string { // define a stringify function for abs
		return fmt.Sprintf("abs(%s)", e)
	}
	overload.OpTypes[builtin.Abs] = overload.Unary // register abs function type
	overload.UnaryOps[builtin.Abs] = []*overload.UnaryOp{
		{
			Typ:        types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float32)
				if origVec.Ref == 1 || origVec.Ref == 0 { // reuse the original vector when we don't need the original one anymore
					origVec.Ref = 0
					abs.AbsFloat32(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 4*int64(len(origVecCol)), types.Type{Oid: types.T_float32, Size: 4}) // get a new types.T_float32 vector to store the result vector
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat32Slice(resultVector.Data)   // decode the vector's data to float32 type
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)                         // the new vector's nulls are the same as the original vector
				vector.SetCol(resultVector, abs.AbsFloat32(origVecCol, results)) // set the vector col with the return value from abs.AbsFloat32 function
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float64)
				if origVec.Ref == 1 || origVec.Ref == 0 {
					origVec.Ref = 0
					abs.AbsFloat64(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, abs.AbsFloat64(origVecCol, results))
				return resultVector, nil
			},
		},
	}
}
```

### 步骤 2：实现 `abs` 函数

在 MatrixOne 中，所有内置函数定义代码都放在 *pkg/vectorize/* 目录中。实现 `abs` 函数，首先你需要在 *pkg/vectorize/* 目录中创建一个子目录 *abs*，然后在子目录 *abs* 中新建一个名为 *abs.go* 的文件，你可以在 *abs.go* 的文件文件中编写 `abs` 函数代码。

基于特定的 CPU 架构，你可以利用 CPU 内部的 `SIMD` 指令来计算绝对值，从而提高函数的性能；在不同 CPU 架构中实现 `abs` 函数，仍然需要做出区分。参考下面的代码示例，声明 `abs` 函数的基于 `golang` 语言版本：

```go
package abs

var (
	AbsFloat32 func([]float32, []float32) []float32
	AbsFloat64 func([]float64, []float64) []float64
)

func init() {
	AbsFloat32 = absFloat32
	AbsFloat64 = absFloat64
}

func absFloat32(xs, rs []float32) []float32 {
	// See below
}

func absFloat64(xs, rs []float64) []float64 {
	// See below
}
```

当前 MatrixOne 已经在 `absFloat32` 和 `absFloat64` 内部实现了针对 `float32` 和 `float64` 类型的 Golang 版本。其他数据类型(例如，int8、int16、int32、int64)实现方式大致相同，参见以下代码示例：

```go
func absFloat32(xs, rs []float32) []float32 {
   for i := range xs {
      if xs[i] < 0 {
         rs[i] = -xs[i]
      } else {
         rs[i] = xs[i]
      }
   }
   return rs
}

func absFloat64(xs, rs []float64) []float64 {
   for i := range xs {
      if xs[i] < 0 {
         rs[i] = -xs[i]
      } else {
         rs[i] = xs[i]
      }
   }
   return rs
}
```

现在我们可以启动 MatrixOne 并尝试使用 `abs()` 函数了。

## **编译并运行 MatrixOne**

本章节讲述编译并运行 MatrixOne 来查看函数的行为。
Once the aggregation function is ready, we can compile and run MatrixOne to see the function behavior.

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

示例如下：

```sql
mysql> create table abs_test_table(a float, b double);
Query OK, 0 rows affected (0.44 sec)

mysql> insert into abs_test_table values(12.34, -43.21);
Query OK, 1 row affected (0.08 sec)

mysql> insert into abs_test_table values(-12.34, 43.21);
Query OK, 1 row affected (0.02 sec)

mysql> insert into abs_test_table values(2.718, -3.14);
Query OK, 1 row affected (0.02 sec)

mysql> select a, b, abs(a), abs(b) from abs_test_table;
+----------+----------+---------+---------+
| a        | b        | abs(a)  | abs(b)  |
+----------+----------+---------+---------+
|  12.3400 | -43.2100 | 12.3400 | 43.2100 |
| -12.3400 |  43.2100 | 12.3400 | 43.2100 |
|   2.7180 |  -3.1400 |  2.7180 |  3.1400 |
+----------+----------+---------+---------+
3 rows in set (0.01 sec)
```

Bingo!

!!! info
		除了 `abs()` 之外，MatrixOne 已经有一些系统函数的简洁示例，例如 `floor()`、`round()`、`year()`。稍作相应改动后，过程与其他功能基本相同。

## **为你的函数编写测试单元**

本章节将指导你为新函数编写一个单元测试。

Go 有一个名为 `go test` 的内置测试命令和一个名为 *testing* 的测试包，它们结合起来可以提供最小但完整的测试体验。它自动执行表单的任何函数。

```
func TestXxx(*testing.T)
```

编写一个新的测试套件，先创建一个以 *_test.go* 结尾的文件，命名需要描述函数名称，例如 *variance_test.go*；将 *variance_test.go* 文件与测试文件放在同一个包中，打包构建时，不包含 *variance_test.go* 文件，但在运行 `go test` 命令时会包含 *variance_test.go* 文件，详细步骤，参见下述步骤。

### 步骤 1：在 `vectorize/abs/` 目录下新建一个名为 `abs_test.go` 的文件，并导入用于测试的 `testing` 框架和 `testify` 框架，测试数学上的 `equal`

```
package abs

import (
    "testing"
    "github.com/stretchr/testify/require"
)

function TestAbsFloat32(t *testing.T) {

}

function TestAbsFloat64(t *testing.T) {

}
```

### 步骤 2：通过一些预定义值执行 `TestAbs` 函数

```
func TestAbsFloat32(t *testing.T) {
    //Test values
    nums := []float32{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, -1.2, -12.3, -123.4, -1234.5, -12345.6}
    //Predefined Correct Values
    absNums := []float32{1.5, 1.5, 2.5, 2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, 1.2, 12.3, 123.4, 1234.5, 12345.6}

    //Init a new variable
    newNums := make([]float32, len(nums))
    //Run abs function
    newNums = AbsFloat32(nums, newNums)

    for i := range newNums {
        require.Equal(t, absNums[i], newNums[i])
    }
}

func TestAbsFloat64(t *testing.T) {
    //Test values
    nums := []float64{1.5, -1.5, 2.5, -2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, -1.2, -12.3, -123.4, -1234.5, -12345.6}
    //Predefined Correct Values
    absNums := []float64{1.5, 1.5, 2.5, 2.5, 1.2, 12.3, 123.4, 1234.5, 12345.6, 1234.567, 1.2, 12.3, 123.4, 1234.5, 12345.6}

    //Init a new variable
    newNums := make([]float64, len(nums))
    //Run abs function
    newNums = AbsFloat64(nums, newNums)

    for i := range newNums {
        require.Equal(t, absNums[i], newNums[i])
    }
}
```

### 步骤 3：启动测试

1. 在与步骤 2 同一个目录下进行测试：

   ```
   go test
   ```

	 这条命令会选取任何与 *packagename_test.go* 匹配的文件，在本示例中，执行 `go test` 将会选取测试 *abs_test.go* 文件。

2. 运行结果为 `PASS`，表示通过了单元测试。

	 在 MatrixOne 中，我们有一个 `bvt` 测试框架，它将运行整个包中定义的所有单元测试，并且每次你向代码库发出拉取请求时，测试将自动运行。

3. `bvt` 测试通过，你的代码将被合并。
