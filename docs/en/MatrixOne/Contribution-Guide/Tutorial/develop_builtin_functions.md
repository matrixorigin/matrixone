# **Develop a built-in function**

## **Prerequisite**

To develop a built-in function for MatrixOne, you need a basic knowledge of Golang programming. You can go through this excellent [Golang tutorial](https://www.educative.io/blog/golang-tutorial) to learn some basic Golang concepts. 

## **Preparation**

Before you start, please make sure that you have `Go` installed, cloned the `MatrixOne` code base.
Please refer to [Preparation](../How-to-Contribute/preparation.md) and [Contribute Code](../How-to-Contribute/contribute-code.md) for more details.

## **What is a built-in function?**

There are two types of functions in a database,  built-in functions  and user-defined functions. Built-in functions are functions that are shipped with the database. In contrast, user-defined functions are customized by the users. Built-in functions can be categorized according to the data types that they operate on i.e. strings, date and numeric built-in functions. 

An example of a built-in function is `abs()`, which calculates the absolute (non-negative) value of a given number.

Some functions, such as `abs()` are used to perform calculations. Others such as `getdate()` are used to obtain a system value, such as the current data, or others, like `left()`, are used to manipulate textual data.

Usually the built-in functions are categorized into major categories:  

* Conversion Functions
* Logical Functions
* Math Functions
* String Functions
* Date Functions

## **Develop an abs() function:**

In this tutorial, we use the function ABS (get the absolute value) as an example.

Step 1: register function

MatrixOne doesn't distinguish between operators and functions. In our code repository, the file `pkg/builtin/types.go` register builtin functions as operators and we assign each operator a distinct integer number. To add a new function `abs()`, add a new const Abs in the const declaration.

```go
const (
	Length = iota + overload.NE + 1
	Year
	Round
	Floor
	Abs
)
```

In the directory `pkg/builtin/unary`, create a new go file `abs.go`.

!!! info 
    The functions under `unary` directory take only one value as input. The functions under `binary` directory take only two values as input. Other forms of functions are put under `multi` directory.

This `abs.go` file has the following functionalities:

 	1. function name registration
 	2. declare all the different parameter types this function accepts, and the different return type when given different parameter types.
 	3. the stringification method for this function.
 	4. preparation for function calling and function calling.

```go
package unary

func init() {

}

```

In Golang, init function will be called when a package is initialized. We wrap all `ABS()`'s functionality inside this init function so we don't need to call it explicitly.

**1.** function name registration

```go
func init() {
	extend.FunctionRegistry["abs"] = builtin.Abs // register function name
}
```

In MatrixOne, all letters in a function name will be lowercased during the parsing process, so the register function names using only lowercase letters otherwise the function won't be recognized.

**2.** declare function parameter types and return types.

The function abs accepts all numeric types as its parameter (uint8, int8, float32...), we can return a 64bit value covering all different parameter types. To optimize the performance of our function, we can also return different types with respect to the parameter type.

Outside the init function, declare these variables for each pair of parameter type and return type.

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

Register parameter types and return types for abs function:

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

Define a stringify function and register abs function type:

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

For simplicity, we demonstrate only two cases where abs function has parameter type float32 and float64.

Preparation for function calling:

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

some annotations for this code snippet above:

1.process.Get: MatrixOne assigns each query a "virtual process", during the execution of a query, we may need to generate new Vector, allocate memory for it, and we do it using this Get function

```go
// proc: the process for this query, size: the memory allocation size  we are asking for, type: the new Vector's type.
func Get(proc *Process, size int64, typ types.Type) (*vector.Vector, error)
```

since we need a float32 vector here, its size should be 4 * len(origVecCol), 4 bytes for each float32.

2.encoding.DecodeFloat32Slice: this is just type casting. 

3.Vector.Nsp: MatrixOne uses bitmaps to store the NULL values in a column, Vector.Nsp is a wrap up struct for this bitmap.

4.the boolean parameter of the Fn: this boolean value is usually used to indicate whether the vector passed in is a constant(it has length 1). Sometimes we could make use of this situation for our function implementation, for example, pkg/sql/colexec/extend/overload/plus.go. 

Since the result vector has the same type as the original vector, we could use the original vector to store our result when we don't need our original vector anymore in our execution plan(i.e., the reference count of the original vector is 0 or 1).

To reuse the original vector when possible:

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

For float64 type parameter:

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

Step 2: Implement Abs function

In MatrixOne, We put all of our builtin function definition code in the `pkg/vectorize/` directory. To implement abs functions, first we need to create a subdirectory `abs` in this vectorized directory.
In this fresh `abs` directory, create a file `abs.go`, the place where our abs function implementation code goes.
For certain cpu architectures, we could utilize the cpu's intrinsic SIMD instruction to compute the absolute value and hence boost our function's performance, to differentiate function implementations for different cpu architectures, we declare our pure go version of abs function this way:

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

Inside the absFloat32 and absFloat64, we implement our golang version of abs function for float32 and float64 type. The other data types (int8, int16, int32, int64) are more or less the same. 

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

Here we go. Now we can fire up MatrixOne and take our abs function for a little spin.

## **Compile and run MatrixOne**

Once the function is ready, we could compile and run MatrixOne to see the function behavior. 

Step1: Run `make config` and `make build` to compile the MatrixOne project and build binary file. 

```
make config
make build
``` 

!!! info 
    `make config` generates a new configuration file, in this tutorial, you only need to run it once. If you modify some code and want to recompile, you only have to run `make build`.  

Step2: Run `./mo-server system_vars_config.toml` to launch MatrixOne, the MatrixOne server will start to listen for client connecting. 

```
./mo-server system_vars_config.toml
```

!!! info 
	The logger print level of `system_vars_config.toml` is set to default as `DEBUG`, which will print a lot of information for you. If you only care about what your built-in function will print, you can modify the `system_vars_config.toml` and set `cubeLogLevel` and `level` to `ERROR` level.
	
	cubeLogLevel = "error"
	
	level = "error"

!!! info 
	Sometimes a `port is in use` error at port 50000 will occur. You could checkout what process in occupying port 50000 by `lsof -i:50000`. This command helps you to get the PIDNAME of this process, then you can kill the process by `kill -9 PIDNAME`.

Step3: Connect to MatrixOne server with a MySQL client. Use the built-in test account for example:

user: dump
password: 111

```
mysql -h 127.0.0.1 -P 6001 -udump -p
Enter password:
```

Step4: Test your function behavior with some data. Below is an example. 

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
    Except for `abs()`, MatrixOne has already some neat examples for built-in functions, such as `floor()`, `round()`, `year()`. With some minor corresponding changes, the procedure is quite the same as other functions.
​

## **Write a unit Test for your function**

We recommend that you also write a unit test for the new function. 
Go has a built-in testing command called `go test` and a package `testing` which combine to give a minimal but complete testing experience. It automates execution of any function of the form.

```
func TestXxx(*testing.T)
```

To write a new test suite, create a file whose name ends `_test.go` and contains the `TestXxx` functions as described here. Put the file in the same package as the one being tested. The file will be excluded from regular package builds but will be included when the `go test` command is run. 

Step1: Create a file named `abs_test.go` under `vectorize/abs/` directory. Import the `testing` framework and the `testify` framework we are going to use for testing mathematical `equal`. 

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

Step2: Implement the `TestXxx` functions with some predefined values.

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

Step3: Launch Test.
Within the same directory as the test:

```
go test
```

This picks up any files matching packagename_test.go.
If you are getting a `PASS`, you are passing the unit test. 

In MatrixOne, we have a `bvt` test framework which will run all the unit tests defined in the whole package, and each time your code is merged in the code base, the test will automatically run. 
