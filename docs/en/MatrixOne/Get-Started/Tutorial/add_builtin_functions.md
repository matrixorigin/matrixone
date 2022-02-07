
### A little primer for adding builtin functions

##### add unary functions:

In this guide, we use the function abs(get the absolute value) as an example.

Step 1: register function

MatrixOne doesn't distinguish between operators and functions. In our code repository, the file pkg/builtin/types.go register builtin functions as operators and we assign each operator a distinct integer number. To add a new function abs, add a new const Abs in the const declaration.

```go
const (
	Length = iota + overload.NE + 1
	Year
	Round
	Floor
	Abs
)
```

In the directory pkg/builtin/unary, create a new go file abs.go.

This abs.go file has the following functionalities:

 	1. function name registration
 	2. declare all the different parameter types this function accepts, and the different return type when given different parameter types.
 	3. the stringification method for this function.
 	4. preparation for function calling and function calling.



```go
package unary

func init() {

}

```

In Golang, init function will be called when a package is initialized. We wrap all abs.go's functionality inside this init function so we don't need to call it explicitly.

1. function name registration

```go
func init() {
	extend.FunctionRegistry["abs"] = builtin.Abs // register function name
}
```

In MatrixOne, all letters in a function name will be lowercased during the parsing process, so register function names using only lowercase letters otherwise the function won't be recognized.

2. declare function parameter types and return types.

The function abs accepts all numeric types as its parameter(uint8, int8, float32...), we can return a 64bit value covering all different parameter types, or, to optimize the performance of our function, we can return different types with respect to the parameter type.

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

In MatrixOne, We put all of our builtin function definition code in the vectorize directory, to implement abs functions, first we need to create a subdirectory abs in this vectorize directory.
In this fresh abs directory, create a file abs.go, the place where our abs function implementation code goes.
For certain cpu architectures, we could utilize the cpu's intrinsic SIMD instruction to compute the absolute value and hence boost our function's performance, to differentiate function implementations for different cpu architectures, we declare our pure go version of abs function this way:

```go
package abs

var (
	absFloat32 func([]float32, []float32) []float32
	absFloat64 func([]float64, []float64) []float64
)

func init() {
	absFloat32 = absFloat32Pure
	absFloat64 = absFloat64Pure
}

func AbsFloat32(xs, rs []float32) []float32 {
	return absFloat32(xs, rs)
}

func absFloat32Pure(xs, rs []float32) []float32 {
}

func AbsFloat64(xs, rs []float64) []float64 {
	return absFloat64(xs, rs)
}

func absFloat64Pure(xs, rs []float64) []float64 {
}
```

Inside the absFloat32Pure and absFloat64Pure, we implement our golang version of abs function for float32 and float64 type.

```go
func absFloat32Pure(xs, rs []float32) []float32 {
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

```go
func absFloat64Pure(xs, rs []float64) []float64 {
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

##### add binary and variadic functions for MatrixOne:

MatrixOne has some neat examples for adding binary and variadic functions(bitAnd function, floor function, etc. ), with some minor corresponding changes, the procedure is quite the same as the unary function.

​

Special thanks to [nnsgmsone](https://github.com/nnsgmsone)!

