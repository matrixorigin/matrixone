# **Develop an aggregate function**

## **Prerequisite**

To develop an aggregate function for MatrixOne, you need a basic knowledge of Golang programming. You can go through this excellent [Golang tutorial](https://www.educative.io/blog/golang-tutorial) to learn some basic Golang concepts. 

## **Preparation**

Before you start, please make sure that you have `Go` installed and have cloned the `MatrixOne` code base.
Please refer to [Preparation](../How-to-Contribute/preparation.md) and [Contribute Code](../How-to-Contribute/contribute-code.md) for more details.

## **What is an aggregation function?**

In database systems, an aggregate function or aggregation function is a function where the values of multiple rows are grouped together to form a single summary value.

Common aggregate functions include:

* `COUNT` counts how many rows are in a particular column.
* `SUM` adds together all the values in a particular column.
* `MIN` and `MAX` return the lowest and highest values in a particular column, respectively.
* `AVG` calculates the average of a group of selected values.

## **Aggregate function in MatrixOne**

The function `join` in MatrixOne's database is highly efficient and less redundant in comparison with
other state-of-the-art databases via factorisation making it a key feature. Therefore, many operations in MatrixOne need to be adapted to the factorisation method, in order
to improve efficiency when performing `join`. Aggregate functions are an example of an important feature among these operations. 

To implement aggragate functions in MatrixOne, we design a data structure named `Ring`. Every aggregate function needs to implement the `Ring` interface
in order to be factorized when `join` occurs. 

For the common aggregate function `AVG` as an example, we need to calculate the number of groups and their total numeric sum, then get an average result.
This is common practice for any database design. However, when a query with `join` occurs between two tables, the common method is to get a Cartesian product by joining tables first,
then perform an `AVG` with that Cartesian product. This is an expensive computational cost as a Cartesian product can be very large. 
In MatrixOne's implementation, the factorisation method pushs down the calculation of group statistics and sum before the `join` operation is performed. This method helps to reduce
a lot in computational and storage cost. Factorisation 
is realized by the `Ring` interface and its inner functions. 

To checkout more about the factorisation theory and factorised database, please refer to [Principles of Factorised Databases](https://fdbresearch.github.io/principles.html).

## **What is a `Ring`**

`Ring` is an important data structure for MatrixOne factorisation, as well as a mathematical algebraic concept with a clear [definition](https://en.wikipedia.org/wiki/Ring_(mathematics)).
An algebraic `Ring` is a set equipped with two binary operations  `+` (addition) and `⋅` (multiplication) satisfying several axioms.

A `Ring` in MatrixOne is an interface with several functions similar to the algebraic `Ring` structure. We use `Ring` interface to implement aggragate functions.
The `+`(addition) is defined as merging two `Ring`s groups and the `⋅`(multiplication) operation is defined as the computation of a grouped aggregate value combined with its grouping key frequency information.

| Method of Ring Interface | Do What                                                |
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

The implementation of `Ring` data structure is under `/pkg/container/ring/`. 

## **How does `Ring` work with query:**

To better understand the `Ring` interface, we can take aggregate function `sum()` as an example. We'll walk you through the whole process
of `Ring`.

There are two different scenarios for aggregation functions with `Ring`s.

*1. Query with single table.*

In the single table scenario, when we run the below query, it generates one or several `Ring`s, 
depending on the storage blocks the `T1` table is stored. The number of blocks depends on the storage strategy. Each `Ring` will 
store several groups of sums. The number of the group depends on how many duplicate rows are in this `Ring`. 

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

For example, if two `Ring`s were generated, the first `Ring` holds the group sums of the first 4 rows, which will be like:

```sql
|  one   |  23+20+22 |
|  two   |  20       |
```

The second `Ring` holds the group sums of the last 6 rows, which will be like:

```sql
|  two   |  19       |
|  three |  18+20+21+24+19 |
```

Then the `Add` method of `Ring` will be called to merge two groups together, and at last the `Eval` method will return the overall result to user.

```sql
|  one   |  23+20+22       |
|  two   |  20+19       |
|  three |  18+20+21+24+19 |
```

*2. Query with joining multiple tables.*

In the multiple tables join scenario, we have two tables `Tc` and `Ts`. The query looks like the following.

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

When we run this query, it will firstly generate `Ring`s for the `Ts` table since we are performing an aggeration over the `age` column.
It might also generate one or several `Ring`s same as the single table. For simplicity, we imagine only one `Ring` is created for each table.
The `Ring-Ts` will start to count sums for the group of `id` as all `id`s are different, so it will maintain the same. Then a hashtable will be created
for performing `join` operation.

The `Ring-Tc` is created in the same time as `join` is performed. This `Ring-Tc` will count the appearing frequency `f` of id. Then the `Mul` method
of `Ring-Ts` is called to calculate the sum calculated from the `Ring-Ts` and frequency from `Ring-Tc`.

```
sum[i] = sum[i] * f[i]
```

Now we get values of `[class, sum(age)]`, then perform a group by with class which will give us the final result.

## **The secret of factorisation**

From the above example, you can see that the `Ring` performs some pre calculations and only the result(like `sum`) is stored in its structure. When performing operations like `join`,
only simple `Add` or `Multiplication` is needed to get the result, which is called a push down calculation in `factorisation`. With the help of this push down, we no longer need to 
deal with a costly Cartesian product. As the joined table number increases, the factorisation allows us to take linear cost of performing that instead of exponential increase.

Take the implementation of `Variance` function as an example. 

The variance formula is as below:

```
Variance = Σ [(xi - x̅)^2]/n

Example: xi = 10,8,6,12,14, x̅ = 10
Calculation: ((10-10)^2+(8-10)^2+(6-10)^2+(12-10)^2+(14-10)^2)/5 = 8
``` 

If we proceed with implementing this formula, we have to record all values of each group, and also maintain these values 
with `Add` and `Mul` operations of `Ring`. Eventually the result is calculated in an `Eval()` function. This implementation has a drawback of high memory cost since we have to store all the values during processing.

In the `Avg` implementation, it doesn't store all values in the `Ring`. Instead it stores only the `sum` of each group and the null numbers.  It returns the final result with a simple division. This method saves a lot of memory space.

Now let's turn the `Variance` formula a bit into a different form:

```
Variance = Σ (xi^2)/n-x̅^2

Example: xi = 10,8,6,12,14, x̅ = 10
Calculation: (10^2+8^2+6^2+12^2+14^2)/5-10^2 = 8
``` 

This formula's result is exactly the same as the previous one, but we only have to record the values sum of `xi^2` and the sum of `xi`. We can largely reduce the memory space 
with this kind of reformulation.

To conclude, every aggregate function needs to find a way to record as little values as possible in order to reduce memory cost.
Below are two different implementations for `Variance` (the second one has a better performance):

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

## **Develop an var() function**

In this tutorial, we will walk you through the complete implementation of the Variance (get the standard overall variance value) aggregate function as an example with two different methods. 

Step 1: register function

MatrixOne doesn't distinguish between operators and functions. 
In our code repository, the file `pkg/sql/viewexec/transformer/types.go` registers aggregate functions as operators 
and we assign each operator a distinct integer number. 
To add a new function `var()`, first add a new const `Variance` in the const declaration and `var` in the name declaration.

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

Step2: implement the `Ring` interface

*1. Define `Ring` structure*

Create `variance.go` under `pkg/container/ring`, and define a structure of `VarRing`.

As we calculate the overall variance, we need to calculate:

* The numeric `Sums` and the null value numbers of each group, to calculate the average.
* Values of each group, to calculate the variance. 

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

*2. Implement the functions of `Ring` interface*

You can checkout the full implmetation at [variance.go](https://github.com/matrixorigin/matrixone/blob/main/pkg/container/ring/variance/variance.go).

* `Fill` function

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

* `Add` function   

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

* `Mul` function   

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

* `Eval` function  

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

*3. Implement encoding and decoding for `VarRing`*

In the `pkg/sql/protocol/protocol.go` file, implement the code for serialization and deserialization of `VarRing`. 

   | Serialization function | Deserialization function    |
   | ------------------ | ----------------------------------- |
   | EncodeRing         | DecodeRing<br>DecodeRingWithProcess |

Serialization: 

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

Deserialization:

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

Here we go. Now we can fire up MatrixOne and try with our `var()` function.

## **Compile and run MatrixOne**

Once the aggregation function is ready, we can compile and run MatrixOne to see the function behavior. 

Step1: Run `make config` and `make build` to compile the MatrixOne project and build binary file. 

```
make config
make build
``` 

!!! info 
    `make config` generates a new configuration file. In this tutorial you only need to run it once. If you modify some code and want to recompile, you only have to run `make build`.  

Step2: Run `./mo-server system_vars_config.toml` to launch MatrixOne, the MatrixOne server will start to listen for client connecting. 

```
./mo-server system_vars_config.toml
```

!!! info 
	The logger print level of `system_vars_config.toml` is set to default as `DEBUG`, which will print a lot of information for you. If you only care about what your function will print, you can modify the `system_vars_config.toml` and set `cubeLogLevel` and `level` to `ERROR` level.
	
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

Step4: Test your function behavior with some data. Below is an example. You can check if you get the right mathematical variance result. 
You can also try an `inner join` and check the result, if the result is correct, the factorisation is valid. 

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

Bingo!

!!! info 
    Except for `var()`, MatrixOne has already some neat examples for aggregate functions, such as `sum()`, `count()`, `max()`, `min()` and `avg()`. With some minor corresponding changes, the procedure is quite the same as other functions.
​

## **Write unit Test for your function**

We recommend that you also write a unit test for the new function. 
Go has a built-in testing command called `go test` and a package `testing` which combine to give a minimal yet complete testing experience. It automates execution of any function of the form.

```
func TestXxx(*testing.T)
```

To write a new test suite, create a file whose name ends `_test.go` that contains the `TestXxx` functions as described here. Put the file in the same package as the one being tested. The file will be excluded from regular package builds but will be included when the `go test` command is run. 

Step1: Create a file named `variance_test.go` under `pkg/container/ring/variance/` directory. 
Import the `testing` framework and the `reflect` framework we are going to use for testing. 

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

Step2: Implement the `TestVariance` function with some predefined values.

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

Step3: Complete the unit test for Serialization and Deserialization in the function `TestRing` in the file `pkg/sql/protocol/protocol_test.go`.
You can check the complete test code of `VarRing` there.

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

Step4: Launch Test.

Within the same directory as the test:

```
go test
```

This picks up any files matching packagename_test.go.
If you are getting a `PASS`, you are passing the unit test. 

In MatrixOne, we have a `bvt` test framework which will run all the unit tests defined in the whole package, and each time your make a pull request to the code base, the test will automatically run.
You code will be merged only if the `bvt` test pass.

## **Conduct a Performance Test**

Aggregate functions are an important feature of a database system, with queries on hundreds of millions of data rows, the time consumption of an aggregate function is quite significant.
Thus, we recommend you to run a performance test. 

Step1: Download the standard test dataset.

We have prepared a single table SSB query dataset with 10 million rows of data. The raw data file size is about 4GB, 500MB after being zipped.  You can get the data files directly:

```
https://community-shared-data-1308875761.cos.ap-beijing.myqcloud.com/lineorder_flat.tar.bz2
```

Step2: Unzip the file and Load the data into MatrixOne. 

With the following SQL you can create the database and table, and load the `lineorder_flat.tbl` into MatrixOne.

```sql
create database if not exists ssb;
use ssb;
drop table if exists lineorder_flat;
CREATE TABLE lineorder_flat(
  LO_ORDERKEY bigint primary key,
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

If you load successfully this dataset, you are normally getting a result as:

```
Query OK, 10272594 rows affected (1 min 7.09 sec)
```

Step3: Run your aggregate function and `sum()`, `avg()` on the column `LO_SUPPKEY` respectively to check the performance.

```sql
select avg(LO_SUPPKEY) from lineorder_flat;
select sum(LO_SUPPKEY) from lineorder_flat;
select yourfunction(LO_SUPPKEY) from lineorder_flat;
```

Step4: When you submit your PR, please submit these performance results in your PR comment as well. 
