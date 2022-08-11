// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compare

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestI32Eq(t *testing.T) {
	as := make([]int32, 10)
	bs := make([]int32, 10)
	for i := 0; i < 10; i++ {
		as[i] = 4
		bs[i] = int32(i - 3)
	}
	cs := make([]bool, 10)
	av := testutil.MakeInt32Vector(as, nil)
	bv := testutil.MakeInt32Vector(bs, nil)
	cv := testutil.MakeBoolVector(cs)

	err := NumericEqual[int32](av, bv, cv)
	if err != nil {
		t.Fatal(err)
		t.Fatalf("should not error.")
	}

	res := vector.MustTCols[bool](cv)
	for i := 0; i < 10; i++ {
		fmt.Printf("%+v == %+v \n", as[i], bs[i])
		fmt.Printf("actual res:%+v\n", res[i])
		fmt.Printf("expect res:%+v\n", as[i] == bs[i])
		if res[i] != (as[i] == bs[i]) {
			t.Fatalf("int32 equal wrong result")
		}
	}
}

func TestU32Eq(t *testing.T) {
	as := make([]uint32, 10)
	bs := make([]uint32, 10)
	for i := 0; i < 10; i++ {
		as[i] = 8
		bs[i] = uint32(i + 3)
	}
	cs := make([]bool, 10)
	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeBoolVector(cs)

	err := NumericEqual[uint32](av, bv, cv)
	if err != nil {
		t.Fatal(err)
		t.Fatalf("should not error.")
	}

	res := vector.MustTCols[bool](cv)
	for i := 0; i < 10; i++ {
		fmt.Printf("%+v == %+v \n", as[i], bs[i])
		fmt.Printf("actual res:%+v\n", res[i])
		fmt.Printf("expect res:%+v\n", as[i] == bs[i])
		if res[i] != (as[i] == bs[i]) {
			t.Fatalf("uint32 equal wrong result")
		}
	}
}

func TestF32Eq(t *testing.T) {
	as := make([]float32, 2)
	bs := make([]float32, 2)
	for i := 0; i < 2; i++ {
		as[i] = 2.5
		bs[i] = float32(i) + 1.5
	}
	cs := make([]bool, 2)
	av := testutil.MakeFloat32Vector(as, nil)
	bv := testutil.MakeFloat32Vector(bs, nil)
	cv := testutil.MakeBoolVector(cs)

	err := NumericEqual[float32](av, bv, cv)
	if err != nil {
		t.Fatalf("should not error.")
	}

	res := vector.MustTCols[bool](cv)
	for i := 0; i < 2; i++ {
		//fmt.Printf("%+v == %+v \n", as[i], bs[i])
		//fmt.Printf("actual res:%+v\n", res[i])
		//fmt.Printf("expect res:%+v\n", as[i] == bs[i])
		if res[i] != (as[i] == bs[i]) {
			t.Fatalf("float32 equal wrong result")
		}
	}
}

func TestF64Eq(t *testing.T) {
	as := make([]float64, 2)
	bs := make([]float64, 2)
	for i := 0; i < 2; i++ {
		as[i] = 2.5
		bs[i] = float64(i) + 1.5
	}
	cs := make([]bool, 2)
	av := testutil.MakeFloat64Vector(as, nil)
	bv := testutil.MakeFloat64Vector(bs, nil)
	cv := testutil.MakeBoolVector(cs)

	err := NumericEqual[float64](av, bv, cv)
	if err != nil {
		t.Fatalf("should not error.")
	}

	res := vector.MustTCols[bool](cv)
	for i := 0; i < 2; i++ {
		//fmt.Printf("%+v == %+v \n", as[i], bs[i])
		//fmt.Printf("actual res:%+v\n", res[i])
		//fmt.Printf("expect res:%+v\n", as[i] == bs[i])
		if res[i] != (as[i] == bs[i]) {
			t.Fatalf("float32 equal wrong result")
		}
	}
}

func TestBoolEq(t *testing.T) {
	as := make([]bool, 2)
	bs := make([]bool, 2)
	for i := 0; i < 2; i++ {
		as[i] = true
		bs[i] = false
	}
	cs := make([]bool, 2)
	av := testutil.MakeBoolVector(as)
	bv := testutil.MakeBoolVector(bs)
	cv := testutil.MakeBoolVector(cs)

	err := NumericEqual[bool](av, bv, cv)
	if err != nil {
		t.Fatalf("should not error.")
	}

	res := vector.MustTCols[bool](cv)
	for i := 0; i < 2; i++ {
		fmt.Printf("%+v == %+v : %v \n", as[i], bs[i], res[i])
		assert.Equal(t, as[i] == bs[i], res[i])
	}
}

func TestDec64Eq(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]bool, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64(i)
		bs[i] = int64(3 * i)
	}

	av := testutil.MakeDecimal64Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal64Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeBoolVector(cs)

	err := Decimal64VecEq(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal64 equal failed")
	}

	res := vector.MustTCols[bool](cv)
	for i := 0; i < 10; i++ {
		fmt.Printf("%+v == %+v \n", as[i], bs[i])
		fmt.Printf("actual res:%+v\n", res[i])
		fmt.Printf("expect res:%+v\n", as[i] == bs[i])
		if res[i] != (as[i] == bs[i]) {
			t.Fatalf("decimal64 equal wrong result")
		}
	}
}

func TestDec128Eq(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]bool, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64(i)
		bs[i] = int64(3 * i)
	}

	av := testutil.MakeDecimal128Vector(as, nil, types.T_decimal128.ToType())
	bv := testutil.MakeDecimal128Vector(bs, nil, types.T_decimal128.ToType())
	cv := testutil.MakeBoolVector(cs)

	err := Decimal128VecEq(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal128 equal failed")
	}

	res := vector.MustTCols[bool](cv)
	for i := 0; i < 10; i++ {
		fmt.Printf("%+v == %+v \n", as[i], bs[i])
		fmt.Printf("actual res:%+v\n", res[i])
		fmt.Printf("expect res:%+v\n", as[i] == bs[i])
		if res[i] != (as[i] == bs[i]) {
			t.Fatalf("decimal128 equal wrong result")
		}
	}
}

// benach mark test

func BenchmarkEqI32Eq_C(b *testing.B) {
	as := make([]int32, 8192)
	bs := make([]int32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int32(i)
		bs[i] = 1
	}

	cs := make([]bool, 8192)

	av := testutil.MakeInt32Vector(as, nil)
	bv := testutil.MakeInt32Vector(bs, nil)
	cv := testutil.MakeBoolVector(cs)

	for i := 0; i < b.N; i++ {
		if err := NumericEqual[int32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkEqUI32Eq_C(b *testing.B) {
	as := make([]uint32, 8192)
	bs := make([]uint32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = uint32(i)
		bs[i] = 1
	}

	cs := make([]bool, 8192)

	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeBoolVector(cs)

	for i := 0; i < b.N; i++ {
		if err := NumericEqual[uint32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkEqF64Eq_C(b *testing.B) {
	as := make([]float64, 8192)
	bs := make([]float64, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = float64(i)
		bs[i] = 1
	}

	cs := make([]bool, 8192)

	av := testutil.MakeFloat64Vector(as, nil)
	bv := testutil.MakeFloat64Vector(bs, nil)
	cv := testutil.MakeBoolVector(cs)

	for i := 0; i < b.N; i++ {
		if err := NumericEqual[float64](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkEqDec64(b *testing.B) {
	as := make([]int64, 8192)
	bs := make([]int64, 8192)
	cs := make([]bool, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int64(i)
		bs[i] = 1
	}

	av := testutil.MakeDecimal64Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal64Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeBoolVector(cs)

	for i := 0; i < b.N; i++ {
		if err := Decimal64VecEq(av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkEqDec128(b *testing.B) {
	as := make([]int64, 8192)
	bs := make([]int64, 8192)
	cs := make([]bool, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int64(i)
		bs[i] = 1
	}

	av := testutil.MakeDecimal128Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal128Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeBoolVector(cs)

	for i := 0; i < b.N; i++ {
		if err := Decimal128VecEq(av, bv, cv); err != nil {
			b.Fail()
		}
	}
}
