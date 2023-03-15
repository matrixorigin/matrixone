// Copyright 2021 Matrix Origin
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

package add

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestI32Of(t *testing.T) {
	as := make([]int32, 2)
	bs := make([]int32, 2)
	for i := 0; i < 2; i++ {
		as[i] = math.MaxInt32
		bs[i] = int32(i)
	}
	cs := make([]int32, 2)
	av := testutil.MakeInt32Vector(as, nil)
	bv := testutil.MakeInt32Vector(bs, nil)
	cv := testutil.MakeInt32Vector(cs, nil)

	err := NumericAddSigned[int32](av, bv, cv)
	if err == nil {
		t.Fatalf("should have overflowed.")
	}
}

func TestU32Of(t *testing.T) {
	as := make([]uint32, 2)
	bs := make([]uint32, 2)
	for i := 0; i < 2; i++ {
		as[i] = math.MaxUint32
		bs[i] = uint32(i)
	}
	cs := make([]uint32, 2)
	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeUint32Vector(cs, nil)

	err := NumericAddUnsigned[uint32](av, bv, cv)
	if err == nil {
		t.Fatalf("should have overflowed.")
	}
}

func TestDec64(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64(i)
		bs[i] = int64(3 * i)
	}

	av := testutil.MakeDecimal64Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal64Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeDecimal64Vector(cs, nil, types.T_decimal64.ToType())

	err := Decimal64VecAdd(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal64 add failed")
	}

	res := vector.MustFixedCol[types.Decimal64](cv)
	for i := 0; i < 10; i++ {
		d, _ := types.Decimal64_FromInt64(as[i]+bs[i], 64, 0)
		if !res[i].Eq(d) {
			t.Fatalf("decimal64 add wrong result")
		}
	}
}

func TestDec128(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64(i)
		bs[i] = int64(3 * i)
	}

	av := testutil.MakeDecimal128Vector(as, nil, types.T_decimal128.ToType())
	bv := testutil.MakeDecimal128Vector(bs, nil, types.T_decimal128.ToType())
	cv := testutil.MakeDecimal128Vector(cs, nil, types.T_decimal128.ToType())

	err := Decimal128VecAdd(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal128 add failed")
	}

	res := vector.MustFixedCol[types.Decimal128](cv)
	for i := 0; i < 10; i++ {
		d, _ := types.Decimal128_FromInt64(as[i]+bs[i], 64, 0)
		if !res[i].Eq(d) {
			t.Fatalf("decimal128 add wrong result")
		}
	}
}

func TestDec64OfOppNumber(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64(-i)
		bs[i] = int64(i)
	}

	av := testutil.MakeDecimal64Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal64Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeDecimal64Vector(cs, nil, types.T_decimal64.ToType())

	err := Decimal64VecAdd(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal64 add failed")
	}

	res := vector.MustFixedCol[types.Decimal64](cv)
	for i := 0; i < 10; i++ {
		d, _ := types.Decimal64_FromInt64(as[i]+bs[i], 64, 0)
		if !res[i].Eq(d) {
			t.Fatalf("decimal64 add wrong result")
		}
	}
}

func TestDec128OfOppNumber(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64(i)
		bs[i] = int64(-i)
	}

	av := testutil.MakeDecimal128Vector(as, nil, types.T_decimal128.ToType())
	bv := testutil.MakeDecimal128Vector(bs, nil, types.T_decimal128.ToType())
	cv := testutil.MakeDecimal128Vector(cs, nil, types.T_decimal128.ToType())

	err := Decimal128VecAdd(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal128 add failed")
	}

	res := vector.MustFixedCol[types.Decimal128](cv)
	for i := 0; i < 10; i++ {
		d, _ := types.Decimal128_FromInt64(as[i]+bs[i], 64, 0)
		if !res[i].Eq(d) {
			t.Fatalf("decimal128 add wrong result")
		}
	}
}

func BenchmarkAddI32(b *testing.B) {
	as := make([]int32, 8192)
	bs := make([]int32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int32(i)
		bs[i] = 1
	}

	cs := make([]int32, 8192)

	av := testutil.MakeInt32Vector(as, nil)
	bv := testutil.MakeInt32Vector(bs, nil)
	cv := testutil.MakeInt32Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := goNumericAddSigned[int32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddI32_C(b *testing.B) {
	as := make([]int32, 8192)
	bs := make([]int32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int32(i)
		bs[i] = 1
	}

	cs := make([]int32, 8192)

	av := testutil.MakeInt32Vector(as, nil)
	bv := testutil.MakeInt32Vector(bs, nil)
	cv := testutil.MakeInt32Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := cNumericAddSigned[int32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddUI32(b *testing.B) {
	as := make([]uint32, 8192)
	bs := make([]uint32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = uint32(i)
		bs[i] = 1
	}

	cs := make([]uint32, 8192)

	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeUint32Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := goNumericAddUnsigned[uint32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddUI32_C(b *testing.B) {
	as := make([]uint32, 8192)
	bs := make([]uint32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = uint32(i)
		bs[i] = 1
	}

	cs := make([]uint32, 8192)

	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeUint32Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := cNumericAddUnsigned[uint32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddF64(b *testing.B) {
	as := make([]float64, 8192)
	bs := make([]float64, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = float64(i)
		bs[i] = 1
	}

	cs := make([]float64, 8192)

	av := testutil.MakeFloat64Vector(as, nil)
	bv := testutil.MakeFloat64Vector(bs, nil)
	cv := testutil.MakeFloat64Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := goNumericAddFloat[float64](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddF64_C(b *testing.B) {
	as := make([]float64, 8192)
	bs := make([]float64, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = float64(i)
		bs[i] = 1
	}

	cs := make([]float64, 8192)

	av := testutil.MakeFloat64Vector(as, nil)
	bv := testutil.MakeFloat64Vector(bs, nil)
	cv := testutil.MakeFloat64Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := cNumericAddFloat[float64](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddDec64(b *testing.B) {
	as := make([]int64, 8192)
	bs := make([]int64, 8192)
	cs := make([]int64, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int64(i)
		bs[i] = 1
	}

	av := testutil.MakeDecimal64Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal64Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeDecimal64Vector(cs, nil, types.T_decimal64.ToType())

	for i := 0; i < b.N; i++ {
		if err := Decimal64VecAdd(av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddDec128(b *testing.B) {
	as := make([]int64, 8192)
	bs := make([]int64, 8192)
	cs := make([]int64, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int64(i)
		bs[i] = 1
	}

	av := testutil.MakeDecimal128Vector(as, nil, types.T_decimal128.ToType())
	bv := testutil.MakeDecimal128Vector(bs, nil, types.T_decimal128.ToType())
	cv := testutil.MakeDecimal128Vector(cs, nil, types.T_decimal128.ToType())

	for i := 0; i < b.N; i++ {
		if err := Decimal128VecAdd(av, bv, cv); err != nil {
			b.Fail()
		}
	}
}
