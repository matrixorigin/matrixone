// Copyright 2021 - 2022 Matrix Origin
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

package sub

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"math"
	"testing"
)

func TestI32SubOf(t *testing.T) {
	as := make([]int32, 2)
	bs := make([]int32, 2)
	for i := 0; i < 2; i++ {
		as[i] = math.MaxInt32
		bs[i] = int32(-i)
	}
	cs := make([]int32, 2)
	av := testutil.MakeInt32Vector(as, nil)
	bv := testutil.MakeInt32Vector(bs, nil)
	cv := testutil.MakeInt32Vector(cs, nil)

	err := NumericSubSigned[int32](av, bv, cv)
	if err == nil {
		t.Fatalf("should have overflowed.")
	}
}

func TestU32SubOf(t *testing.T) {
	as := make([]uint32, 2)
	bs := make([]uint32, 2)
	for i := 0; i < 2; i++ {
		as[i] = uint32(i)
		bs[i] = math.MaxUint32
	}
	cs := make([]uint32, 2)
	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeUint32Vector(cs, nil)

	err := NumericSubUnsigned[uint32](av, bv, cv)
	if err == nil {
		t.Fatalf("should have overflowed.")
	}
}

func TestF32Sub(t *testing.T) {
	as := make([]float32, 2)
	bs := make([]float32, 2)
	for i := 0; i < 2; i++ {
		as[i] = 1
		bs[i] = 0.5
	}
	cs := make([]float32, 2)
	av := testutil.MakeFloat32Vector(as, nil)
	bv := testutil.MakeFloat32Vector(bs, nil)
	cv := testutil.MakeFloat32Vector(cs, nil)

	err := NumericSubFloat[float32](av, bv, cv)
	if err != nil {
		t.Fatalf("should not error.")
	}

	res := vector.MustTCols[float32](cv)
	for i := 0; i < 2; i++ {
		//fmt.Printf("%+v - %+v \n", as[i], bs[i])
		//fmt.Printf("actual res:%+v\n", res[i])
		//fmt.Printf("expect res:%+v\n", as[i]-bs[i])
		if res[i] != as[i]-bs[i] {
			t.Fatalf("float32 sub wrong result")
		}
	}
}

func TestDec64Sub(t *testing.T) {
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

	err := Decimal64VecSub(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal64 sub failed")
	}

	res := vector.MustTCols[types.Decimal64](cv)
	for i := 0; i < 10; i++ {
		//fmt.Printf("%+v - %+v \n", as[i], bs[i])
		//fmt.Printf("actual res:%+v\n", res[i].String())
		//fmt.Printf("expect res:%+v\n", as[i]-bs[i])
		if !res[i].Eq(types.Decimal64_FromInt64(as[i] - bs[i])) {
			t.Fatalf("decimal64 sub wrong result")
		}
	}
}

func TestDec128Sub(t *testing.T) {
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

	err := Decimal128VecSub(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal128 sub failed")
	}

	res := vector.MustTCols[types.Decimal128](cv)
	for i := 0; i < 10; i++ {
		//fmt.Printf("%+v - %+v \n", as[i], bs[i])
		//fmt.Printf("actual res:%+v\n", res[i].String())
		//fmt.Printf("expect res:%+v\n", as[i]-bs[i])
		if !res[i].Eq(types.Decimal128_FromInt64(as[i] - bs[i])) {
			t.Fatalf("decimal128 sub wrong result")
		}
	}
}

func TestDec64SubOfOppNumber(t *testing.T) {
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

	err := Decimal64VecSub(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal64 add failed")
	}

	res := vector.MustTCols[types.Decimal64](cv)
	for i := 0; i < 10; i++ {
		if !res[i].Eq(types.Decimal64_FromInt64(as[i] - bs[i])) {
			t.Fatalf("decimal64 sub wrong result")
		}
	}
}

func TestDec128SubOfOppNumber(t *testing.T) {
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

	err := Decimal128VecSub(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal128 sub failed")
	}

	res := vector.MustTCols[types.Decimal128](cv)
	for i := 0; i < 10; i++ {
		//fmt.Printf("%+v - %+v \n", as[i], bs[i])
		//fmt.Printf("actual res:%+v\n", res[i].String())
		//fmt.Printf("expect res:%+v\n", as[i]-bs[i])
		if !res[i].Eq(types.Decimal128_FromInt64(as[i] - bs[i])) {
			t.Fatalf("decimal128 sub wrong result")
		}
	}
}

func TestDec128SubByFloat64(t *testing.T) {
	cases := []struct {
		name string
		a    float64
		b    float64
		want float64
	}{
		{
			name: "test01",
			a:    1,
			b:    0.5,
			want: 0.5,
		},
		{
			name: "test02",
			a:    1,
			b:    0.4,
			want: 0.6,
		},
		{
			name: "test03",
			a:    0,
			b:    0,
			want: 0,
		},
		{
			name: "test04",
			a:    0,
			b:    0.5,
			want: -0.5,
		},
		{
			name: "test05",
			a:    1,
			b:    0,
			want: 1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			av := testutil.MakeScalarDecimal128ByFloat64(c.a, 1, types.T_decimal128.ToType())
			bv := testutil.MakeScalarDecimal128ByFloat64(c.b, 1, types.T_decimal128.ToType())
			cv := testutil.MakeScalarDecimal128ByFloat64(0, 1, types.T_decimal128.ToType())
			err := Decimal128VecSub(av, bv, cv)
			if err != nil {
				t.Fatalf("decimal128 sub failed")
			}

			res := vector.MustTCols[types.Decimal128](cv)
			if !res[0].Eq(types.Decimal128_FromFloat64(c.want)) {
				t.Fatalf("decimal128 sub wrong result")
			}
		})
	}
}

func BenchmarkSubI32(b *testing.B) {
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
		if err := goNumericSubSigned[int32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkSubI32_C(b *testing.B) {
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
		if err := NumericSubSigned[int32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkSubUI32(b *testing.B) {
	as := make([]uint32, 8192)
	bs := make([]uint32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = uint32(i)
		bs[i] = 0
	}

	cs := make([]uint32, 8192)

	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeUint32Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := goNumericSubUnsigned[uint32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkSubUI32_C(b *testing.B) {
	as := make([]uint32, 8192)
	bs := make([]uint32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = uint32(i)
		bs[i] = 0
	}

	cs := make([]uint32, 8192)

	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeUint32Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := NumericSubUnsigned[uint32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkSubF64(b *testing.B) {
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
		if err := goNumericSubFloat[float64](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkSubF64_C(b *testing.B) {
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
		if err := NumericSubFloat[float64](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkSubDec64(b *testing.B) {
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
		if err := Decimal64VecSub(av, bv, cv); err != nil {
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
		if err := Decimal128VecSub(av, bv, cv); err != nil {
			b.Fail()
		}
	}
}
