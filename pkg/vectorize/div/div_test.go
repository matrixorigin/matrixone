// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package div

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestF32DivOf(t *testing.T) {
	as := make([]float32, 2)
	bs := make([]float32, 2)
	for i := 0; i < 2; i++ {
		as[i] = 50
		bs[i] = float32(i)
	}
	cs := make([]float32, 2)
	av := testutil.MakeFloat32Vector(as, nil)
	bv := testutil.MakeFloat32Vector(bs, nil)
	cv := testutil.MakeFloat32Vector(cs, nil)

	err := NumericDivFloat[float32](av, bv, cv)
	if err == nil {
		t.Fatalf("should have overflowed.")
	}
}

func TestF32Div(t *testing.T) {
	as := make([]float32, 10)
	bs := make([]float32, 10)
	cs := make([]float32, 10)
	for i := 0; i < 10; i++ {
		as[i] = float32(i * 100)
		bs[i] = float32(5)
	}

	av := testutil.MakeFloat32Vector(as, nil)
	bv := testutil.MakeFloat32Vector(bs, nil)
	cv := testutil.MakeFloat32Vector(cs, nil)

	err := NumericDivFloat[float32](av, bv, cv)
	if err != nil {
		t.Fatalf("decimal64 div failed")
	}

	res := vector.MustFixedCol[float32](cv)
	for i := 0; i < 10; i++ {
		require.Equal(t, res[i], as[i]/bs[i])
	}
}

func TestF64Div(t *testing.T) {
	as := make([]float64, 10)
	bs := make([]float64, 10)
	cs := make([]float64, 10)
	for i := 0; i < 10; i++ {
		as[i] = float64(i * 1024)
		bs[i] = float64(8.2)
	}

	av := testutil.MakeFloat64Vector(as, nil)
	bv := testutil.MakeFloat64Vector(bs, nil)
	cv := testutil.MakeFloat64Vector(cs, nil)

	err := NumericDivFloat[float64](av, bv, cv)
	if err != nil {
		t.Fatalf("decimal64 div failed")
	}

	res := vector.MustFixedCol[float64](cv)
	for i := 0; i < 10; i++ {
		require.Equal(t, res[i], as[i]/bs[i])
	}
}

func TestDec64DivByZero(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64((i + 1) * 1024)
		bs[i] = int64(i)
	}

	av := testutil.MakeDecimal64Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal64Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeDecimal128Vector(cs, nil, types.T_decimal128.ToType())

	err := Decimal64VecDiv(av, bv, cv)
	if err != nil {
		if !moerr.IsMoErrCode(err, moerr.ErrDivByZero) {
			t.Fatalf("should have div by zero error.")
		}
	}
}

func TestDec128DivByZero(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64((i + 1) * 1024)
		bs[i] = int64(i)
	}

	av := testutil.MakeDecimal128Vector(as, nil, types.T_decimal128.ToType())
	bv := testutil.MakeDecimal128Vector(bs, nil, types.T_decimal128.ToType())
	cv := testutil.MakeDecimal128Vector(cs, nil, types.T_decimal128.ToType())

	err := Decimal128VecDiv(av, bv, cv)
	if err != nil {
		if !moerr.IsMoErrCode(err, moerr.ErrDivByZero) {
			t.Fatalf("should have div by zero error.")
		}
	}
}

func TestDec128Div(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64((i + 1) * 1024)
		bs[i] = int64(2)
	}

	av := testutil.MakeDecimal128Vector(as, nil, types.T_decimal128.ToType())
	bv := testutil.MakeDecimal128Vector(bs, nil, types.T_decimal128.ToType())
	cv := testutil.MakeDecimal128Vector(cs, nil, types.T_decimal128.ToType())

	err := Decimal128VecDiv(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal128 div failed")
	}

	res := vector.MustFixedCol[types.Decimal128](cv)
	for i := 0; i < 10; i++ {
		d, _ := types.Decimal128_FromInt64(as[i]/bs[i], 64, 0)
		if !res[i].Eq(d) {
			t.Fatalf("decimal128 div wrong result")
		}
	}
}

func BenchmarkDivF64_C(b *testing.B) {
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
		if err := NumericDivFloat[float64](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkDivDec64(b *testing.B) {
	as := make([]int64, 8192)
	bs := make([]int64, 8192)
	cs := make([]int64, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int64(i)
		bs[i] = 1
	}

	av := testutil.MakeDecimal64Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal64Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeDecimal128Vector(cs, nil, types.T_decimal128.ToType())

	for i := 0; i < b.N; i++ {
		if err := Decimal64VecDiv(av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkDivDec128(b *testing.B) {
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
		if err := Decimal128VecDiv(av, bv, cv); err != nil {
			b.Fail()
		}
	}
}
