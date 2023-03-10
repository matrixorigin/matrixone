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

package mod

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestI32ModOf(t *testing.T) {
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

	err := NumericModSigned[int32](av, bv, cv)
	if err == nil {
		t.Fatalf("should have overflowed.")
	}
}

func TestU32ModOf(t *testing.T) {
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

	err := NumericModUnsigned[uint32](av, bv, cv)
	if err == nil {
		t.Fatalf("should have overflowed.")
	}
}

func TestI32Mod(t *testing.T) {
	as := make([]int32, 10)
	bs := make([]int32, 10)
	cs := make([]int32, 10)
	for i := 0; i < 10; i++ {
		as[i] = int32((i + 1) * 1024)
		bs[i] = int32(7)
	}

	av := testutil.MakeInt32Vector(as, nil)
	bv := testutil.MakeInt32Vector(bs, nil)
	cv := testutil.MakeInt32Vector(cs, nil)

	err := NumericModSigned[int32](av, bv, cv)
	if err != nil {
		t.Fatalf("int32 mod failed")
	}

	res := vector.MustFixedCol[int32](cv)
	for i := 0; i < 10; i++ {
		if res[i] != as[i]%bs[i] {
			t.Fatalf("int mod wrong result")
		}
	}
}

func TestUI32Mod(t *testing.T) {
	as := make([]uint32, 10)
	bs := make([]uint32, 10)
	cs := make([]uint32, 10)
	for i := 0; i < 10; i++ {
		as[i] = uint32((i + 1) * 1024)
		bs[i] = uint32(7)
	}

	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeUint32Vector(cs, nil)

	err := NumericModUnsigned[uint32](av, bv, cv)
	if err != nil {
		t.Fatalf("uint32 mod failed")
	}

	res := vector.MustFixedCol[uint32](cv)
	for i := 0; i < 10; i++ {
		if res[i] != as[i]%bs[i] {
			t.Fatalf("int mod wrong result")
		}
	}
}

func TestF32Mod(t *testing.T) {
	as := make([]float64, 10)
	bs := make([]float64, 10)
	cs := make([]float64, 10)
	for i := 0; i < 10; i++ {
		as[i] = float64(747.34 * (float64)(i+3))
		bs[i] = float64(2.5)
	}

	av := testutil.MakeFloat64Vector(as, nil)
	bv := testutil.MakeFloat64Vector(bs, nil)
	cv := testutil.MakeFloat64Vector(cs, nil)

	err := NumericModFloat[float64](av, bv, cv)
	if err != nil {
		t.Fatalf("uint32 mod failed")
	}

	res := vector.MustFixedCol[float64](cv)
	for i := 0; i < 10; i++ {
		if res[i] != math.Mod(as[i], bs[i]) {
			t.Fatalf("int mod wrong result")
		}
	}
}

func BenchmarkModI32_C(b *testing.B) {
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
		if err := NumericModSigned[int32](av, bv, cv); err != nil {
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
		if err := NumericModUnsigned[uint32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkModF64_C(b *testing.B) {
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
		if err := NumericModFloat[float64](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}
