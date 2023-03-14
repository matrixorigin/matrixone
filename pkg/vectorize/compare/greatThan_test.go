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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestI32Gt(t *testing.T) {
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

	err := NumericGreatThan[int32](av, bv, cv)
	if err != nil {
		t.Fatal(err)
		t.Fatalf("should not error.")
	}

	res := vector.MustFixedCol[bool](cv)
	for i := 0; i < 10; i++ {
		fmt.Printf("%+v > %+v \n", as[i], bs[i])
		assert.Equal(t, as[i] > bs[i], res[i])
	}
}

func TestU32Gt(t *testing.T) {
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

	err := NumericGreatThan[uint32](av, bv, cv)
	if err != nil {
		t.Fatal(err)
		t.Fatalf("should not error.")
	}

	res := vector.MustFixedCol[bool](cv)
	for i := 0; i < 10; i++ {
		fmt.Printf("%+v > %+v \n", as[i], bs[i])
		assert.Equal(t, as[i] > bs[i], res[i])
	}
}

func TestF32Gt(t *testing.T) {
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

	err := NumericGreatThan[float32](av, bv, cv)
	if err != nil {
		t.Fatalf("should not error.")
	}

	res := vector.MustFixedCol[bool](cv)
	for i := 0; i < 2; i++ {
		fmt.Printf("%+v > %+v \n", as[i], bs[i])
		assert.Equal(t, as[i] > bs[i], res[i])
	}
}

func TestF64Gt(t *testing.T) {
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

	err := NumericGreatThan[float64](av, bv, cv)
	if err != nil {
		t.Fatalf("should not error.")
	}

	res := vector.MustFixedCol[bool](cv)
	for i := 0; i < 2; i++ {
		fmt.Printf("%+v > %+v \n", as[i], bs[i])
		assert.Equal(t, as[i] > bs[i], res[i])
	}
}

func TestBoolGt(t *testing.T) {
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

	err := NumericGreatThan[bool](av, bv, cv)
	if err != nil {
		t.Fatalf("should not error.")
	}

	res := vector.MustFixedCol[bool](cv)
	for i := 0; i < 2; i++ {
		fmt.Printf("%+v > %+v : %v \n", as[i], bs[i], res[i])
		assert.Equal(t, as[i] && !(bs[i]), res[i])
	}
}

func TestDec64Gt(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]bool, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64(i + 5)
		bs[i] = int64(3 * i)
	}

	av := testutil.MakeDecimal64Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal64Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeBoolVector(cs)

	err := Decimal64VecGt(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal64 great than failed")
	}

	res := vector.MustFixedCol[bool](cv)
	for i := 0; i < 10; i++ {
		fmt.Printf("%+v > %+v \n", as[i], bs[i])
		assert.Equal(t, as[i] > bs[i], res[i])
	}
}

func TestDec128Gt(t *testing.T) {
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

	err := Decimal128VecGt(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal128 great than failed")
	}

	res := vector.MustFixedCol[bool](cv)
	for i := 0; i < 10; i++ {
		fmt.Printf("%+v > %+v \n", as[i], bs[i])
		assert.Equal(t, as[i] > bs[i], res[i])
	}
}
