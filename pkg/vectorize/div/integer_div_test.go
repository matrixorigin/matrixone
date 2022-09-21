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
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestF32IntegerDivByZero(t *testing.T) {
	as := make([]float32, 2)
	bs := make([]float32, 2)
	for i := 0; i < 2; i++ {
		as[i] = 50.23
		bs[i] = 0
	}
	cs := make([]int64, 2)
	av := testutil.MakeFloat32Vector(as, nil)
	bv := testutil.MakeFloat32Vector(bs, nil)
	cv := testutil.MakeInt64Vector(cs, nil)

	err := NumericIntegerDivFloat[float32](av, bv, cv)
	if err != nil {
		if !moerr.IsMoErrCode(err, moerr.ErrDivByZero) {
			t.Fatalf("should have div by zero error.")
		}
	}
}

func TestF64IntegerDivByZero(t *testing.T) {
	as := make([]float64, 2)
	bs := make([]float64, 2)
	for i := 0; i < 2; i++ {
		as[i] = 50
		bs[i] = float64(i)
	}
	cs := make([]int64, 2)
	av := testutil.MakeFloat64Vector(as, nil)
	bv := testutil.MakeFloat64Vector(bs, nil)
	cv := testutil.MakeInt64Vector(cs, nil)

	err := NumericIntegerDivFloat[float64](av, bv, cv)
	if err != nil {
		if !moerr.IsMoErrCode(err, moerr.ErrDivByZero) {
			t.Fatalf("should have div by zero error.")
		}
	}
}

func TestF32IntegerDiv(t *testing.T) {
	as := make([]float32, 1)
	bs := make([]float32, 1)
	cs := make([]int64, 1)
	for i := 0; i < 1; i++ {
		as[i] = float32((i + 1) * 100)
		bs[i] = float32(5)
	}

	av := testutil.MakeFloat32Vector(as, nil)
	bv := testutil.MakeFloat32Vector(bs, nil)
	cv := testutil.MakeInt64Vector(cs, nil)

	err := NumericIntegerDivFloat[float32](av, bv, cv)
	if err != nil {
		t.Fatal(err, "decimal64 integer div failed")
	}
}

func TestF64IntegerDiv(t *testing.T) {
	as := make([]float64, 10)
	bs := make([]float64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = float64(i * 1024)
		bs[i] = float64(8.2)
	}

	av := testutil.MakeFloat64Vector(as, nil)
	bv := testutil.MakeFloat64Vector(bs, nil)
	cv := testutil.MakeInt64Vector(cs, nil)

	err := NumericIntegerDivFloat[float64](av, bv, cv)
	if err != nil {
		t.Fatalf("decimal64 div failed")
	}
}
