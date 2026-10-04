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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"testing"
)

func TestMixedDecimalPlusReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	dt := types.T_decimal64.ToType()
	dt.Width = 18
	dt.Scale = 2
	a, e := vector.NewConstFixed(dt, types.Decimal64(123), 1, proc.Mp())
	if e != nil {
		t.Fatal(e)
	}
	defer a.Free(proc.Mp())
	b, e := vector.NewConstFixed(types.T_float64.ToType(), float64(2), 1, proc.Mp())
	if e != nil {
		t.Fatal(e)
	}
	defer b.Free(proc.Mp())
	rt := types.T_decimal128.ToType()
	rt.Width = 38
	rt.Scale = 16
	rs := vector.NewFunctionResultWrapper(rt, proc.Mp())
	defer rs.Free()
	for i := 0; i < 2; i++ {
		if e = rs.PreExtendAndReset(1); e != nil {
			t.Fatal(e)
		}
		if e = plusFn([]*vector.Vector{a, b}, rs, proc, 1, nil); e != nil {
			t.Fatal(e)
		}
		v := vector.MustFixedColWithTypeCheck[types.Decimal128](rs.GetResultVector())[0]
		if v != (types.Decimal128{B0_63: 32300000000000000}) {
			t.Fatalf("call %d wrong coefficient %#v", i, v)
		}
		t.Logf("call %d exact 3.23 passed", i)
	}
}
