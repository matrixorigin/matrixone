// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func integerDecimalCastBatch(tb testing.TB, oid types.T, unsigned bool) *FunctionTestCase {
	tb.Helper()
	proc := testutil.NewProcess(tb)
	tb.Cleanup(proc.Free)
	const rows = 4096
	values := make([]int64, rows)
	uvalues := make([]uint64, rows)
	for i := range values {
		values[i] = int64(1000 + i)
		uvalues[i] = uint64(values[i])
	}
	source := NewFunctionTestInput(types.T_int64.ToType(), values, nil)
	if unsigned {
		source = NewFunctionTestInput(types.T_uint64.ToType(), uvalues, nil)
	}
	target := oid.ToType()
	target.Scale = 0
	fc := NewFunctionTestCase(proc, []FunctionTestInput{source, NewFunctionTestInput(target, emptyCastTargetValues(target), nil)},
		NewFunctionTestResult(target, false, emptyCastTargetValues(target), nil), NewCast)
	tb.Cleanup(fc.Free)
	return &fc
}

func TestIntegerDecimalCastSuccessAllocations(t *testing.T) {
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		for _, unsigned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unsigned%v", oid, unsigned), func(t *testing.T) {
				fc := integerDecimalCastBatch(t, oid, unsigned)
				var result *vector.Vector
				var err error
				allocs := testing.AllocsPerRun(5, func() { result, err = fc.DebugRun() })
				require.NoError(t, err)
				require.Equal(t, 4096, result.Length())
				// NewCast creates one fixed-size dispatch wrapper per batch;
				// successful range checks must not add a box for each row.
				require.LessOrEqual(t, allocs, float64(1))
			})
		}
	}
}

func BenchmarkIntegerDecimalCastBatch(b *testing.B) {
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		b.Run(oid.String(), func(b *testing.B) {
			fc := integerDecimalCastBatch(b, oid, false)
			_, err := fc.DebugRun()
			require.NoError(b, err)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := fc.DebugRun(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
