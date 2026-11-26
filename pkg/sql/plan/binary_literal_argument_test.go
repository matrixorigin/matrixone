// Copyright 2026 Matrix Origin
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

package plan

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// Hex/bit literals keep their numeric meaning as kernel arguments after the
// vector lost its isBin bit: the binder, plan-time folding and the executor
// all see the literal's own flag, while a function result never is binary.
func TestBinaryLiteralNumericContextThroughPlanner(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want []string
	}{
		{"select 0x41 + 0, '5' + 0", []string{"65", "5"}},
		{"select cast(X'41' as signed), cast(0x41 as unsigned)", []string{"65", "65"}},
		// The explicit CAST applies its scale to the unsigned integer
		// magnitude (#31816): 0x3135 is the number 12597, so decimal(10,2)
		// renders 12597.00 while the quoted text control stays 15.00.
		{"select cast(X'3135' as decimal(10, 2)), cast('15' as decimal(10, 2))", []string{"12597.00", "15.00"}},
		{"select cast(0x41 as double), cast(b'01000001' as double)", []string{"65", "65"}},
		{"select cast(concat(0x3135) as signed), 0x3135 + 0", []string{"15", "12597"}},
		// x'30' is 48 as a number but '0' as text.
		{"select if(x'30', 1, 2), if('0', 1, 2)", []string{"1", "2"}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			p, err := runOneStmt(NewMockOptimizer(false, proc), t, tc.sql)
			require.NoError(t, err)
			query := p.GetQuery()
			projections := query.Nodes[query.Steps[query.MainOutputStep()]].ProjectList
			require.Len(t, projections, len(tc.want))
			input := batch.NewWithSize(0)
			input.SetRowCount(1)
			for i, expr := range projections {
				result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{input})
				require.NoError(t, err, "column %d", i)
				require.Equal(t, tc.want[i], binaryLiteralResultText(t, result), "column %d", i)
				free()
			}
		})
	}
}

func binaryLiteralResultText(t *testing.T, vec *vector.Vector) string {
	t.Helper()
	require.False(t, vec.IsNull(0))
	switch typ := vec.GetType(); typ.Oid {
	case types.T_int64:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[int64](vec, 0))
	case types.T_uint64:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[uint64](vec, 0))
	case types.T_float64:
		return fmt.Sprint(vector.GetFixedAtNoTypeCheck[float64](vec, 0))
	case types.T_decimal64:
		return vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, 0).Format(typ.Scale)
	case types.T_decimal128:
		return vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, 0).Format(typ.Scale)
	default:
		t.Fatalf("unexpected result type %s", typ)
		return ""
	}
}
