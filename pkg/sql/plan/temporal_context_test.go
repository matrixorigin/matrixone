// Copyright 2026 Matrix Origin
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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalContextSQLExpressions(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want any
	}{
		{"not cast('2024-01-02' as date)", false},
		{"!cast('0001-01-01' as date)", false},
		{"cast('2024-01-02 03:04:05' as datetime) and 1", true},
		{"0 or cast('00:00:00.000001' as time(6))", true},
		{"cast('-00:00:00.000001' as time(6)) xor 1", false},
		{"cast('00:00:00' as time) is false", true},
		{"cast(null as datetime) is not true", true},
		{"cast(null as date) is true", false},
		{"cast(null as time) is false", false},
		{"cast(null as time) is not false", true},
		{"cast(null as time) and 0", false},
		{"cast(null as datetime) or 1", true},
		{"abs(cast('-00:00:00.000001' as time(6)))", float64(0.000001)},
		{"sign(cast('-00:00:00.000001' as time(6)))", int64(-1)},
		{"sqrt(cast('00:01:40' as time))", float64(11.832159566199232)},
		{"truncate(cast('2024-01-02 03:04:05' as datetime), -2)", float64(20240102030400)},
		{"round(cast('12:59:59.6' as time(1)),0)", float64(125960)},
		{"floor(cast('-00:00:00.1' as time(1)))", float64(-1)},
		{"ceiling(cast('00:00:00.1' as time(1)))", float64(1)},
		{"abs(cast('2024-01-02' as date))", float64(20240102)},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "select "+tc.expr)
			require.NoError(t, err)
			query := p.GetQuery()
			expr := query.Nodes[query.Steps[0]].ProjectList[0]
			proc := testutil.NewProc(t)
			defer proc.Free()
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.False(t, result.IsNull(0))
			switch want := tc.want.(type) {
			case bool:
				require.Equal(t, types.T_bool, result.GetType().Oid)
				require.Equal(t, want, vector.GetFixedAtWithTypeCheck[bool](result, 0))
			case int64:
				require.Equal(t, types.T_int64, result.GetType().Oid)
				require.Equal(t, want, vector.GetFixedAtWithTypeCheck[int64](result, 0))
			case float64:
				require.Equal(t, types.T_float64, result.GetType().Oid)
				require.Equal(t, want, vector.GetFixedAtWithTypeCheck[float64](result, 0))
			}
		})
	}
}
