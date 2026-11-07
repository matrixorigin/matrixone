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

// IF conditions are a boolean context (#32158): temporal values select the
// true branch when their numeric value is nonzero.
func TestIffTemporalConditionSQLExpressions(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want int64
	}{
		{"if(cast('2024-01-02' as date), 1, 0)", 1},
		{"if(cast('2024-01-02 03:04:05' as datetime), 1, 0)", 1},
		{"if(cast('12:34:56' as time), 1, 0)", 1},
		{"if(cast('00:00:00' as time), 1, 0)", 0},
		{"if(cast('00:00:00.000001' as time(6)), 1, 0)", 1},
		{"if(cast('-00:00:00.000001' as time(6)), 1, 0)", 1},
		{"if(cast(null as date), 1, 0)", 0},
		{"if(cast(null as datetime), 1, 0)", 0},
		{"if(cast(null as time), 1, 0)", 0},
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
			require.Equal(t, tc.want, vector.GetFixedAtWithTypeCheck[int64](result, 0))
		})
	}
}

func TestIffTemporalColumnConditionCastsToBool(t *testing.T) {
	for _, oid := range []types.T{types.T_date, types.T_datetime, types.T_time, types.T_timestamp} {
		t.Run(oid.String(), func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.tables["nation"].Cols[0].Typ = oid.ToType().PlanType()
			p, err := runOneStmt(mock, t, "select if(n_nationkey, 1, 0) from nation")
			require.NoError(t, err)
			q := p.GetQuery()
			iff := q.Nodes[q.Steps[0]].ProjectList[0].GetF()
			require.NotNil(t, iff)
			cond := iff.Args[0]
			require.Equal(t, int32(types.T_bool), cond.Typ.Id)
			cast := cond.GetF()
			require.NotNil(t, cast)
			require.Equal(t, "cast", cast.Func.ObjName)
			require.Equal(t, int32(oid), cast.Args[0].Typ.Id)
		})
	}
}
