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

package frontend

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldNumericModeGeneration(t *testing.T) {
	for _, tc := range []struct {
		setting, expr string
		before, after any
	}{
		{"sql_mode", "cast(? as unsigned)-cast(1 as unsigned)", "", "NO_UNSIGNED_SUBTRACTION"},
		{"div_precision_increment", "cast(? as decimal(10,2))/cast(1 as decimal(10,2))", int64(4), int64(8)},
	} {
		t.Run(tc.setting, func(t *testing.T) {
			ses, prepared, cw, ec := newPreparedExecuteEnvForSQL(t, 25305, "select field(case when ? then ? else null end,'A','a'),"+tc.expr)
			prepared.proc = cw.proc
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })
			original, err := ses.GetSessionSysVar(tc.setting)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, ses.SetSessionSysVar(ec.reqCtx, tc.setting, original)) })
			require.NoError(t, ses.SetSessionSysVar(ec.reqCtx, tc.setting, tc.before))
			execute := func(null bool, want uint64, fail bool) plan.Type {
				cw.discardRuntimeCacheCandidate()
				prepared.clearBinaryParamState(cw.proc)
				prepared.params = vector.NewVec(types.T_text.ToType())
				require.NoError(t, vector.AppendBytes(prepared.params, []byte("1"), false, cw.proc.Mp()))
				require.NoError(t, vector.AppendBytes(prepared.params, []byte("a"), null, cw.proc.Mp()))
				require.NoError(t, vector.AppendBytes(prepared.params, []byte("5"), false, cw.proc.Mp()))
				typ := defines.MYSQL_TYPE_VAR_STRING
				if null {
					typ = defines.MYSQL_TYPE_NULL
				}
				prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0, byte(typ), 0, byte(defines.MYSQL_TYPE_VAR_STRING), 0}
				_, p, stmt, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
				if owned && stmt != nil {
					stmt.Free()
				}
				if fail {
					require.ErrorContains(t, err, "field numeric mode metadata failure")
					return plan.Type{}
				}
				require.NoError(t, err)
				q := p.GetQuery()
				exprs := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList
				result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, exprs[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				defer free()
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[uint64](result, 0))
				return exprs[1].Typ
			}
			execute(true, 0, false)
			before := execute(false, 2, false)
			snapshot, oldPlan := prepared.fieldTypes, prepared.PreparePlan
			require.NoError(t, ses.SetSessionSysVar(ec.reqCtx, tc.setting, tc.after))
			writer := ec.resper.MysqlRrWr().(*testMysqlWriter)
			calls := 0
			writer.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
				calls++
				if calls == 2 {
					return nil, errors.New("field numeric mode metadata failure")
				}
				return nil, nil
			}
			t.Cleanup(func() { writer.makeColumnDefDataFunc = nil })
			execute(false, 0, true)
			require.NotSame(t, oldPlan, prepared.PreparePlan)
			require.Same(t, snapshot, prepared.fieldTypes)
			require.True(t, prepared.fieldTypesResetPending)
			writer.makeColumnDefDataFunc = nil
			after := execute(false, 1, false)
			require.NotSame(t, snapshot, prepared.fieldTypes)
			require.False(t, prepared.fieldTypesResetPending)
			if tc.setting == "sql_mode" {
				require.Equal(t, int32(types.T_uint64), before.Id)
				require.Equal(t, int32(types.T_int64), after.Id)
			} else {
				require.Greater(t, after.Scale, before.Scale)
			}
		})
	}
}
