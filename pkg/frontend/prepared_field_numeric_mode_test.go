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

func TestPreparedFieldExplicitCastKeepsHistory(t *testing.T) {
	for _, cast := range []string{"cast(? as signed)", "cast(? as decimal(10,2))", "cast(? as char)", "cast(cast(? as char) as signed)"} {
		t.Run(cast, func(t *testing.T) {
			ses, prepared, cw, ec := newPreparedExecuteEnvForSQL(t, 25306,
				"select field(case when ? then ? else null end,'A','a'),"+cast)
			prepared.proc = cw.proc
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })
			execute := func(null, integer bool, want uint64) {
				cw.discardRuntimeCacheCandidate()
				prepared.clearBinaryParamState(cw.proc)
				prepared.params = vector.NewVec(types.T_text.ToType())
				for i, value := range []string{"1", "a", "1"} {
					require.NoError(t, vector.AppendBytes(prepared.params, []byte(value), null && i == 1, cw.proc.Mp()))
				}
				valueType, operandType := defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VAR_STRING
				if null {
					valueType = defines.MYSQL_TYPE_NULL
				}
				if integer {
					operandType = defines.MYSQL_TYPE_LONGLONG
				}
				prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0, byte(valueType), 0, byte(operandType), 0}
				_, p, stmt, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
				if owned && stmt != nil {
					defer stmt.Free()
				}
				require.NoError(t, err)
				q := p.GetQuery()
				expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
				result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				defer free()
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[uint64](result, 0))
			}
			execute(true, false, 0)
			execute(false, false, 2)
			history := prepared.fieldTypes
			require.NotNil(t, history)
			revision := history.Revision()
			execute(false, true, 2)
			require.Same(t, history, prepared.fieldTypes, "an unrelated explicit CAST pins its parameter contract")
			require.Equal(t, revision, prepared.fieldTypes.Revision())
		})
	}
}

func TestPreparedFieldNumericModeGeneration(t *testing.T) {
	for _, tc := range []struct {
		name, setting, expr string
		before, after       any
		runtimeUnsigned     bool
		changed             bool
		earlyFailure        bool
	}{
		{"static unsigned", "sql_mode", "cast(? as unsigned)-cast(1 as unsigned)", "", "NO_UNSIGNED_SUBTRACTION", false, true, false},
		{"runtime unsigned", "sql_mode", "0-?", "", "NO_UNSIGNED_SUBTRACTION", true, true, false},
		{"runtime unsigned early rejection", "sql_mode", "0-?", "", "NO_UNSIGNED_SUBTRACTION", true, true, true},
		{"unrelated unsigned mode early rejection", "sql_mode", "?", "", "NO_UNSIGNED_SUBTRACTION", false, false, true},
		{"unrelated precision early rejection", "div_precision_increment", "?", int64(4), int64(8), false, false, true},
		{"decimal scale changes", "div_precision_increment", "cast(? as decimal(10,2))/cast(1 as decimal(10,2))", int64(4), int64(8), false, true, false},
		{"decimal scale capped", "div_precision_increment", "cast(? as decimal(65,30))/cast(1 as decimal(65,30))", int64(4), int64(8), false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := "select field(case when ? then ? else null end,'A','a')," + tc.expr
			if tc.runtimeUnsigned {
				sql = "select field(case when ? then 0-? else null end,0,1)," + tc.expr
			}
			ses, prepared, cw, ec := newPreparedExecuteEnvForSQL(t, 25305, sql)
			prepared.proc = cw.proc
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })
			original, err := ses.GetSessionSysVar(tc.setting)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, ses.SetSessionSysVar(ec.reqCtx, tc.setting, original)) })
			require.NoError(t, ses.SetSessionSysVar(ec.reqCtx, tc.setting, tc.before))
			settingChanged, earlyRejection := false, false
			execute := func(null bool, want uint64, fail bool) plan.Type {
				cw.discardRuntimeCacheCandidate()
				prepared.clearBinaryParamState(cw.proc)
				prepared.params = vector.NewVec(types.T_text.ToType())
				require.NoError(t, vector.AppendBytes(prepared.params, []byte("1"), false, cw.proc.Mp()))
				payload, operand := "a", "5"
				typ, operandType := defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VAR_STRING
				var flags byte
				if tc.runtimeUnsigned {
					payload = "0"
					if settingChanged {
						payload = "1"
					}
					operand, typ, operandType, flags = payload, defines.MYSQL_TYPE_LONGLONG, defines.MYSQL_TYPE_LONGLONG, 0x80
				}
				require.NoError(t, vector.AppendBytes(prepared.params, []byte(payload), null, cw.proc.Mp()))
				require.NoError(t, vector.AppendBytes(prepared.params, []byte(operand), false, cw.proc.Mp()))
				if null {
					typ = defines.MYSQL_TYPE_NULL
				}
				prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0, byte(typ), flags, byte(operandType), flags}
				if earlyRejection {
					prepared.params.SetLength(2)
				}
				_, p, stmt, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
				if owned && stmt != nil {
					stmt.Free()
				}
				if earlyRejection {
					require.ErrorContains(t, err, "Incorrect arguments to EXECUTE")
					return plan.Type{}
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
			beforeWant, afterWant := uint64(2), uint64(1)
			if tc.runtimeUnsigned {
				beforeWant, afterWant = 1, 0
			} else if !tc.changed {
				afterWant = beforeWant
			}
			before := execute(false, beforeWant, false)
			snapshot, oldPlan := prepared.fieldTypes, prepared.PreparePlan
			require.NoError(t, ses.SetSessionSysVar(ec.reqCtx, tc.setting, tc.after))
			settingChanged = true
			if tc.earlyFailure {
				earlyRejection = true
				execute(false, 0, false)
				require.Same(t, snapshot, prepared.fieldTypes)
				require.False(t, prepared.fieldTypesResetPending)
				// Unknown contracts must be compared on retry, not discarded.
				if tc.setting == "sql_mode" {
					require.False(t, prepared.NoUnsignedSubtraction)
				} else {
					require.Equal(t, tc.before, prepared.divPrecisionIncrement)
				}
				earlyRejection = false
			}
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
			require.Equal(t, tc.changed, prepared.fieldTypesResetPending)
			writer.makeColumnDefDataFunc = nil
			after := execute(false, afterWant, false)
			if tc.changed {
				require.NotSame(t, snapshot, prepared.fieldTypes)
			} else {
				require.Same(t, snapshot, prepared.fieldTypes)
			}
			require.False(t, prepared.fieldTypesResetPending)
			if tc.setting == "sql_mode" && tc.changed {
				require.Equal(t, int32(types.T_uint64), before.Id)
				require.Equal(t, int32(types.T_int64), after.Id)
			} else if tc.changed {
				require.Greater(t, after.Scale, before.Scale)
			} else {
				require.Equal(t, before, after)
			}
		})
	}
}
