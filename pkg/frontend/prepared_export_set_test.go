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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedExportSetNullDomainHistory(t *testing.T) {
	for _, scalar := range []bool{true, false} {
		sql := `select export_set(coalesce((select ?),1.5),'Y','N','',4)`
		if !scalar {
			sql = `select export_set((select coalesce(x,1.5) from (select max(?) as x from tpch.nation) d),'Y','N','',4)`
		}
		t.Run(sql, func(t *testing.T) {
			ses, stmt, cw, execCtx := newPreparedExecuteEnvForSQLWithCompilerContext(t, 391, sql, plan2.NewMockCompilerContext(true))
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil); stmt.Close() })
			require.Equal(t, []int32{0}, stmt.exportSetParamPositions)
			cached := stmt.PreparePlan.GetDcl().GetPrepare().Plan
			before := cached.String()
			for _, tc := range []struct {
				value    string
				null     bool
				mysql    defines.MysqlType
				want     string
				decimal  bool
				floating bool
				wantErr  bool
				rebuild  bool
			}{
				{null: true, mysql: defines.MYSQL_TYPE_NULL, want: "YNNN"},
				{value: "2.5", mysql: defines.MYSQL_TYPE_NEWDECIMAL, want: "YYNN", decimal: true},
				{value: "invalid", mysql: defines.MYSQL_TYPE_NEWDECIMAL, wantErr: true},
				{null: true, mysql: defines.MYSQL_TYPE_NULL, want: "NYNN", decimal: true, rebuild: true},
				{value: "2.5", mysql: defines.MYSQL_TYPE_DOUBLE, want: "NYNN", floating: true},
				{null: true, mysql: defines.MYSQL_TYPE_NULL, want: "NYNN", floating: true},
				{value: "2.5", mysql: defines.MYSQL_TYPE_NEWDECIMAL, want: "YYNN", decimal: true},
			} {
				cw.proc.SetPrepareParams(nil)
				if stmt.params != nil {
					stmt.params.Free(cw.proc.Mp())
					stmt.params = nil
				}
				params := vector.NewVec(types.T_text.ToType())
				stmt.params = params
				require.NoError(t, vector.AppendBytes(params, []byte(tc.value), tc.null, cw.proc.Mp()))
				stmt.ParamTypes = []byte{byte(tc.mysql), 0}
				history := append([]types.Type(nil), stmt.exportSetParamTypes...)
				if tc.rebuild && scalar {
					stmt.needsRebuild = true
				}
				_, filled, _, _, _, err := initExecuteStmtParam(execCtx, ses, cw, nil, stmt.Name)
				if tc.wantErr {
					require.Error(t, err)
					require.Equal(t, history, stmt.exportSetParamTypes, "decoder failure must not publish history")
					require.Equal(t, before, cached.String())
					continue
				}
				require.NoError(t, err)
				if tc.rebuild && scalar {
					require.False(t, stmt.needsRebuild)
					require.NotSame(t, cached, stmt.PreparePlan.GetDcl().GetPrepare().Plan)
				}
				var export *plan.Expr
				require.NoError(t, plan.VisitExpressionsInOwner(filled, func(root *plan.Expr) error {
					return plan.VisitExprTree(root, func(expr *plan.Expr) error {
						if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "export_set" {
							export = expr
						}
						return nil
					})
				}))
				require.NotNil(t, export)
				if tc.decimal {
					require.True(t, types.T(export.GetF().Args[0].Typ.Id).IsDecimal(), export.String())
				}
				if tc.floating {
					require.Equal(t, int32(types.T_float64), export.GetF().Args[0].Typ.Id, export.String())
				}
				if scalar {
					result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, export, []*batch.Batch{batch.EmptyForConstFoldBatch})
					require.NoError(t, err)
					func() {
						defer free()
						require.Equal(t, tc.want, result.GetStringAt(0), "params=%+v expr=%s", cw.paramVals, export.String())
					}()
				}
				require.Equal(t, before, cached.String())
			}
		})
	}
}

func TestPreparedExportSetRebuildTypeOwnership(t *testing.T) {
	_, prepared, _, _ := newPreparedExecuteEnvForSQL(t, 392, `select export_set(coalesce((select ?),1.5),?,?,'',4)`)
	t.Cleanup(prepared.Close)
	cached := prepared.PreparePlan.GetDcl().GetPrepare().Plan
	decimal := types.New(types.T_decimal256, 65, 1)
	prepared.exportSetParamTypes = []types.Type{decimal, types.T_float64.ToType(), types.T_float64.ToType()}
	prepared.refreshExportSetParamPositions(cached, 3)
	require.Equal(t, []types.Type{decimal, {}, {}}, prepared.exportSetParamTypes)
	prepared.refreshExportSetParamPositions(cached, 4)
	require.Nil(t, prepared.exportSetParamTypes, "incompatible marker count invalidates the mapping")
	prepared.exportSetParamTypes = []types.Type{decimal}
	prepared.refreshExportSetParamPositions(nil, 1)
	require.Nil(t, prepared.exportSetParamPositions)
	require.Nil(t, prepared.exportSetParamTypes)
}

func TestPreparedExportSetNullTypeStateBoundaries(t *testing.T) {
	stmt := &PrepareStmt{exportSetParamPositions: []int32{-1, 0, 2, 5}}
	t.Cleanup(stmt.Close)
	decimal := types.New(types.T_decimal256, 65, 1)
	values := []any{plan2.ParamValue{Value: "2.5", SourceType: decimal, HasSourceType: true}, plan2.ParamValue{Value: 7}, nil}
	stmt.applyExportSetNullRuntimeTypes(values)
	require.Equal(t, decimal, stmt.exportSetParamTypes[0])
	require.Equal(t, types.T_any, stmt.exportSetParamTypes[1].Oid)
	require.Nil(t, values[2], "first NULL remains untyped")
	values[0] = plan2.ParamValue{Value: nil, IsBinaryProtocol: true}
	stmt.applyExportSetNullRuntimeTypes(values)
	current := values[0].(plan2.ParamValue)
	require.Nil(t, current.Value)
	require.True(t, current.IsBinaryProtocol)
	require.Equal(t, decimal, current.RuntimeType)
	values[0] = plan2.ParamValue{Value: "text", SourceType: types.T_text.ToType(), HasSourceType: true}
	stmt.applyExportSetNullRuntimeTypes(values)
	require.Equal(t, types.T_any, stmt.exportSetParamTypes[0].Oid)
	values[0] = nil
	stmt.applyExportSetNullRuntimeTypes(values)
	require.Nil(t, values[0])
	values[2] = plan2.ParamValue{Value: nil, HasRuntimeType: true, RuntimeType: types.T_blob.ToType()}
	stmt.applyExportSetNullRuntimeTypes(values)
	require.Equal(t, types.T_blob, values[2].(plan2.ParamValue).RuntimeType.Oid)
	stmt.Close()
	require.Nil(t, stmt.exportSetParamTypes)
	require.Nil(t, stmt.exportSetParamPositions)
	stmt.applyExportSetNullRuntimeTypes(values)
	require.Nil(t, stmt.exportSetParamTypes)
}
