// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"encoding/json"
	"testing"

	"github.com/gogo/protobuf/proto"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestExactJSONPersistedExpressionOwners(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		old, had := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if had {
				rt.SetGlobalVariables(key, old)
			} else if value, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, value)
			}
		})
	}
	for _, id := range []int32{function.JSON_PARSE_EXACT, function.CAST, function.CAST_STRICT, function.CAST_ASSIGN, function.CAST_IGNORE} {
		source, result := types.T_json, types.T_decimal128
		if id == function.JSON_PARSE_EXACT {
			source, result = types.T_varchar, types.T_json
		}
		expr := &planpb.Expr{Typ: planpb.Type{Id: int32(result), Width: 38, Scale: 3}, Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(id, 0)},
			Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(source)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}},
		}}}
		owner := &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}}
		encoded, err := proto.Marshal(owner)
		require.NoError(t, err)
		loaded := new(planpb.TableDef)
		require.NoError(t, proto.Unmarshal(encoded, loaded))
		floor, err := RequiredPersistedExpressionProtocolVersion(loaded)
		require.NoError(t, err)
		require.Equal(t, int64(113), floor, "bound function ID %d", id)
		for _, version := range []int64{112, 113} {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, version)
			readErr := RequirePersistedExpressionProtocol(proc.Ctx, proc, loaded)
			writeErr := RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, loaded)
			if version == 112 {
				require.ErrorContains(t, readErr, "protocol version 113")
				require.ErrorContains(t, writeErr, "protocol version 113")
			} else {
				require.NoError(t, readErr)
				require.NoError(t, writeErr)
			}
		}
	}
}

func TestExactJSONViewAuthoringBeforeConstantFolding(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		old, had := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if had {
				rt.SetGlobalVariables(key, old)
			} else if value, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, value)
			}
		})
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(113))
	for _, sql := range []string{
		`create view exact_constant as select json_parse_exact('{"cost":9007199254740993.001}') as doc`,
		`create view exact_cast as select cast(json_extract(json_parse_exact('{"cost":9007199254740993.001}'),'$.cost') as decimal(38,3)) as cost`,
		`create view exact_row as select json_parse_exact(l_comment) as doc from lineitem`,
	} {
		t.Run(sql, func(t *testing.T) {
			for _, floor := range []int64{0, 112, 113} {
				rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, floor)
				stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)
				built, err := func() (*Plan, error) {
					defer stmt.Free()
					return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}, stmt, false)
				}()
				if floor < 113 {
					require.ErrorContains(t, err, "protocol version 113")
					continue
				}
				require.NoError(t, err)
				var view ViewData
				require.NoError(t, json.Unmarshal([]byte(built.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &view))
				require.NotNil(t, view.RequiredProtocolVersion)
				require.Equal(t, int64(113), *view.RequiredProtocolVersion)
			}
		})
	}
}

func TestExactJSONSQLPlanRetainsBoundIdentity(t *testing.T) {
	for _, sql := range []string{
		`select l_comment from lineitem where json_extract_string(json_parse_exact(l_comment),'$.x')='v'`,
		`prepare exact_remote from 'select json_parse_exact(?)'`,
	} {
		p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
		require.NoError(t, err)
		expr := findPlanFunctionExpr(p, "json_parse_exact")
		if p.GetDcl() != nil {
			expr = findPlanFunctionExpr(p.GetDcl().GetPrepare().GetPlan(), "json_parse_exact")
		}
		require.NotNil(t, expr)
		require.Equal(t, int32(598), int32(expr.GetF().Func.Obj>>32))
		features, err := planpb.RequiredRemoteExpressionFeatures(p)
		require.NoError(t, err)
		require.True(t, features.Any())
		floor, err := RequiredPersistedExpressionProtocolVersion(p)
		require.NoError(t, err)
		require.Equal(t, int64(113), floor)
	}
	require.GreaterOrEqual(t, int64(defines.MORPCLatestVersion), int64(113))
}
