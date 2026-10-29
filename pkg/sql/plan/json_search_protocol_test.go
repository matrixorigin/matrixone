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
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func jsonSearchProtocolExpr() *planpb.Expr {
	return &planpb.Expr{
		Typ: types.T_json.ToType().PlanType(),
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: 620 << 32, ObjName: "json_search"},
		}},
	}
}

func TestJSONSearchRequiresProtocol136(t *testing.T) {
	expr := jsonSearchProtocolExpr()
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.JSONSearch)
	require.True(t, features.Any())

	required, err := RequiredPersistedExpressionProtocolVersion(expr)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion136, required)
	require.Equal(t, int64(136), defines.MORPCLatestVersion)
}

func TestJSONSearchRejectedInNonViewDefinitions(t *testing.T) {
	require.ErrorContains(
		t,
		RejectJSONSearchInNonViewDefinition(context.Background(), jsonSearchProtocolExpr()),
		"JSON_SEARCH is not supported",
	)
	require.NoError(t, RejectJSONSearchInNonViewDefinition(
		context.Background(),
		&planpb.Expr{Typ: types.T_json.ToType().PlanType()},
	))
}

func TestJSONSearchProtocolSurvivesWireRoundTrip(t *testing.T) {
	expr := jsonSearchProtocolExpr()
	encoded, err := expr.Marshal()
	require.NoError(t, err)
	var decoded planpb.Expr
	require.NoError(t, decoded.Unmarshal(encoded))

	features, err := planpb.RequiredRemoteExpressionFeatures(&decoded)
	require.NoError(t, err)
	require.True(t, features.JSONSearch)
	required, err := RequiredPersistedExpressionProtocolVersion(&decoded)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion136, required)
}

func TestJSONSearchViewAdmissionBeforeFold(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldFloor)
		} else if value, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, value)
		}
	})

	const sql = `create view v_json_search as select JSON_SEARCH('{"a":"x"}','one','x') as loc`
	build := func(floor int64) (*Plan, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion136)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, floor)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}, stmt, false)
	}

	_, err := build(defines.MORPCVersion134)
	require.ErrorContains(t, err, "protocol version 136")
	created, err := build(defines.MORPCVersion136)
	require.NoError(t, err)

	var data ViewData
	require.NoError(t, json.Unmarshal(
		[]byte(created.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()),
		&data,
	))
	require.NotNil(t, data.RequiredProtocolVersion)
	require.Equal(t, defines.MORPCVersion136, *data.RequiredProtocolVersion)
}
