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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEnumSourceAssignmentPersistedAdmission(t *testing.T) {
	proc := newPlanTestProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		old, present := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if present {
				rt.SetGlobalVariables(key, old)
			} else if value, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, value)
			}
		})
	}
	set := func(local, read, author int64) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, local)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, read)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, author)
	}
	const sql = "create table t (v enum('red','blue'), g enum('blue','red') generated always as (v) stored)"
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	col := stmt.(*tree.CreateTable).Defs[1].(*tree.ColumnTableDef)
	target := planpb.Type{Id: int32(types.T_enum), Enumvalues: "blue,red"}
	source := []*ColDef{{Name: "v", Typ: planpb.Type{Id: int32(types.T_enum), Enumvalues: "red,blue"}}}
	for _, floors := range [][3]int64{{122, 128, 128}, {128, 128, 122}, {128, 128, 128}} {
		set(floors[0], floors[1], floors[2])
		generated, err := buildGeneratedExpr(proc.Ctx, col, target, source, proc)
		if floors[0] < 128 || floors[2] < 128 {
			require.ErrorContains(t, err, "protocol version 128")
			continue
		}
		require.NoError(t, err)
		required, err := RequiredPersistedExpressionProtocolVersion(generated)
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion128, required)
		data, err := generated.Marshal()
		require.NoError(t, err)
		var decoded planpb.GeneratedCol
		require.NoError(t, decoded.Unmarshal(data))
		set(128, 122, 128)
		require.ErrorContains(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, &decoded), "protocol version 128")
		set(128, 128, 122)
		require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, &decoded), "read does not depend on the authoring floor")
	}
	set(122, 122, 122)
	legacy := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CAST_VALUE_TO_INDEX, 0)}}}}
	require.NoError(t, RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, legacy))
}

func TestGeneratedDisplayWrapperChecksVolatileChildren(t *testing.T) {
	for _, id := range []int32{function.CAST_INDEX_TO_VALUE, function.CAST_INDEX_TO_SET_VALUE} {
		display := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(id, 0)}}}}
		require.NoError(t, checkExprForVolatileFunc(t.Context(), display))
		volatile, err := BindFuncExprImplByPlanExpr(t.Context(), "rand", nil)
		require.NoError(t, err)
		display.GetF().Args = []*planpb.Expr{volatile}
		require.ErrorContains(t, checkExprForVolatileFunc(t.Context(), display), "non-deterministic")
	}
}
