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
	"encoding/json"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestTableCollationExpressionProtocolIdentity(t *testing.T) {
	for _, id := range []int32{function.FORMAT_BYTES, function.FORMAT_PICO_TIME, function.INTERNAL_TABLE_COLLATION} {
		expr := &planpb.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(id, 0)}}}}
		required, err := RequiredPersistedExpressionProtocolVersion(expr)
		require.NoError(t, err)
		if id == function.INTERNAL_TABLE_COLLATION {
			require.Equal(t, defines.MORPCVersion118, required)
		} else {
			require.Zero(t, required, "main's existing function identities retain their gates")
		}
	}
}

func TestTableCollationViewProtocol(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	rt := moruntime.ServiceRuntime(ctx.GetProcess().GetService())
	for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor} {
		old, existed := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if existed {
				rt.SetGlobalVariables(key, old)
			} else if current, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, current)
			}
		})
	}
	set := func(local, read, author int64) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, local)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, read)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, author)
	}
	build := func(sql string) (*Plan, error) {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}, stmt, false)
	}
	const authored = "create view collation_v as select internal_table_collation('') as c"
	const legacy = "create view legacy_v as select 1 as c"
	for _, tc := range []struct {
		name                string
		local, read, author int64
	}{
		{"all_113", 113, 113, 113},
		{"old_local", 113, 118, 118},
		{"native_unique_local", 114, 118, 118},
		{"calendar_interval_local", 115, 118, 118},
		{"division_warning_local", 116, 118, 118},
		{"least_greatest_local", 117, 118, 118},
		{"old_authoring_floor", 118, 118, 117},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set(tc.local, tc.read, tc.author)
			_, err := build(legacy)
			require.NoError(t, err)
			_, err = build(authored)
			require.ErrorContains(t, err, "protocol version 118")
		})
	}
	set(118, 118, 118)
	created, err := build(authored)
	require.NoError(t, err)
	payload := created.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()
	var metadata ViewData
	require.NoError(t, json.Unmarshal([]byte(payload), &metadata))
	require.NotNil(t, metadata.RequiredProtocolVersion)
	require.Equal(t, defines.MORPCVersion118, *metadata.RequiredProtocolVersion)
	for _, marked := range []bool{true, false} {
		t.Run(map[bool]string{true: "marked_reader", false: "unmarked_reader"}[marked], func(t *testing.T) {
			data := metadata
			if !marked {
				data.RequiredProtocolVersion = nil
			}
			encoded, err := json.Marshal(data)
			require.NoError(t, err)
			for _, floors := range [][2]int64{{113, 118}, {114, 118}, {115, 118}, {116, 118}, {117, 118}, {118, 117}, {118, 118}} {
				set(floors[0], floors[1], 113)
				_, err := RegenerateViewDefinition(ctx, string(encoded))
				if floors[0] < 118 || floors[1] < 118 {
					require.ErrorContains(t, err, "protocol version 118")
				} else {
					require.NoError(t, err, "read admission must not depend on the authoring floor")
				}
			}
		})
	}
	set(113, 113, 113)
	created, err = build(legacy)
	require.NoError(t, err)
	// Unmarshal into a fresh value: omitted fields must not inherit the previous view's marker.
	var legacyData ViewData
	require.NoError(t, json.Unmarshal([]byte(created.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &legacyData))
	require.Nil(t, legacyData.RequiredProtocolVersion)
}
