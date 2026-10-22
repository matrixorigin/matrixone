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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestViewDefinitionFunctionsPersistAndEnforceProtocol(t *testing.T) {
	const rootSQL = "create view v as select mo_view_definition('legacy') as definition, coalesce(mo_view_check_option('none'), 'NONE') as check_option"

	runtime := moruntime.ServiceRuntime("")
	oldProtocol, hadProtocol := runtime.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := runtime.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := runtime.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			runtime.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else if current, ok := runtime.GetGlobalVariables(moruntime.MOProtocolVersion); ok {
			runtime.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, current)
		}
		if hadReadFloor {
			runtime.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := runtime.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			runtime.CompareAndDeleteGlobalVariables(
				moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			runtime.SetGlobalVariables(
				moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := runtime.GetGlobalVariables(
			moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			runtime.CompareAndDeleteGlobalVariables(
				moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	buildView := func(rootSQL string, authoringFloor int64) (*TableDef, error) {
		runtime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
		runtime.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor,
			int64(defines.MORPCVersion126))
		runtime.SetGlobalVariables(
			moruntime.PersistedExpressionProtocolAuthoringFloor, authoringFloor)
		ctx := &rootSQLCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)),
			rootSQL:             rootSQL,
		}
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		built, err := BuildPlan(ctx, stmt, false)
		if err != nil {
			return nil, err
		}
		return built.GetDdl().GetCreateView().GetTableDef(), nil
	}

	for _, authoringFloor := range []int64{0, defines.MORPCVersion104, defines.MORPCVersion105, defines.MORPCVersion106, defines.MORPCVersion108, defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion112, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion121, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion123, defines.MORPCVersion122, defines.MORPCVersion120} {
		_, err := buildView(rootSQL, authoringFloor)
		require.ErrorContains(t, err, "protocol version 126")
	}

	generated, err := buildView(rootSQL, defines.MORPCVersion126)
	require.NoError(t, err)
	require.NotNil(t, generated.GetViewSql())
	var data ViewData
	require.NoError(t, json.Unmarshal([]byte(generated.GetViewSql().GetView()), &data))
	require.NotNil(t, data.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion126), *data.RequiredProtocolVersion)

	for _, sql := range []string{
		"create view plain as select 1",
		"create view bytes_v as select format_bytes(1) as formatted",
		"create view time_v as select format_pico_time(1) as formatted",
	} {
		t.Run(sql, func(t *testing.T) {
			plain, err := buildView(sql, 0)
			require.NoError(t, err)
			var plainData ViewData
			require.NoError(t, json.Unmarshal([]byte(plain.GetViewSql().GetView()), &plainData))
			require.Nil(t, plainData.RequiredProtocolVersion)
		})
	}

	bindView := func(protocol, readFloor int64) error {
		runtime.SetGlobalVariables(moruntime.MOProtocolVersion, protocol)
		runtime.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, readFloor)
		runtime.SetGlobalVariables(
			moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion126))
		bindCtx := NewMockCompilerContext(false, newPlanTestProcess(t))
		builder := NewQueryBuilder(planpb.Query_SELECT, bindCtx, true, false)
		_, err := builder.bindView(
			NewBindContext(builder, nil),
			&TableDef{ViewSql: &planpb.ViewDef{View: generated.GetViewSql().GetView()}},
			nil,
			&ObjectRef{SchemaName: "db", ObjName: "v"},
			"db",
			"v",
			nil,
		)
		return err
	}

	for _, test := range []struct {
		name    string
		version int64
		floor   int64
		wantErr bool
	}{
		{name: "read floor zero rejects", version: defines.MORPCVersion126, floor: 0, wantErr: true},
		{name: "read floor v104 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion104, wantErr: true},
		{name: "read floor v105 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion105, wantErr: true},
		{name: "read floor v106 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion106, wantErr: true},
		{name: "read floor v108 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion108, wantErr: true},
		{name: "read floor v110 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion110, wantErr: true},
		{name: "read floor v111 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion111, wantErr: true},
		{name: "read floor v112 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion112, wantErr: true},
		{name: "read floor v113 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion113, wantErr: true},
		{name: "read floor v114 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion114, wantErr: true},
		{name: "read floor v115 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion115, wantErr: true},
		{name: "read floor v116 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion116, wantErr: true},
		{name: "read floor v117 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion117, wantErr: true},
		{name: "predecessor v104 rejects", version: defines.MORPCVersion104, floor: defines.MORPCVersion104, wantErr: true},
		{name: "predecessor v105 rejects", version: defines.MORPCVersion105, floor: defines.MORPCVersion105, wantErr: true},
		{name: "predecessor v106 rejects", version: defines.MORPCVersion106, floor: defines.MORPCVersion106, wantErr: true},
		{name: "predecessor v108 rejects", version: defines.MORPCVersion108, floor: defines.MORPCVersion108, wantErr: true},
		{name: "predecessor v109 rejects", version: defines.MORPCVersion109, floor: defines.MORPCVersion109, wantErr: true},
		{name: "predecessor v110 rejects", version: defines.MORPCVersion110, floor: defines.MORPCVersion110, wantErr: true},
		{name: "predecessor v111 rejects", version: defines.MORPCVersion111, floor: defines.MORPCVersion111, wantErr: true},
		{name: "predecessor v113 rejects", version: defines.MORPCVersion113, floor: defines.MORPCVersion113, wantErr: true},
		{name: "predecessor v114 rejects", version: defines.MORPCVersion114, floor: defines.MORPCVersion114, wantErr: true},
		{name: "predecessor v115 rejects", version: defines.MORPCVersion115, floor: defines.MORPCVersion115, wantErr: true},
		{name: "predecessor v116 rejects", version: defines.MORPCVersion116, floor: defines.MORPCVersion116, wantErr: true},
		{name: "v117 rejects", version: defines.MORPCVersion117, floor: defines.MORPCVersion117, wantErr: true},
		{name: "v118 rejects", version: defines.MORPCVersion118, floor: defines.MORPCVersion118, wantErr: true},
		{name: "v119 rejects", version: defines.MORPCVersion119, floor: defines.MORPCVersion119, wantErr: true},
		{name: "predecessor v120 rejects", version: defines.MORPCVersion120, floor: defines.MORPCVersion120, wantErr: true},
		{name: "read floor v120 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion120, wantErr: true},
		{name: "read floor v119 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion119, wantErr: true},
		{name: "read floor v118 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion118, wantErr: true},
		{name: "v121 rejects", version: defines.MORPCVersion121, floor: defines.MORPCVersion121, wantErr: true},
		{name: "read floor v121 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion121, wantErr: true},
		{name: "predecessor v122 rejects", version: defines.MORPCVersion122, floor: defines.MORPCVersion122, wantErr: true},
		{name: "read floor v122 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion122, wantErr: true},
		{name: "predecessor v123 rejects", version: defines.MORPCVersion123, floor: defines.MORPCVersion123, wantErr: true},
		{name: "read floor v123 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion123, wantErr: true},
		{name: "predecessor v124 rejects", version: defines.MORPCVersion124, floor: defines.MORPCVersion124, wantErr: true},
		{name: "immediate predecessor v125 rejects", version: defines.MORPCVersion125, floor: defines.MORPCVersion125, wantErr: true},
		{name: "read floor v125 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion125, wantErr: true},
		{name: "read floor v124 rejects", version: defines.MORPCVersion126, floor: defines.MORPCVersion124, wantErr: true},
		{name: "current protocol accepts", version: defines.MORPCVersion126, floor: defines.MORPCVersion126},
	} {
		t.Run(test.name, func(t *testing.T) {
			bindErr := bindView(test.version, test.floor)
			if test.wantErr {
				require.ErrorContains(t, bindErr, "protocol version 126")
			} else {
				require.NoError(t, bindErr)
			}
		})
	}
}

func TestViewDefinitionProtocolSurvivesConstantFolding(t *testing.T) {
	runtime := moruntime.ServiceRuntime("")
	oldProtocol, hadProtocol := runtime.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := runtime.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := runtime.GetGlobalVariables(
		moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			runtime.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else if current, ok := runtime.GetGlobalVariables(moruntime.MOProtocolVersion); ok {
			runtime.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, current)
		}
		if hadReadFloor {
			runtime.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := runtime.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			runtime.CompareAndDeleteGlobalVariables(
				moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			runtime.SetGlobalVariables(
				moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := runtime.GetGlobalVariables(
			moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			runtime.CompareAndDeleteGlobalVariables(
				moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	buildView := func(rootSQL string, authoringFloor int64) (*TableDef, error) {
		runtime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
		runtime.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor,
			int64(defines.MORPCVersion126))
		runtime.SetGlobalVariables(
			moruntime.PersistedExpressionProtocolAuthoringFloor, authoringFloor)
		ctx := &rootSQLCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)),
			rootSQL:             rootSQL,
		}
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		built, err := BuildPlan(ctx, stmt, false)
		if err != nil {
			return nil, err
		}
		return built.GetDdl().GetCreateView().GetTableDef(), nil
	}

	cases := []struct {
		name string
		sql  string
	}{
		{
			name: "between",
			sql:  `create view v_view_definition_between as select 5 between length(mo_view_check_option('{"Stmt":"CREATE VIEW old AS SELECT 1"}')) and 10 as folded`,
		},
		{
			name: "arithmetic",
			sql:  `create view v_view_definition_arithmetic as select length(mo_view_check_option('{"Stmt":"CREATE VIEW old AS SELECT 1"}')) + 1 as folded`,
		},
		{
			name: "cast",
			sql:  `create view v_view_definition_cast as select cast(mo_view_check_option('{"Stmt":"CREATE VIEW old AS SELECT 1"}') as char) as folded`,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			for _, authoringFloor := range []int64{0, defines.MORPCVersion104, defines.MORPCVersion105, defines.MORPCVersion106, defines.MORPCVersion108, defines.MORPCVersion109, defines.MORPCVersion110, defines.MORPCVersion111, defines.MORPCVersion112, defines.MORPCVersion113, defines.MORPCVersion114, defines.MORPCVersion115, defines.MORPCVersion116, defines.MORPCVersion117, defines.MORPCVersion118, defines.MORPCVersion119, defines.MORPCVersion121, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion123, defines.MORPCVersion122, defines.MORPCVersion120} {
				_, err := buildView(test.sql, authoringFloor)
				require.ErrorContains(t, err, "protocol version 126")
			}

			created, err := buildView(test.sql, defines.MORPCVersion126)
			require.NoError(t, err)
			var data ViewData
			require.NoError(t, json.Unmarshal([]byte(created.GetViewSql().GetView()), &data))
			require.NotNil(t, data.RequiredProtocolVersion)
			require.Equal(t, int64(defines.MORPCVersion126), *data.RequiredProtocolVersion)
		})
	}
}
