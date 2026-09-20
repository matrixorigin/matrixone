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
		runtime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion89)
		runtime.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor,
			int64(defines.MORPCVersion89))
		runtime.SetGlobalVariables(
			moruntime.PersistedExpressionProtocolAuthoringFloor, authoringFloor)
		ctx := &rootSQLCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false),
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

	for _, authoringFloor := range []int64{0, defines.MORPCVersion88} {
		_, err := buildView(rootSQL, authoringFloor)
		require.ErrorContains(t, err, "protocol version 89")
	}

	generated, err := buildView(rootSQL, defines.MORPCVersion89)
	require.NoError(t, err)
	require.NotNil(t, generated.GetViewSql())
	var data ViewData
	require.NoError(t, json.Unmarshal([]byte(generated.GetViewSql().GetView()), &data))
	require.NotNil(t, data.RequiredProtocolVersion)
	require.Equal(t, int64(defines.MORPCVersion89), *data.RequiredProtocolVersion)

	plain, err := buildView("create view plain as select 1", 0)
	require.NoError(t, err)
	var plainData ViewData
	require.NoError(t, json.Unmarshal([]byte(plain.GetViewSql().GetView()), &plainData))
	require.Nil(t, plainData.RequiredProtocolVersion)

	bindView := func(protocol, readFloor int64) error {
		runtime.SetGlobalVariables(moruntime.MOProtocolVersion, protocol)
		runtime.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, readFloor)
		runtime.SetGlobalVariables(
			moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion89))
		bindCtx := NewMockCompilerContext(false)
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
		{name: "read floor zero rejects", version: defines.MORPCVersion89, floor: 0, wantErr: true},
		{name: "read floor predecessor rejects", version: defines.MORPCVersion89, floor: defines.MORPCVersion88, wantErr: true},
		{name: "immediate predecessor rejects", version: defines.MORPCVersion88, floor: defines.MORPCVersion88, wantErr: true},
		{name: "current protocol accepts", version: defines.MORPCVersion89, floor: defines.MORPCVersion89},
	} {
		t.Run(test.name, func(t *testing.T) {
			bindErr := bindView(test.version, test.floor)
			if test.wantErr {
				require.ErrorContains(t, bindErr, "protocol version 89")
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
		runtime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion89)
		runtime.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor,
			int64(defines.MORPCVersion89))
		runtime.SetGlobalVariables(
			moruntime.PersistedExpressionProtocolAuthoringFloor, authoringFloor)
		ctx := &rootSQLCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false),
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
			for _, authoringFloor := range []int64{0, defines.MORPCVersion88} {
				_, err := buildView(test.sql, authoringFloor)
				require.ErrorContains(t, err, "protocol version 89")
			}

			created, err := buildView(test.sql, defines.MORPCVersion89)
			require.NoError(t, err)
			var data ViewData
			require.NoError(t, json.Unmarshal([]byte(created.GetViewSql().GetView()), &data))
			require.NotNil(t, data.RequiredProtocolVersion)
			require.Equal(t, int64(defines.MORPCVersion89), *data.RequiredProtocolVersion)
		})
	}
}
