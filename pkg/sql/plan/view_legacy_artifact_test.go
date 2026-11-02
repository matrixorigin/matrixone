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
	"os"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestRealLegacySystemTemplateFailsClosedAfterMigration(t *testing.T) {
	data, err := os.ReadFile("testdata/view_metadata_legacy_v4_2_6.json")
	require.NoError(t, err)
	var fixture struct {
		Columns string `json:"system_columns_definition"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	var historical ViewData
	require.NoError(t, json.Unmarshal([]byte(fixture.Columns), &historical))
	require.NotEmpty(t, historical.Stmt)
	_, recognized := sysview.ViewMetadataColumnsDDL(historical.Stmt)
	require.False(t, recognized, "this protocol-10 template is not a qualified V41/V46/V58 template")
	for _, migrated := range []bool{false, true} {
		current := historical.Stmt
		if migrated {
			current = sysview.InformationSchemaColumnsDDL
		}
		mock := NewMockCompilerContext(true, newPlanTestProcess(t))
		ctx := &historicalColumnsContext{MockCompilerContext: mock}
		ctx.resolve = func(database, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
			require.Nil(t, snapshot)
			definition, err := json.Marshal(ViewData{Stmt: current})
			require.NoError(t, err)
			return &ObjectRef{SchemaName: database, ObjName: name, Obj: 99},
				&TableDef{TblId: 99, ViewSql: &planpb.ViewDef{View: string(definition)}}, nil
		}
		builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
		adapted, err := builder.historicalViewColumnsSQL("information_schema", "columns", historical.Stmt,
			&Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}})
		if migrated {
			require.ErrorContains(t, err, "unsupported historical information_schema.COLUMNS definition")
			require.Empty(t, adapted)
		} else {
			require.NoError(t, err)
			require.Equal(t, historical.Stmt, adapted)
		}
	}
}

func TestViewSchemaRealLegacyDefinitionsRemainUnqualifiedAfterRestore(t *testing.T) {
	data, err := os.ReadFile("testdata/view_metadata_legacy_v4_2_6.json")
	require.NoError(t, err)
	var fixture struct {
		Views []struct{ Name, Definition string }
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.Len(t, fixture.Views, 2)
	for _, view := range fixture.Views {
		t.Run(view.Name, func(t *testing.T) {
			var original ViewData
			require.NoError(t, json.Unmarshal([]byte(view.Definition), &original))
			require.NotNil(t, original.SQLMode)
			require.Nil(t, original.LowerCaseTableNames)
			ctx := WithRestoredViewDefinition(t.Context(), "tpch", view.Name, view.Definition, original.Stmt)
			restored, err := preserveRestoredViewDefinition(ctx, "tpch", view.Name,
				`{"Stmt":"create view rebound as select n_name from nation","DefaultDatabase":"tpch","sql_mode":"","lower_case_table_names":1}`)
			require.NoError(t, err)
			for _, definition := range []string{view.Definition, restored} {
				f := newViewSchemaTestFixture(t)
				def := f.addDefinition(t, view.Name, original.Stmt)
				def.ViewSql.View = definition
				f.addView(t, "qualified_v", "select n_name from nation")
				for _, disabled := range []bool{false, true} {
					r := f.request(t)
					r.memoDisabled = disabled
					for range 2 {
						result, err := r.Describe("tpch", view.Name, nil)
						result.Release()
						require.Nil(t, result)
						require.ErrorContains(t, err, "LEGACY_CONTEXT_UNAVAILABLE")
					}
					// A rejected imported definition cannot poison the request.
					result, err := r.Describe("tpch", "qualified_v", nil)
					result.Release()
					require.NoError(t, err)
					require.NotNil(t, result)
					r.Close()
				}
			}
		})
	}
}
