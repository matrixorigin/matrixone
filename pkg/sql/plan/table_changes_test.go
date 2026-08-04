// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedTableChangesRecordsSourceSchemaDependency(t *testing.T) {
	mock := NewMockCompilerContext(false)
	mock.objects["source"] = &pbplan.ObjectRef{
		Db: 10, Obj: 20, SchemaName: "db", ObjName: "source",
	}
	mock.tables["source"] = &pbplan.TableDef{
		Name: "source", DbName: "db", DbId: 10, TblId: 20, Version: 7,
		TableType: catalog.SystemOrdinaryRel,
		Cols: []*pbplan.ColDef{{
			Name: "id", Typ: pbplan.Type{Id: int32(types.T_int64)}, Primary: true,
		}},
		Pkey: &pbplan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
	}
	builder := NewQueryBuilder(pbplan.Query_SELECT, mock, true, true)

	_, err := builder.buildTableChanges(
		nil,
		NewBindContext(builder, nil),
		[]*pbplan.Expr{
			makePlan2StringConstExprWithType("db"),
			makePlan2StringConstExprWithType("source"),
			nil,
			nil,
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, builder.qry.CatalogDependencies, 1)
	require.Equal(t, int64(10), builder.qry.CatalogDependencies[0].Db)
	require.Equal(t, int64(20), builder.qry.CatalogDependencies[0].Obj)
	require.Equal(t, int64(7), builder.qry.CatalogDependencies[0].Server)

	schemas, _, err := ResetPreparePlan(mock, &pbplan.Plan{
		Plan: &pbplan.Plan_Query{Query: builder.qry},
	})
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	require.Equal(t, builder.qry.CatalogDependencies[0], schemas[0])
}

func TestValidateTableChangesSourceTemporaryTable(t *testing.T) {
	err := validateTableChangesSource(nil, &pbplan.TableDef{
		TableType:   catalog.SystemTemporaryTable,
		IsTemporary: true,
	})
	require.EqualError(t, err, "not supported: table_changes does not support temporary tables")
}

func TestValidateTableChangesSourceRejectsMetadataColumnNames(t *testing.T) {
	tests := []struct {
		name string
		typ  types.T
	}{
		{name: catalog.TableChangesAttrChangeType, typ: types.T_int64},
		{name: catalog.TableChangesAttrCommitTS, typ: types.T_bool},
		{name: catalog.TableChangesAttrTableID, typ: types.T_varchar},
		{name: catalog.TableChangesAttrSchemaVersion, typ: types.T_json},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTableChangesColumnNames(&pbplan.TableDef{Cols: []*pbplan.ColDef{{
				Name: tt.name,
				Typ:  pbplan.Type{Id: int32(tt.typ)},
			}}})
			require.EqualError(t, err,
				"invalid input: table_changes source column \""+tt.name+"\" conflicts with reserved metadata column")
		})
	}
}
