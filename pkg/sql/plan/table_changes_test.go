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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBuildTableFunctionIncrementalDiscoveryDispatch(t *testing.T) {
	for _, name := range []string{"change_watermark", "table_changes"} {
		t.Run(name, func(t *testing.T) {
			builder := NewQueryBuilder(pbplan.Query_SELECT, NewMockCompilerContext(false), false, true)
			ctx := NewBindContext(builder, nil)
			fn := tree.FuncName2ResolvableFunctionReference(
				tree.NewUnresolvedName(tree.NewCStr(name, 0)),
			)
			tbl := &tree.TableFunction{Func: &tree.FuncExpr{
				Func: fn,
				Type: tree.FUNC_TYPE_TABLE,
			}}
			nodeID, err := builder.buildTableFunction(tbl, ctx, nil)
			if name == "table_changes" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, name, builder.qry.Nodes[nodeID].TableDef.TblFunc.Name)
		})
	}
}

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

func TestBuildChangeWatermark(t *testing.T) {
	builder := NewQueryBuilder(pbplan.Query_SELECT, NewMockCompilerContext(false), false, true)
	ctx := NewBindContext(builder, nil)
	nodeID, err := builder.buildChangeWatermark(nil, ctx, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int32(0), nodeID)
	require.Equal(t, "change_watermark", builder.qry.Nodes[nodeID].TableDef.TblFunc.Name)

	_, err = builder.buildChangeWatermark(
		nil, ctx, []*pbplan.Expr{makePlan2StringConstExprWithType("unexpected")}, nil,
	)
	require.ErrorContains(t, err, "invalid input args length")
}

func TestValidateTableChangesSourceContracts(t *testing.T) {
	valid := func() *pbplan.TableDef {
		return &pbplan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Pkey:      &pbplan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		}
	}
	require.EqualError(t, validateTableChangesSource(nil, nil),
		"invalid input: table_changes source table does not exist")
	require.EqualError(t, validateTableChangesSource(
		&pbplan.ObjectRef{PubInfo: &pbplan.PubInfo{}}, valid()),
		"not supported: table_changes does not support subscription tables")

	unsupported := valid()
	unsupported.TableType = catalog.SystemViewRel
	require.ErrorContains(t, validateTableChangesSource(nil, unsupported),
		"table_changes does not support table type")
	partitioned := valid()
	partitioned.Partition = &pbplan.Partition{}
	require.EqualError(t, validateTableChangesSource(nil, partitioned),
		"not supported: table_changes does not support partitioned tables")

	withoutPK := valid()
	withoutPK.Pkey = nil
	require.EqualError(t, validateTableChangesSource(nil, withoutPK),
		"not supported: table_changes requires an explicit primary key")
	cluster := valid()
	cluster.TableType = catalog.SystemClusterRel
	require.EqualError(t, validateTableChangesSource(nil, cluster),
		"not supported: table_changes requires cluster table primary keys to include account_id")
	cluster.Pkey.Names = append(cluster.Pkey.Names, "ACCOUNT_ID")
	require.NoError(t, validateTableChangesSource(nil, cluster))
	require.True(t, containsChangeKey(cluster.Pkey.Names, "account_id"))
	require.False(t, containsChangeKey(cluster.Pkey.Names, "missing"))
}

func TestTableChangesStringLiteral(t *testing.T) {
	value, ok := stringLiteral(makePlan2StringConstExprWithType("source"))
	require.True(t, ok)
	require.Equal(t, "source", value)
	_, ok = stringLiteral(nil)
	require.False(t, ok)
	_, ok = stringLiteral(makePlan2Int64ConstExprWithType(1))
	require.False(t, ok)
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
