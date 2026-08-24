// Copyright 2025 Matrix Origin
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
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

func Test_runSql(t *testing.T) {
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()

	ctx := context.Background()
	proc.Ctx = context.Background()
	proc.ReplaceTopCtx(ctx)

	compilerContext := NewMockCompilerContext2(ctrl)
	compilerContext.EXPECT().GetProcess().Return(proc).AnyTimes()

	_, err := runSql(compilerContext, "")
	require.Error(t, err, "internal error: no account id in context")
}

func TestGetSqlForFkReferredToEscapesStringLiterals(t *testing.T) {
	sql := GetSqlForFkReferredTo("db\\name", "quote'src")
	require.Contains(t, sql, "refer_db_name = 'db\\\\name'")
	require.Contains(t, sql, "refer_table_name = 'quote\\'src'")
	require.Contains(t, sql, "table_name != 'quote\\'src'")
}

func TestForeignKeyCatalogLayoutIsExtendedOnlyAfterAllColumnsExist(t *testing.T) {
	ctx := NewEmptyCompilerContext()
	ctx.tables[catalog.MOForeignKeys] = &TableDef{
		Name: catalog.MOForeignKeys,
		Cols: []*ColDef{{Name: "referenced_index_name"}, {Name: "on_delete_origin"}},
	}
	layout, err := resolveForeignKeyCatalogLayout(ctx)
	require.NoError(t, err)
	require.Equal(t, foreignKeyCatalogLegacy, layout)

	ctx.tables[catalog.MOForeignKeys].Cols = append(ctx.tables[catalog.MOForeignKeys].Cols,
		&ColDef{Name: "on_update_origin"})
	layout, err = resolveForeignKeyCatalogLayout(ctx)
	require.NoError(t, err)
	require.Equal(t, foreignKeyCatalogExtended, layout)
}

func TestLegacyForeignKeyCatalogSQLAvoidsNewColumns(t *testing.T) {
	readSQL := getSqlForFkReferredToWithCatalogLayout("parent_db", "parent", foreignKeyCatalogLegacy)
	require.NotContains(t, readSQL, "referenced_index_name")
	require.NotContains(t, readSQL, "on_delete_origin")
	require.Contains(t, readSQL, "on_update from `mo_catalog`.`mo_foreign_keys`")

	fkData := &FkData{
		Def: &plan.ForeignKeyDef{
			Name:           "fk_child_parent",
			OnDelete:       plan.ForeignKeyDef_RESTRICT,
			OnUpdate:       plan.ForeignKeyDef_RESTRICT,
			OnDeleteOrigin: plan.ForeignKeyDef_ACTION_ORIGIN_EXPLICIT,
			OnUpdateOrigin: plan.ForeignKeyDef_ACTION_ORIGIN_EXPLICIT,
		},
		Cols:            &plan.FkColName{Cols: []string{"parent_id"}},
		ColsReferred:    &plan.FkColName{Cols: []string{"id"}},
		ParentDbName:    "parent_db",
		ParentTableName: "parent",
	}
	insertSQL := getSqlForAddFkWithCatalogLayout("child_db", "child", fkData, foreignKeyCatalogLegacy)
	require.NotContains(t, insertSQL, "referenced_index_name")
	require.NotContains(t, insertSQL, "on_delete_origin")
	require.Contains(t, insertSQL, "on_delete, on_update) values")
}

func TestLegacyForeignKeyActionOriginIsConservative(t *testing.T) {
	require.Equal(t, plan.ForeignKeyDef_ACTION_ORIGIN_LEGACY_AMBIGUOUS.String(),
		legacyForeignKeyActionOrigin("RESTRICT"))
	require.Equal(t, plan.ForeignKeyDef_ACTION_ORIGIN_LEGACY_AMBIGUOUS.String(),
		legacyForeignKeyActionOrigin("NO ACTION"))
	require.Equal(t, plan.ForeignKeyDef_ACTION_ORIGIN_EXPLICIT.String(),
		legacyForeignKeyActionOrigin("CASCADE"))
}

func TestGetSqlForAddFkEscapesStringLiterals(t *testing.T) {
	fkData := &FkData{
		Def: &plan.ForeignKeyDef{
			Name:     "fk'child",
			OnDelete: plan.ForeignKeyDef_CASCADE,
			OnUpdate: plan.ForeignKeyDef_RESTRICT,
		},
		Cols:            &plan.FkColName{Cols: []string{"child'col"}},
		ColsReferred:    &plan.FkColName{Cols: []string{"parent'col"}},
		ParentDbName:    "parent\\db",
		ParentTableName: "parent'table",
	}

	sql := getSqlForAddFk("child'db", "child\\table", fkData)
	require.Contains(t, sql, "'fk\\'child'")
	require.Contains(t, sql, "'child\\'db'")
	require.Contains(t, sql, "'child\\\\table'")
	require.Contains(t, sql, "'child\\'col'")
	require.Contains(t, sql, "'parent\\\\db'")
	require.Contains(t, sql, "'parent\\'table'")
	require.Contains(t, sql, "'parent\\'col'")
}

func TestGetSqlForAddFkRecordsCompositeColumnOrder(t *testing.T) {
	fkData := &FkData{
		Def:  &plan.ForeignKeyDef{Name: "fk_child_parent"},
		Cols: &plan.FkColName{Cols: []string{"child_first", "child_second"}},
		ColsReferred: &plan.FkColName{Cols: []string{
			"parent_first", "parent_second",
		}},
		ParentDbName:    "parent_db",
		ParentTableName: "parent",
	}

	sql := getSqlForAddFk("child_db", "child", fkData)
	require.Contains(t, sql, "('fk_child_parent','1','child_db','0','child','0','child_first'")
	require.Contains(t, sql, "('fk_child_parent','2','child_db','0','child','0','child_second'")
	require.Contains(t, GetSqlForFkReferredTo("parent_db", "parent"),
		"order by db_name, table_name, constraint_name, constraint_id")
}

func TestGetSqlForAddFkStoresDefaultActionsAsNoAction(t *testing.T) {
	fkData := &FkData{
		Def: &plan.ForeignKeyDef{
			Name:           "fk_default_action",
			OnDelete:       plan.ForeignKeyDef_RESTRICT,
			OnUpdate:       plan.ForeignKeyDef_RESTRICT,
			OnDeleteOrigin: plan.ForeignKeyDef_ACTION_ORIGIN_DEFAULT,
			OnUpdateOrigin: plan.ForeignKeyDef_ACTION_ORIGIN_DEFAULT,
		},
		Cols:            &plan.FkColName{Cols: []string{"child_id"}},
		ColsReferred:    &plan.FkColName{Cols: []string{"parent_id"}},
		ParentDbName:    "parent_db",
		ParentTableName: "parent",
	}

	sql := getSqlForAddFk("child_db", "child", fkData)
	require.Contains(t, sql, "'NO_ACTION','NO_ACTION'")
	require.Contains(t, sql, "'ACTION_ORIGIN_DEFAULT','ACTION_ORIGIN_DEFAULT'")
}

func TestGetSqlForUpdateFkReferencedIndexEscapesCatalogValues(t *testing.T) {
	require.Equal(t,
		"update `mo_catalog`.`mo_foreign_keys` set referenced_index_name = 'uk\\'parent' where db_name = 'child\\'db' and table_name = 'child\\\\table' and constraint_name = 'fk\\'child'",
		getSqlForUpdateFkReferencedIndex("child'db", `child\table`, "fk'child", "uk'parent"),
	)
}

func TestGetSqlForCheckHasDBRefersToEscapesStringLiterals(t *testing.T) {
	sql := getSqlForCheckHasDBRefersTo("db'name")
	require.Contains(t, sql, "refer_db_name = 'db\\'name'")
	require.Contains(t, sql, "db_name != 'db\\'name'")
}

func TestGetSqlForTransferAlterCopyFk(t *testing.T) {
	prepare, finalize := GetSqlForTransferAlterCopyFk(
		"db'1",
		"source'child",
		"copy'child",
	)

	require.Equal(t, []string{
		"delete from `mo_catalog`.`mo_foreign_keys` where db_name = 'db\\'1' and table_name = 'copy\\'child'",
		"update `mo_catalog`.`mo_foreign_keys` set table_name = 'copy\\'child' where db_name = 'db\\'1' and table_name = 'source\\'child'",
	}, prepare)
	require.Equal(t, []string{
		"update `mo_catalog`.`mo_foreign_keys` set table_name = 'source\\'child' where db_name = 'db\\'1' and table_name = 'copy\\'child'",
	}, finalize)
}

func TestGetSqlForAddFkEscapesCatalogValues(t *testing.T) {
	fk := &FkData{
		ParentDbName:    `parent\db'name`,
		ParentTableName: `parent\table'name`,
		Cols: &plan.FkColName{Cols: []string{
			`child\col'name`,
			`child_col_two`,
		}},
		ColsReferred: &plan.FkColName{Cols: []string{
			`parent\col'name`,
			`parent_col_two`,
		}},
		Def: &plan.ForeignKeyDef{
			Name:     `fk\name'one`,
			OnDelete: plan.ForeignKeyDef_CASCADE,
			OnUpdate: plan.ForeignKeyDef_RESTRICT,
		},
	}

	require.Equal(t,
		"insert into `mo_catalog`.`mo_foreign_keys` (constraint_name, constraint_id, db_name, db_id, table_name, table_id, column_name, column_id, refer_db_name, refer_db_id, refer_table_name, refer_table_id, refer_column_name, refer_column_id, on_delete, on_update, referenced_index_name, on_delete_origin, on_update_origin) values "+
			"('fk\\\\name\\'one','1','child\\'db\\\\part','0','child\\\\table\\'name','0','child\\\\col\\'name','0','parent\\\\db\\'name','0','parent\\\\table\\'name','0','parent\\\\col\\'name','0','CASCADE','RESTRICT','','ACTION_ORIGIN_EXPLICIT','ACTION_ORIGIN_EXPLICIT'),"+
			"('fk\\\\name\\'one','2','child\\'db\\\\part','0','child\\\\table\\'name','0','child_col_two','0','parent\\\\db\\'name','0','parent\\\\table\\'name','0','parent_col_two','0','CASCADE','RESTRICT','','ACTION_ORIGIN_EXPLICIT','ACTION_ORIGIN_EXPLICIT')",
		getSqlForAddFk(`child'db\part`, `child\table'name`, fk),
	)
}

func TestFkCatalogMutationSqlEscapesIdentifiers(t *testing.T) {
	const (
		db         = `db'name\part`
		table      = `table'name\part`
		oldName    = `old'name\part`
		newName    = `new'name\part`
		constraint = `fk'name\part`
	)

	require.Equal(t,
		"delete from `mo_catalog`.`mo_foreign_keys` where db_name = 'db\\'name\\\\part' and table_name = 'table\\'name\\\\part'",
		getSqlForDeleteTable(db, table))
	require.Equal(t,
		"delete from `mo_catalog`.`mo_foreign_keys` where constraint_name = 'fk\\'name\\\\part' and db_name = 'db\\'name\\\\part' and table_name = 'table\\'name\\\\part'",
		getSqlForDeleteConstraint(db, table, constraint))
	require.Equal(t,
		"delete from `mo_catalog`.`mo_foreign_keys` where db_name = 'db\\'name\\\\part'",
		getSqlForDeleteDB(db))
	require.Equal(t, []string{
		"update `mo_catalog`.`mo_foreign_keys` set table_name = 'new\\'name\\\\part' where db_name = 'db\\'name\\\\part' and table_name = 'old\\'name\\\\part' ; ",
		"update `mo_catalog`.`mo_foreign_keys` set refer_table_name = 'new\\'name\\\\part' where refer_db_name = 'db\\'name\\\\part' and refer_table_name = 'old\\'name\\\\part' ; ",
	}, getSqlForRenameTable(db, oldName, newName))
	require.Equal(t, []string{
		"update `mo_catalog`.`mo_foreign_keys` set column_name = 'new\\'name\\\\part' where db_name = 'db\\'name\\\\part' and table_name = 'table\\'name\\\\part' and column_name = 'old\\'name\\\\part' ; ",
		"update `mo_catalog`.`mo_foreign_keys` set refer_column_name = 'new\\'name\\\\part' where refer_db_name = 'db\\'name\\\\part' and refer_table_name = 'table\\'name\\\\part' and refer_column_name = 'old\\'name\\\\part' ; ",
	}, getSqlForRenameColumn(db, table, oldName, newName))
	require.Equal(t,
		"select count(*) > 0 from `mo_catalog`.`mo_foreign_keys` where refer_db_name = 'db\\'name\\\\part' and db_name != 'db\\'name\\\\part';",
		getSqlForCheckHasDBRefersTo(db))
}

func Test_buildPreDeleteFullTextIndexAsync(t *testing.T) {
	{
		//invalid json
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":1}`,
		}

		err := buildPreDeleteFullTextIndex(nil, nil, nil, nil, idxdef, 0, nil, nil)
		require.NotNil(t, err)
	}

	{

		// async true
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":"true"}`,
		}

		err := buildPreDeleteFullTextIndex(nil, nil, nil, nil, idxdef, 0, nil, nil)
		require.Nil(t, err)
	}

}

// Test WITH clause support for INSERT statement (Issue #22583)
func TestBuildWithInsert(t *testing.T) {
	sqls := []string{
		"WITH cte AS (SELECT * FROM t1 WHERE id = 1) INSERT INTO t1 SELECT id + 10, name, value FROM cte",
		"WITH cte AS (SELECT id, name FROM t1) INSERT INTO t1 (id, name, value) SELECT id + 20, name, 100 FROM cte",
		"WITH cte1 AS (SELECT * FROM t1), cte2 AS (SELECT * FROM cte1 WHERE id > 5) INSERT INTO t1 SELECT id + 30, name, value FROM cte2",
	}

	for _, sql := range sqls {
		t.Run(sql, func(t *testing.T) {
			// Just verify the SQL can be parsed and the WITH clause is present
			stmts, err := mysql.Parse(context.TODO(), sql, 1)
			require.NoError(t, err)
			require.Equal(t, 1, len(stmts))

			ins, ok := stmts[0].(*tree.Insert)
			require.True(t, ok)
			require.NotNil(t, ins.With, "INSERT.With should not be nil")
			require.Greater(t, len(ins.With.CTEs), 0, "WITH clause should have at least one CTE")
		})
	}
}

func TestMakeInsertValueConstExprGeometry(t *testing.T) {
	proc := testutil.NewProcess(t)
	colType := types.T_geometry.ToType()
	numVal := tree.NewNumVal("POINT(1 1)", "POINT(1 1)", false, tree.P_char)

	expr, err := MakeInsertValueConstExpr(proc, numVal, &colType, false)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_geometry), expr.Typ.Id)

	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "cast", fn.Func.ObjName)
	require.Equal(t, int32(types.T_varchar), fn.Args[0].Typ.Id)
	require.Equal(t, "POINT(1 1)", fn.Args[0].GetLit().GetSval())
	require.Equal(t, int32(types.T_geometry), fn.Args[1].Typ.Id)
}

func TestMakeInsertValueConstExprBinaryHexPadding(t *testing.T) {
	proc := testutil.NewProcess(t)

	testCases := []struct {
		name        string
		literal     string
		colType     types.Type
		expected    []byte
		expectError bool
	}{
		{
			name:     "binary pads to declared width",
			literal:  "0x4142",
			colType:  types.New(types.T_binary, 4, 0),
			expected: []byte{0x41, 0x42, 0x00, 0x00},
		},
		{
			name:        "binary rejects decoded value over declared width",
			literal:     "0x4142",
			colType:     types.New(types.T_binary, 1, 0),
			expectError: true,
		},
		{
			name:        "varbinary counts decoded bytes instead of runes",
			literal:     "0xC3A9",
			colType:     types.New(types.T_varbinary, 1, 0),
			expectError: true,
		},
		{
			name:     "binary accepts odd digit hex literal",
			literal:  "0x1",
			colType:  types.New(types.T_binary, 2, 0),
			expected: []byte{0x01, 0x00},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			numVal := tree.NewNumVal(tc.literal, tc.literal, false, tree.P_hexnum)
			expr, err := MakeInsertValueConstExpr(proc, numVal, &tc.colType, false)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, string(tc.expected), expr.GetLit().GetSval())
		})
	}
}

func TestMakeInsertValueConstExprBitIgnoreTruncates(t *testing.T) {
	proc := testutil.NewProcess(t)
	colType := types.New(types.T_bit, 4, 0)
	numVal := tree.NewNumVal("0b11111", "0b11111", false, tree.P_bit)

	_, err := MakeInsertValueConstExpr(proc, numVal, &colType, false)
	require.Error(t, err)

	expr, err := MakeInsertValueConstExpr(proc, numVal, &colType, true)
	require.NoError(t, err)
	require.Equal(t, uint64(15), expr.GetLit().GetU64Val())
}

func TestMakeInsertIgnoreMySQLSpecialTypeConstExpr(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name     string
		target   plan.Type
		value    *tree.NumVal
		wantType types.T
		wantEnum uint32
		wantSet  uint64
	}{
		{
			name:     "invalid enum becomes error member",
			target:   plan.Type{Id: int32(types.T_enum), Enumvalues: "a,b,"},
			value:    tree.NewNumVal("bad", "bad", false, tree.P_char),
			wantType: types.T_enum,
		},
		{
			name:     "invalid set member is dropped",
			target:   plan.Type{Id: int32(types.T_uint64), Enumvalues: "x,y,z"},
			value:    tree.NewNumVal("x,bad", "x,bad", false, tree.P_char),
			wantType: types.T_uint64,
			wantSet:  1,
		},
		{
			name:     "invalid year becomes zero",
			target:   plan.Type{Id: int32(types.T_year)},
			value:    tree.NewNumVal(int64(2156), "2156", false, tree.P_int64),
			wantType: types.T_year,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, handled, err := makeInsertIgnoreMySQLSpecialTypeConstExpr(ctx, tt.value, tt.target)
			require.NoError(t, err)
			require.True(t, handled)
			require.Equal(t, int32(tt.wantType), expr.Typ.Id)
			if tt.wantType == types.T_enum {
				require.Equal(t, tt.wantEnum, expr.GetLit().GetEnumVal())
			}
			if tt.wantType == types.T_uint64 {
				require.Equal(t, tt.wantSet, expr.GetLit().GetU64Val())
			}
		})
	}
}

func TestAppendIndexPrefixProjection(t *testing.T) {
	newBuilder := func(t *testing.T) (*QueryBuilder, *BindContext, int32) {
		t.Helper()

		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		lastNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT,
			ProjectList: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{Name: "id", ColPos: 0},
					},
				},
				{
					Typ: plan.Type{Id: int32(types.T_text), Width: types.MaxVarcharLen},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{Name: "body", ColPos: 1},
					},
				},
			},
		}, bindCtx)
		return builder, bindCtx, lastNodeID
	}

	t.Run("empty prefix lengths keeps original projection", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)
		useColumns := []int32{1}

		gotNodeID, gotUseColumns, err := appendIndexPrefixProjection(
			builder,
			bindCtx,
			&plan.TableDef{Name: "t"},
			lastNodeID,
			[]string{"body"},
			map[string]int{"body": 1},
			useColumns,
			nil,
		)

		require.NoError(t, err)
		require.Equal(t, lastNodeID, gotNodeID)
		require.Equal(t, useColumns, gotUseColumns)
		require.Len(t, builder.qry.Nodes, 1)
	})

	t.Run("prefix key appends substring projection", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)

		gotNodeID, gotUseColumns, err := appendIndexPrefixProjection(
			builder,
			bindCtx,
			&plan.TableDef{Name: "t"},
			lastNodeID,
			[]string{"body"},
			map[string]int{"id": 0, "body": 1},
			[]int32{1},
			map[string]int{"body": 8},
		)

		require.NoError(t, err)
		require.NotEqual(t, lastNodeID, gotNodeID)
		require.Equal(t, []int32{2}, gotUseColumns)
		require.Len(t, builder.qry.Nodes, 2)

		projectNode := builder.qry.Nodes[gotNodeID]
		require.Equal(t, plan.Node_PROJECT, projectNode.NodeType)
		require.Equal(t, []int32{lastNodeID}, projectNode.Children)
		require.Len(t, projectNode.ProjectList, 3)

		prefixExpr := projectNode.ProjectList[2]
		require.Equal(t, int32(types.T_varchar), prefixExpr.Typ.Id)
		require.Equal(t, int32(types.MaxVarcharLen), prefixExpr.Typ.Width)

		castFn := prefixExpr.GetF()
		require.NotNil(t, castFn)
		require.Equal(t, "cast", castFn.Func.ObjName)
		require.Len(t, castFn.Args, 2)

		substringFn := castFn.Args[0].GetF()
		require.NotNil(t, substringFn)
		require.Equal(t, "substring", substringFn.Func.ObjName)
		require.Len(t, substringFn.Args, 3)
		require.Equal(t, "body", substringFn.Args[0].GetCol().Name)
		require.Equal(t, int64(1), substringFn.Args[1].GetLit().GetI64Val())
		require.Equal(t, int64(8), substringFn.Args[2].GetLit().GetI64Val())
	})

	t.Run("missing and non-positive prefix parts do not append projection", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)
		useColumns := []int32{0}

		gotNodeID, gotUseColumns, err := appendIndexPrefixProjection(
			builder,
			bindCtx,
			&plan.TableDef{Name: "t"},
			lastNodeID,
			[]string{"missing", "id"},
			map[string]int{"id": 0, "body": 1},
			useColumns,
			map[string]int{"missing": 4, "id": 0},
		)

		require.NoError(t, err)
		require.Equal(t, lastNodeID, gotNodeID)
		require.Equal(t, useColumns, gotUseColumns)
		require.Len(t, builder.qry.Nodes, 1)
	})
}

func TestAppendDeleteIndexTablePlanUsesPrefixLookupKey(t *testing.T) {
	newBuilder := func(t *testing.T) (*QueryBuilder, *BindContext, int32) {
		t.Helper()

		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		lastNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT,
			Stats:    &plan.Stats{Selectivity: 1, Outcnt: 1, Cost: 1, TableCnt: 1},
			ProjectList: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{Name: "id", ColPos: 0},
					},
				},
				{
					Typ: plan.Type{Id: int32(types.T_text), Width: types.MaxVarcharLen},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{Name: "body", ColPos: 1},
					},
				},
				{
					Typ: plan.Type{Id: int32(types.T_varchar), Width: 32},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{Name: "tenant", ColPos: 2},
					},
				},
			},
		}, bindCtx)
		return builder, bindCtx, lastNodeID
	}

	indexTableDef := &plan.TableDef{
		Name: "idx_body",
		Cols: []*plan.ColDef{
			{
				Name: catalog.Row_ID,
				Typ:  plan.Type{Id: int32(types.T_Rowid), Width: 16},
			},
			{
				Name: catalog.IndexTableIndexColName,
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
			},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.IndexTableIndexColName},
	}
	typMap := map[string]plan.Type{
		"id":     {Id: int32(types.T_int64)},
		"body":   {Id: int32(types.T_text), Width: types.MaxVarcharLen},
		"tenant": {Id: int32(types.T_varchar), Width: 32},
	}
	posMap := map[string]int{
		"id":     0,
		"body":   1,
		"tenant": 2,
	}

	extractJoinNode := func(t *testing.T, builder *QueryBuilder, nodeID int32) *plan.Node {
		t.Helper()
		output := builder.qry.Nodes[nodeID]
		require.Equal(t, plan.Node_PROJECT, output.NodeType)
		require.Len(t, output.BindingTags, 1)
		require.Len(t, output.Children, 1)
		return builder.qry.Nodes[output.Children[0]]
	}
	extractLookupExpr := func(t *testing.T, joinNode *plan.Node) *plan.Expr {
		t.Helper()
		require.Equal(t, plan.Node_JOIN, joinNode.NodeType)
		require.Len(t, joinNode.OnList, 1)

		joinFn := joinNode.OnList[0].GetF()
		require.NotNil(t, joinFn)
		require.Equal(t, "=", joinFn.Func.ObjName)
		require.Len(t, joinFn.Args, 2)
		return joinFn.Args[1]
	}
	requirePrefixExpr := func(t *testing.T, expr *plan.Expr, colName string, length int64, tag int32) {
		t.Helper()

		castFn := expr.GetF()
		require.NotNil(t, castFn)
		require.Equal(t, "cast", castFn.Func.ObjName)
		require.Len(t, castFn.Args, 2)

		substringFn := castFn.Args[0].GetF()
		require.NotNil(t, substringFn)
		require.Equal(t, "substring", substringFn.Func.ObjName)
		require.Len(t, substringFn.Args, 3)
		require.Equal(t, colName, substringFn.Args[0].GetCol().Name)
		require.Equal(t, tag, substringFn.Args[0].GetCol().RelPos)
		require.Equal(t, int64(1), substringFn.Args[1].GetLit().GetI64Val())
		require.Equal(t, length, substringFn.Args[2].GetLit().GetI64Val())
	}

	t.Run("single prefix part", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)
		builder.qry.HasForeignKeyAction = true

		gotNodeID, err := appendDeleteIndexTablePlan(
			builder,
			bindCtx,
			&plan.ObjectRef{ObjName: "idx_body"},
			indexTableDef,
			&plan.IndexDef{
				Parts:           []string{"body"},
				IndexAlgoParams: `{"prefix_lengths":"body:8"}`,
			},
			typMap,
			posMap,
			lastNodeID,
			true, true, false,
		)

		require.NoError(t, err)
		joinNode := extractJoinNode(t, builder, gotNodeID)
		require.Len(t, joinNode.Children, 2)
		indexScan := builder.qry.Nodes[joinNode.Children[0]]
		source := builder.qry.Nodes[joinNode.Children[1]]
		require.Len(t, indexScan.BindingTags, 1)
		require.Len(t, source.BindingTags, 1)
		require.NotEqual(t, indexScan.BindingTags[0], source.BindingTags[0])
		require.Empty(t, indexScan.RuntimeFilterProbeList)
		require.Empty(t, joinNode.RuntimeFilterBuildList)
		lookupExpr := extractLookupExpr(t, joinNode)
		requirePrefixExpr(t, lookupExpr, "body", 8, source.BindingTags[0])
	})

	t.Run("foreign key action preserves source rows", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)

		gotNodeID, err := appendDeleteIndexTablePlan(
			builder,
			bindCtx,
			&plan.ObjectRef{ObjName: "idx_body"},
			indexTableDef,
			&plan.IndexDef{Parts: []string{"body"}},
			typMap,
			posMap,
			lastNodeID,
			false, true, true,
		)

		require.NoError(t, err)
		joinNode := extractJoinNode(t, builder, gotNodeID)
		require.Equal(t, plan.Node_LEFT, joinNode.JoinType)
		require.False(t, joinNode.IsRightJoin)
		require.Equal(t, plan.Node_PROJECT, builder.qry.Nodes[joinNode.Children[0]].NodeType)
		require.Equal(t, plan.Node_TABLE_SCAN, builder.qry.Nodes[joinNode.Children[1]].NodeType)
	})

	t.Run("composite prefix part", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)

		gotNodeID, err := appendDeleteIndexTablePlan(
			builder,
			bindCtx,
			&plan.ObjectRef{ObjName: "idx_body_tenant"},
			indexTableDef,
			&plan.IndexDef{
				Parts:           []string{"body", "tenant"},
				IndexAlgoParams: `{"prefix_lengths":"body:8"}`,
			},
			typMap,
			posMap,
			lastNodeID,
			false, true, false,
		)

		require.NoError(t, err)
		joinNode := extractJoinNode(t, builder, gotNodeID)
		lookupExpr := extractLookupExpr(t, joinNode)

		serialFn := lookupExpr.GetF()
		require.NotNil(t, serialFn)
		require.Equal(t, "serial_full", serialFn.Func.ObjName)
		require.Len(t, serialFn.Args, 2)
		requirePrefixExpr(t, serialFn.Args[0], "body", 8, builder.qry.Nodes[joinNode.Children[1]].BindingTags[0])
		require.Equal(t, "tenant", serialFn.Args[1].GetCol().Name)
	})

	t.Run("composite unique prefix part", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)

		gotNodeID, err := appendDeleteIndexTablePlan(
			builder,
			bindCtx,
			&plan.ObjectRef{ObjName: "idx_body_tenant"},
			indexTableDef,
			&plan.IndexDef{
				Parts:           []string{"body", "tenant"},
				IndexAlgoParams: `{"prefix_lengths":"body:8"}`,
			},
			typMap,
			posMap,
			lastNodeID,
			true, true, false,
		)

		require.NoError(t, err)
		joinNode := extractJoinNode(t, builder, gotNodeID)
		lookupExpr := extractLookupExpr(t, joinNode)

		serialFn := lookupExpr.GetF()
		require.NotNil(t, serialFn)
		require.Equal(t, "serial", serialFn.Func.ObjName)
		require.Len(t, serialFn.Args, 2)
		requirePrefixExpr(t, serialFn.Args[0], "body", 8, builder.qry.Nodes[joinNode.Children[1]].BindingTags[0])
		require.Equal(t, "tenant", serialFn.Args[1].GetCol().Name)
	})
}

func TestUniqueIndexDeletePreservesTagThroughFilterAndLock(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(plan.Query_DELETE, ctx, false, false)
	bindCtx := NewBindContext(builder, nil)
	sourceTag := builder.genNewBindTag()
	rowIDType := plan.Type{Id: int32(types.T_Rowid), Width: 16}
	keyType := plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	sourceID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{
			{Name: catalog.Row_ID, Typ: rowIDType},
			{Name: catalog.IndexTableIndexColName, Typ: keyType},
		}},
		ProjectList: []*plan.Expr{
			{Typ: rowIDType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: sourceTag, ColPos: 0}}},
			{Typ: keyType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: sourceTag, ColPos: 1}}},
		},
		BindingTags: []int32{sourceTag},
	}, bindCtx)
	delInfo := &deleteNodeInfo{
		objRef:             &plan.ObjectRef{ObjName: "idx_unique"},
		tableDef:           &plan.TableDef{Name: "idx_unique"},
		deleteIndex:        0,
		pkPos:              1,
		pkTyp:              keyType,
		preserveProjection: true,
	}

	deleteID, err := makeOneDeletePlan(builder, bindCtx, sourceID, delInfo, true, false, false)
	require.NoError(t, err)
	deleteNode := builder.qry.Nodes[deleteID]
	require.Equal(t, plan.Node_DELETE, deleteNode.NodeType)
	require.Equal(t, int32(0), deleteNode.DeleteCtx.RowIdIdx)
	require.Equal(t, int32(1), deleteNode.DeleteCtx.PrimaryKeyIdx)

	deleteProject := builder.qry.Nodes[deleteNode.Children[0]]
	require.Equal(t, plan.Node_PROJECT, deleteProject.NodeType)
	require.Len(t, deleteProject.ProjectList, 2)
	require.Len(t, deleteProject.BindingTags, 1)
	lockNode := builder.qry.Nodes[deleteProject.Children[0]]
	require.Equal(t, plan.Node_LOCK_OP, lockNode.NodeType)
	require.Equal(t, int32(1), lockNode.LockTargets[0].PrimaryColIdxInBat)
	filterNode := builder.qry.Nodes[lockNode.Children[0]]
	require.Equal(t, plan.Node_FILTER, filterNode.NodeType)
	compactProject := builder.qry.Nodes[filterNode.Children[0]]
	require.Equal(t, plan.Node_PROJECT, compactProject.NodeType)
	require.Len(t, compactProject.ProjectList, 2)
	require.Len(t, compactProject.BindingTags, 1)
	compactTag := compactProject.BindingTags[0]
	require.NotEqual(t, sourceTag, compactTag)
	require.Equal(t, []int32{compactTag}, filterNode.BindingTags)
	require.Empty(t, lockNode.BindingTags)
	require.Equal(t, compactTag, filterNode.FilterList[0].GetF().Args[0].GetCol().RelPos)
	require.Equal(t, compactTag, lockNode.LockTargets[0].PrimaryColRelPos)

	_, err = builder.remapAllColRefs(
		deleteID,
		0,
		make(map[[2]int32]int),
		make(map[[2]int32]bool),
		make(map[[2]int32]int),
	)
	require.NoError(t, err)
	require.Len(t, lockNode.ProjectList, 2)
	require.Len(t, filterNode.ProjectList, 2)
	require.Len(t, compactProject.ProjectList, 2)
	require.Len(t, deleteProject.ProjectList, 2)
	require.Equal(t, int32(0), deleteNode.DeleteCtx.RowIdIdx)
	require.Equal(t, int32(1), deleteNode.DeleteCtx.PrimaryKeyIdx)
}

func TestPrefixIndexDMLPlansMaterializePrefixKeys(t *testing.T) {
	mock := NewMockOptimizer(true)
	emp := mock.ctxt.tables["emp"]
	require.NotNil(t, emp)
	require.NotEmpty(t, emp.Indexes)
	for _, idxDef := range emp.Indexes {
		idxDef.IndexAlgoParams = `{"prefix_lengths":"ename:4"}`
	}

	assertPlanHasEnamePrefix := func(t *testing.T, sql string) {
		t.Helper()

		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
		require.NotNil(t, logicPlan.GetQuery())

		require.True(
			t,
			queryHasPrefixSubstring(logicPlan.GetQuery(), "ename", 4),
			"expected plan for %q to materialize prefix index key with substring(ename, 1, 4)",
			sql,
		)
	}

	assertPlanHasEnamePrefix(t, "update constraint_test.emp set ename = 'abcdef-long' where empno = 1")
	assertPlanHasEnamePrefix(t, "delete from constraint_test.emp where empno = 1")

	assertPlanHasPrefixCount := func(t *testing.T, sql, colName string, minCount int) {
		t.Helper()

		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
		require.NotNil(t, logicPlan.GetQuery())

		got := countQueryPrefixSubstrings(logicPlan.GetQuery(), colName, 4)
		require.GreaterOrEqual(t, got, minCount, "expected plan for %q to materialize at least %d prefix keys", sql, minCount)
	}

	dept := mock.ctxt.tables["dept"]
	require.NotNil(t, dept)
	for _, idxDef := range dept.Indexes {
		if len(idxDef.Parts) == 1 && idxDef.Parts[0] == "dname" {
			idxDef.IndexAlgoParams = `{"prefix_lengths":"dname:4"}`
		}
	}
	assertPlanHasPrefixCount(t, "update constraint_test.dept set dname = 'abcdef-long' where deptno = 1", "dname", 1)
	assertPlanHasPrefixCount(t, "delete from constraint_test.dept where deptno = 1", "dname", 1)

	singleIdx := mock.ctxt.tables["single_idx_t"]
	require.NotNil(t, singleIdx)
	require.Len(t, singleIdx.Indexes, 1)
	singleIdx.Cols[1].Typ = plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	singleIdx.Indexes[0].IndexAlgoParams = `{"prefix_lengths":"val:4"}`
	singleIdxIndexTable := mock.ctxt.tables[singleIdx.Indexes[0].IndexTableName]
	require.NotNil(t, singleIdxIndexTable)
	singleIdxIndexTable.Cols[0].Typ = plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	assertPlanHasPrefixCount(t, "insert into constraint_test.single_idx_t values (1, 'abcdef-long') on duplicate key update val = values(val)", "val", 2)
	assertPlanHasPrefixCount(t, "update constraint_test.single_idx_t set val = 'abcdef-long' where id = 1", "val", 2)
	assertPlanHasPrefixCount(t, "delete from constraint_test.single_idx_t where id = 1", "val", 1)
}

func queryHasPrefixSubstring(query *plan.Query, colName string, length int64) bool {
	return countQueryPrefixSubstrings(query, colName, length) > 0
}

func countQueryPrefixSubstrings(query *plan.Query, colName string, length int64) int {
	count := 0
	for _, node := range query.Nodes {
		count += countExprListPrefixSubstrings(node.ProjectList, colName, length)
		count += countExprListPrefixSubstrings(node.OnList, colName, length)
		count += countExprListPrefixSubstrings(node.FilterList, colName, length)
	}
	return count
}

func countExprListPrefixSubstrings(exprs []*plan.Expr, colName string, length int64) int {
	count := 0
	for _, expr := range exprs {
		count += countExprPrefixSubstrings(expr, colName, length)
	}
	return count
}

func countExprPrefixSubstrings(expr *plan.Expr, colName string, length int64) int {
	if expr == nil {
		return 0
	}

	fn := expr.GetF()
	if fn == nil {
		list := expr.GetList()
		if list == nil {
			return 0
		}
		return countExprListPrefixSubstrings(list.List, colName, length)
	}

	count := 0
	if fn.Func.ObjName == "substring" && len(fn.Args) == 3 {
		start := fn.Args[1].GetLit()
		prefixLen := fn.Args[2].GetLit()
		if exprContainsColumn(fn.Args[0], colName) &&
			start != nil && start.GetI64Val() == 1 &&
			prefixLen != nil && prefixLen.GetI64Val() == length {
			count++
		}
	}

	return count + countExprListPrefixSubstrings(fn.Args, colName, length)
}

func exprContainsColumn(expr *plan.Expr, colName string) bool {
	if expr == nil {
		return false
	}

	if col := expr.GetCol(); col != nil {
		if col.Name == "" {
			return true
		}
		return columnNameMatches(col.Name, colName)
	}

	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if exprContainsColumn(arg, colName) {
				return true
			}
		}
		return false
	}

	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprContainsColumn(item, colName) {
				return true
			}
		}
	}

	return false
}

func columnNameMatches(got, want string) bool {
	return got == want || strings.HasSuffix(got, "."+want)
}

func TestIndexNeedsRewriteForUpdateIvfColumns(t *testing.T) {
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "embedding", Typ: plan.Type{Id: int32(types.T_array_float32)}},
			{Name: "title", Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: "category", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "note", Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
	}
	posMap := map[string]int{
		"id":        0,
		"embedding": 1,
		"title":     2,
		"category":  3,
		"note":      4,
	}
	colMap := make(map[string]*plan.ColDef, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		colMap[col.Name] = col
	}
	indexDef := &plan.IndexDef{
		IndexAlgo:       catalog.MoIndexIvfFlatAlgo.ToString(),
		Parts:           []string{"embedding"},
		IndexAlgoParams: `{"lists":"2","op_type":"` + metric.DistFuncOpTypes["l2_distance"] + `"}`,
		IncludedColumns: []string{"title", "category"},
	}

	tests := []struct {
		name       string
		updateCols map[string]int
		want       bool
	}{
		{name: "unrelated column does not rewrite ivf", updateCols: map[string]int{"note": 4}, want: false},
		{name: "vector key rewrites ivf", updateCols: map[string]int{"embedding": 1}, want: true},
		{name: "primary key rewrites ivf", updateCols: map[string]int{"id": 0}, want: true},
		{name: "first include column rewrites ivf", updateCols: map[string]int{"title": 2}, want: true},
		{name: "second include column rewrites ivf", updateCols: map[string]int{"category": 3}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := indexNeedsRewriteForUpdate(tableDef, indexDef, tt.updateCols, posMap, colMap)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
