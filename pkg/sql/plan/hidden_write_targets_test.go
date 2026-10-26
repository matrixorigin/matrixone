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
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// Build catalog-valid hidden keys through DDL, then add the storage row ID.
// No engine or cluster is needed to test write-target admission.
func hiddenWriteOptimizer(t *testing.T) *MockOptimizer {
	t.Helper()
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	for i, ddl := range []string{
		"create table hidden_composite(a int,b int,v int,g int generated always as(v+1) stored,primary key(a,b))",
		"create table hidden_no_key(a int)",
		"create table hidden_generated(a int,g int generated always as(a+1) stored)",
	} {
		built, err := runOneStmt(mock, t, ddl)
		require.NoError(t, err)
		table := built.GetDdl().GetCreateTable().TableDef
		table.TblId = uint64(88000 + i)
		table.TableType = catalog.SystemOrdinaryRel
		table.Cols = append(table.Cols, MakeRowIdColDef())
		table.Name2ColIndex = make(map[string]int32, len(table.Cols))
		for j, col := range table.Cols {
			col.ColId = uint64(j + 1)
			table.Name2ColIndex[col.Name] = int32(j)
		}
		mock.ctxt.tables[table.Name] = table
		mock.ctxt.objects[table.Name] = &ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
		mock.ctxt.id2name[table.TblId] = table.Name
	}
	return mock
}

func TestHiddenWriteTargets(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"insert values", "insert into hidden_composite(__mo_cpkey_col,a,b) values('x',1,2)"},
		{"insert select", "insert into hidden_composite(a,b,__mo_cpkey_col) select 1,2,'x'"},
		{"insert set", "insert into hidden_composite set a=1,b=2,__mo_cpkey_col='x'"},
		{"insert ignore default", "insert ignore into hidden_composite(a,b,__mo_cpkey_col) values(1,2,default)"},
		{"replace values", "replace into hidden_composite(a,b,__mo_cpkey_col) values(1,2,'x')"},
		{"replace select", "replace into hidden_composite(a,b,__mo_cpkey_col) select 1,2,'x'"},
		{"replace set", "replace into hidden_composite set a=1,b=2,__mo_cpkey_col=default"},
		{"multi insert", "insert all into hidden_composite(a,b,__mo_cpkey_col) values(1,2,'x') select 1"},
		{"hidden generated default", "insert into hidden_composite(a,b,__mo_cpkey_col,g) values(1,2,default,default)"},
		{"odku assignment", "insert into hidden_composite(a,b) values(1,2) on duplicate key update __mo_cpkey_col='x'"},
		{"odku default", "insert ignore into hidden_composite(a,b) values(1,2) on duplicate key update __mo_cpkey_col=default"},
		{"no key odku fallback", "insert into hidden_no_key(a) values(1) on duplicate key update __mo_fake_pk_col=9"},
		{"no key odku unknown", "insert into hidden_no_key(a) values(1) on duplicate key update missing=9"},
		{"update mixed", "update hidden_composite set v=9,__mo_cpkey_col='x' where a=1"},
		{"update qualified", "update hidden_composite c set c.__mo_cpkey_col=default"},
		{"update multi table", "update hidden_composite c join hidden_no_key n on c.a=n.a set c.v=9,n.__mo_fake_pk_col=7"},
		{"fake key insert", "insert into hidden_no_key(a,__mo_fake_pk_col) values(1,9)"},
		{"row id insert", "insert into hidden_composite(a,b,__mo_rowid) values(1,2,default)"},
		{"prepared insert", "prepare s from insert into hidden_composite(a,b,__mo_cpkey_col) values(?,?,?)"},
		{"prepared update", "prepare s from update hidden_composite set __mo_cpkey_col=?"},
		{"prepared replace", "prepare s from replace into hidden_composite(a,b,__mo_cpkey_col) values(?,?,?)"},
		{"prepared multi insert", "prepare s from insert all into hidden_composite(a,b,__mo_cpkey_col) values(1,2,'x') select 1"},
		{"load column list", "load data inline format='csv',data='1,9\\n' into table hidden_no_key fields terminated by ',' (a,__mo_fake_pk_col)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := hiddenWriteOptimizer(t)
			built, err := runOneStmt(mock, t, tc.sql)
			require.Error(t, err)
			require.Nil(t, built)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "%v", err)
		})
	}
}

func TestHiddenLoadSetTargetsDoNotConsumeAST(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, assignment := range []string{"__mo_fake_pk_col=nullif(__mo_fake_pk_col,0)", "__mo_fake_pk_col=nullif(@v,0)"} {
			name := "modern " + assignment
			if legacy {
				name = "legacy " + assignment
			}
			t.Run(name, func(t *testing.T) {
				mock := hiddenWriteOptimizer(t)
				stmt, err := mysql.ParseOne(t.Context(), "load data inline format='csv',data='1,2\\n' into table hidden_no_key fields terminated by ',' (a,@v) set a=nullif(a,0),"+assignment, 1)
				require.NoError(t, err)
				defer stmt.Free()
				load := stmt.(*tree.Load)
				before := tree.String(stmt, dialect.MYSQL)
				for range 2 {
					var built *Plan
					if legacy {
						built, err = buildLoad(load, &mock.ctxt, false)
					} else {
						built, err = BuildPlan(&mock.ctxt, load, false)
					}
					require.Error(t, err)
					require.Nil(t, built)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "%v", err)
					require.Equal(t, before, tree.String(stmt, dialect.MYSQL))
					require.Nil(t, load.Param.NullMap, "rejection must precede consuming the first assignment")
				}
			})
		}
	}
}

func TestHiddenWriteMetadataControls(t *testing.T) {
	mock := hiddenWriteOptimizer(t)
	table := mock.ctxt.tables["hidden_no_key"]
	// Metadata, not a reserved prefix, defines the write boundary. Internal
	// index columns and old visible __mo_fi_* columns remain ordinary targets.
	for _, tc := range []struct {
		name   string
		hidden bool
	}{
		{"arbitrary_hidden", true}, {"__mo_fi_user", false}, {catalog.IndexTableIndexColName, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			column := *table.Cols[0]
			column.Name = tc.name
			column.Hidden = tc.hidden
			def := &TableDef{Name: "target", Cols: []*ColDef{&column}}
			builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, false)
			cols, err := builder.getInsertColsFromStmt(tree.IdentifierList{tree.Identifier(tc.name)}, def)
			if tc.hidden {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError))
				require.Nil(t, cols)
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{tc.name}, cols)
			}
		})
	}
	// Normalized LOAD assignments retain their names and must be revalidated.
	stmt, err := mysql.ParseOne(t.Context(), "load data inline format='csv',data='1' into table hidden_no_key set a=nullif(a,0)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	param := stmt.(*tree.Load).Param
	require.NoError(t, validateLoadSetAssignments(param, &mock.ctxt, table))
	require.NoError(t, InitNullMap(param, &mock.ctxt))
	table.Cols[0].Hidden = true
	require.True(t, moerr.IsMoErrCode(validateLoadSetAssignments(param, &mock.ctxt, table), moerr.ErrBadFieldError))
}

func TestHiddenLegacyWriteTargets(t *testing.T) {
	for _, sql := range []string{
		"insert into hidden_no_key(a,__mo_fake_pk_col) values(1,9)",
		"load data inline format='csv',data='1,9\\n' into table hidden_no_key fields terminated by ',' (a,__mo_fake_pk_col)",
	} {
		t.Run(sql, func(t *testing.T) {
			mock := hiddenWriteOptimizer(t)
			statement, err := mysql.ParseOne(t.Context(), sql, 1)
			require.NoError(t, err)
			defer statement.Free()
			var built *Plan
			switch stmt := statement.(type) {
			case *tree.Insert:
				built, err = buildInsert(stmt, &mock.ctxt, false, false)
			case *tree.Load:
				built, err = buildLoad(stmt, &mock.ctxt, false)
			}
			require.Error(t, err)
			require.Nil(t, built)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "%v", err)
		})
	}
}

func TestHiddenInsertCopyAuthority(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			option  any
			allowed bool
		}{
			{"ordinary", nil, false},
			{"wrong type", "copy", false},
			{"nil option", (*planpb.AlterCopyOpt)(nil), false},
			{"empty target", &planpb.AlterCopyOpt{}, false},
			{"other target", &planpb.AlterCopyOpt{TargetTableName: "other"}, false},
			{"exact target", &planpb.AlterCopyOpt{TargetTableName: "hidden_no_key"}, true},
		} {
			t.Run(fmt.Sprintf("legacy=%v/%s", legacy, tc.name), func(t *testing.T) {
				mock := hiddenWriteOptimizer(t)
				original := mock.ctxt.GetContext()
				ctx := original
				if tc.option != nil {
					ctx = context.WithValue(ctx, defines.AlterCopyOpt{}, tc.option)
				}
				mock.ctxt.SetContext(ctx)
				stmt, err := mysql.ParseOne(ctx, "insert into hidden_no_key(a,__mo_fake_pk_col) select hidden_no_key.a,hidden_no_key.__mo_fake_pk_col from hidden_no_key", 1)
				require.NoError(t, err)
				defer stmt.Free()
				before := tree.String(stmt, dialect.MYSQL)
				var built *Plan
				if legacy {
					built, err = buildInsert(stmt.(*tree.Insert), &mock.ctxt, false, false)
				} else {
					built, err = BuildPlan(&mock.ctxt, stmt, false)
				}
				if tc.allowed {
					require.NoError(t, err)
					require.NotNil(t, built)
					mock.ctxt.SetContext(original)
					_, err = BuildPlan(&mock.ctxt, stmt, false)
				}
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "%v", err)
				require.Equal(t, before, tree.String(stmt, dialect.MYSQL), "COPY authority must not persist in the AST")
			})
		}
	}
	mock := hiddenWriteOptimizer(t)
	mock.ctxt.SetContext(context.WithValue(mock.ctxt.GetContext(), defines.AlterCopyOpt{}, &planpb.AlterCopyOpt{TargetTableName: "hidden_composite"}))
	for _, col := range []string{catalog.CPrimaryKeyColName, catalog.Row_ID} {
		_, err := runOneStmt(mock, t, "insert into hidden_composite(a,b,"+col+") values(1,2,default)")
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "%v", err)
	}
}

func TestHiddenGeneratedCopyTarget(t *testing.T) {
	mock := hiddenWriteOptimizer(t)
	mock.ctxt.SetContext(context.WithValue(mock.ctxt.GetContext(), defines.AlterCopyOpt{}, &planpb.AlterCopyOpt{TargetTableName: "hidden_generated"}))
	built, err := runOneStmt(mock, t, "insert into hidden_generated(a,__mo_fake_pk_col) select a,__mo_fake_pk_col from hidden_generated")
	require.NoError(t, err)
	require.NotNil(t, built)
	// A correct destination does not authorize other hidden metadata, or a
	// fake-key-named column that is not the destination's actual primary key.
	table := mock.ctxt.tables["hidden_generated"]
	table.Cols[0].Hidden = true
	_, err = runOneStmt(mock, t, "insert into hidden_generated(a) values(1)")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError))
	table.Cols[0].Hidden = false
	table.Pkey = nil
	builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, false)
	_, err = builder.getInsertColsFromStmt(tree.IdentifierList{catalog.FakePrimaryKeyColName}, table)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError))
}

func TestLoadSetFilteredColumnLookup(t *testing.T) {
	mock := hiddenWriteOptimizer(t)
	table := &TableDef{Name: "filtered", Cols: []*ColDef{{Name: "b"}, {Name: "a"}}, Name2ColIndex: map[string]int32{"a": 4, "b": 7}}
	stmt, err := mysql.ParseOne(t.Context(), "load data inline format='csv',data='1,2' into table filtered (a,b) set A=nullif(a,0),b=nullif(b,0)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	param := stmt.(*tree.Load).Param
	before := tree.String(stmt, dialect.MYSQL)
	require.NoError(t, validateLoadSetAssignments(param, &mock.ctxt, table))
	require.Equal(t, before, tree.String(stmt, dialect.MYSQL))
	require.Equal(t, map[string]int32{"a": 4, "b": 7}, table.Name2ColIndex)
	for _, legacy := range []bool{false, true} {
		built, err := func() (*Plan, error) {
			statement, err := mysql.ParseOne(t.Context(), "load data inline format='csv',data='1,2' into table hidden_composite (a,b) set a=nullif(a,0),b=nullif(b,0)", 1)
			if err != nil {
				return nil, err
			}
			defer statement.Free()
			if legacy {
				return buildLoad(statement.(*tree.Load), &mock.ctxt, false)
			}
			return BuildPlan(&mock.ctxt, statement, false)
		}()
		require.NoError(t, err, "legacy=%v", legacy)
		require.NotNil(t, built)
	}
}

func TestHiddenLegacyLoadFilteredColumns(t *testing.T) {
	mock := hiddenWriteOptimizer(t)
	// Cols is the legacy filtered view; the original metadata offsets are stale.
	a := &ColDef{Name: "a", Typ: Type{Id: int32(types.T_int32)}}
	b := &ColDef{Name: "b", Typ: Type{Id: int32(types.T_time), Scale: 3}}
	hidden := &ColDef{Name: catalog.FakePrimaryKeyColName, Hidden: true}
	table := &TableDef{Name: "filtered", Cols: []*ColDef{a, b, hidden}, Name2ColIndex: map[string]int32{"a": 1, "b": 4, hidden.Name: 7}}
	for _, columns := range []string{"b,@v,a", "missing", catalog.FakePrimaryKeyColName} {
		t.Run(columns, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), "load data inline format='csv',data='1' into table filtered ("+columns+")", 1)
			require.NoError(t, err)
			defer stmt.Free()
			project, positions, inputs, resolved, err := getExternalWithColListProject(stmt.(*tree.Load), &mock.ctxt, table, table.Name)
			if columns != "b,@v,a" {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrBadFieldError), "%v", err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []*ColDef{b, a}, resolved.Cols)
			require.Equal(t, map[string]int32{"b": 0, "a": 1}, positions)
			require.Equal(t, map[string]int32{"b": 0, "a": 2}, inputs)
			require.Equal(t, int32(types.T_varchar), project[0].Typ.Id)
			require.Equal(t, a.Typ, project[1].Typ)
			require.Equal(t, int32(1), project[1].GetCol().ColPos)
			require.Equal(t, map[string]int32{"a": 1, "b": 4, hidden.Name: 7}, table.Name2ColIndex)
			require.Equal(t, []*ColDef{a, b, hidden}, table.Cols)
		})
	}
}
