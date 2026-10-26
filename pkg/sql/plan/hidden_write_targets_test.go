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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
