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
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func newCheckAlterOptimizer(t *testing.T, ddl string) *MockOptimizer {
	t.Helper()
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	p, err := runOneStmt(mock, t, ddl)
	require.NoError(t, err)
	source := p.GetDdl().GetCreateTable().TableDef
	source.TblId = 25387
	source.Createsql = ddl
	mock.ctxt.tables[source.Name] = source
	mock.ctxt.objects[source.Name] = &ObjectRef{SchemaName: mock.ctxt.DefaultDatabase(), ObjName: source.Name}
	if len(source.Indexes) > 0 {
		proc := mock.ctxt.GetProcess()
		mock.ctxt.internalSQLExecutor = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
			if !strings.Contains(sql, "FROM mo_catalog.mo_indexes") {
				return executor.Result{}, nil
			}
			result := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_int8.ToType()}, proc.Mp())
			transferred := false
			defer func() {
				if !transferred {
					owned := result.GetResult()
					owned.Close()
				}
			}()
			result.NewBatchWithRowCount(len(source.Indexes))
			names := make([]string, 0, len(source.Indexes))
			visible := make([]int8, 0, len(source.Indexes))
			for _, index := range source.Indexes {
				names = append(names, index.IndexName)
				visible = append(visible, 1)
			}
			if err := executor.AppendStringRows(result, 0, names); err != nil {
				return executor.Result{}, err
			}
			if err := executor.AppendFixedRows(result, 1, visible); err != nil {
				return executor.Result{}, err
			}
			transferred = true
			return result.GetResult(), nil
		})
	}
	return mock
}

func TestAlterCheckUsesFinalCopyDefinition(t *testing.T) {
	for _, clause := range []string{
		"add constraint positive_v check(v>0)",
		"add check(v>0)",
		"add constraint positive_w check(w>0), add column w int default 1",
		"add column w int default 1, add constraint positive_w check(w>0)",
	} {
		t.Run(clause, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int)")
			source := mock.ctxt.tables["check_t"]
			before := proto.Clone(source).(*planpb.TableDef)
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "alter table check_t "+clause, 1)
			require.NoError(t, err)
			defer stmt.Free()
			astBefore := tree.String(stmt, dialect.MYSQL)
			for range 2 {
				p, err := BuildPlan(&mock.ctxt, stmt, false)
				require.NoError(t, err)
				alter := p.GetDdl().GetAlterTable()
				require.Equal(t, planpb.AlterTable_COPY, alter.AlgorithmType)
				require.Len(t, alter.CopyTableDef.Checks, 1)
				check := alter.CopyTableDef.Checks[0]
				require.NotNil(t, check.Check)
				require.Contains(t, alter.CreateTmpTableSql, "CONSTRAINT `"+check.Name+"` CHECK")
				require.Contains(t, alter.InsertTmpDataSql, "check_t")
				require.Equal(t, before, source)
				require.Equal(t, astBefore, tree.String(stmt, dialect.MYSQL))
			}
		})
	}
}

func TestAlterCheckOrderedNamesAndLegacyRecovery(t *testing.T) {
	for _, test := range []struct {
		clause string
		want   []string
	}{
		{"drop check __mo_chk_1", []string{"__mo_chk_2"}},
		{"drop constraint __mo_chk_1", []string{"__mo_chk_2"}},
		{"drop check __mo_chk_1, add check(v>0)", []string{"__mo_chk_2", "check_t_chk_2"}},
		{"drop check __mo_chk_1, drop check __mo_chk_2", nil},
		{"drop check __mo_chk_1, add constraint __mo_chk_1 check(v>0)", []string{"__mo_chk_2", "__mo_chk_1"}},
		{"add constraint transient_ck check(v>0), drop constraint transient_ck", []string{"__mo_chk_1", "__mo_chk_2"}},
	} {
		for _, legacy := range []bool{false, true} {
			t.Run(test.clause+map[bool]string{false: "/structured", true: "/legacy"}[legacy], func(t *testing.T) {
				// Use the names emitted by the legacy planner explicitly.  The
				// current CREATE path names an unnamed CHECK as check_t_chk_N;
				// this test is specifically exercising recovery of the historical
				// __mo_chk_N identities when the structured CHECK metadata is absent.
				mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int, constraint __mo_chk_1 check(v>=0), constraint __mo_chk_2 check(v<100))")
				if legacy {
					mock.ctxt.tables["check_t"].Checks = nil
				}
				p, err := runOneStmt(mock, t, "alter table check_t "+test.clause)
				require.NoError(t, err)
				alter := p.GetDdl().GetAlterTable()
				require.Equal(t, planpb.AlterTable_COPY, alter.AlgorithmType)
				require.Len(t, alter.CopyTableDef.Checks, len(test.want))
				for i, name := range test.want {
					require.Equal(t, name, alter.CopyTableDef.Checks[i].Name)
				}
				if len(test.want) == 0 {
					require.NotContains(t, alter.CreateTmpTableSql, "CHECK (")
				}
			})
		}
	}
}

func TestAlterCheckGeneratedNames(t *testing.T) {
	for _, tc := range []struct {
		name, ddl, clause string
		want              []string
	}{
		{"empty table", "create table check_t(v int)", "add check(v>0)", []string{"check_t_chk_1"}},
		{"drop skips occupied name", "create table check_t(v int,check(v>=0),check(v<100))", "drop check check_t_chk_1, add check(v>0)", []string{"check_t_chk_2", "check_t_chk_3"}},
		{"explicit names count", "create table check_t(v int,constraint positive check(v>0))", "add check(v<100)", []string{"positive", "check_t_chk_2"}},
		{"multiple ordered adds", "create table check_t(v int)", "add check(v>0), add check(v<100)", []string{"check_t_chk_1", "check_t_chk_2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, tc.ddl)
			source := mock.ctxt.tables["check_t"]
			before := proto.Clone(source).(*planpb.TableDef)
			p, err := runOneStmt(mock, t, "alter table check_t "+tc.clause)
			require.NoError(t, err)
			checks := p.GetDdl().GetAlterTable().CopyTableDef.Checks
			require.Len(t, checks, len(tc.want))
			for i, name := range tc.want {
				require.Equal(t, name, checks[i].Name)
			}
			require.Equal(t, before, source)
		})
	}
}

func TestAlterCheckRejectsInvalidActions(t *testing.T) {
	for _, test := range []struct{ clause, want string }{
		{"add constraint ck check(v>0)", "duplicate check constraint"},
		{"drop check absent", "Can't DROP"},
		{"drop constraint absent", "Can't DROP"},
		{"drop check ck, drop check ck", "Can't DROP"},
		{"add check(v>0) not enforced", "NOT ENFORCED"},
		{"add check(absent>0)", "absent"},
		{"add check(rand()>0)", "non-deterministic"},
		{"add check(v>0), algorithm=inplace", "requires ALGORITHM=COPY"},
		{"drop check ck, algorithm=instant", "requires ALGORITHM=COPY"},
		{"add check(v>0), lock=none", "exclusive lock"},
		{"drop foreign key ck", "Can't DROP"},
	} {
		t.Run(test.clause, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,constraint ck check(v>=0))")
			before := proto.Clone(mock.ctxt.tables["check_t"]).(*planpb.TableDef)
			_, err := runOneStmt(mock, t, "alter table check_t "+test.clause)
			require.ErrorContains(t, err, test.want)
			require.Equal(t, before, mock.ctxt.tables["check_t"])
		})
	}
}

func TestAlterConstraintDropResolution(t *testing.T) {
	for _, test := range []struct{ indexes, clause, name string }{
		{"", "add unique(v), drop constraint v", "v"},
		{"", "add unique key uv(v), drop constraint uv", "uv"},
		{",key v(v)", "add unique(v), drop constraint v_2", "v_2"},
		{"", "add index(v), add unique(v), drop constraint v_2", "v_2"},
		{",key v(v)", "drop index v, add unique(v), drop constraint v", "v"},
	} {
		t.Run(test.clause+test.indexes, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int"+test.indexes+")")
			source := mock.ctxt.tables["check_t"]
			before := proto.Clone(source).(*planpb.TableDef)
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "alter table check_t "+test.clause, 1)
			require.NoError(t, err)
			defer stmt.Free()
			astBefore := tree.String(stmt, dialect.MYSQL)
			for range 2 {
				p, err := BuildPlan(&mock.ctxt, stmt, false)
				require.NoError(t, err)
				alter := p.GetDdl().GetAlterTable()
				require.Equal(t, planpb.AlterTable_INPLACE, alter.AlgorithmType)
				drop := alter.Actions[len(alter.Actions)-1].GetDrop()
				require.Equal(t, planpb.AlterTableDrop_INDEX, drop.Typ)
				require.Equal(t, test.name, drop.Name)
				require.Equal(t, before, source)
				require.Equal(t, astBefore, tree.String(stmt, dialect.MYSQL))
			}
		})
	}
	t.Run("generic primary drop uses copy", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int)")
		p, err := runOneStmt(mock, t, "alter table check_t drop constraint `PRIMARY`")
		require.NoError(t, err)
		require.Equal(t, planpb.AlterTable_COPY, p.GetDdl().GetAlterTable().AlgorithmType)
	})
	t.Run("generic unique drop stays inplace", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,unique key uv(v))")
		p, err := runOneStmt(mock, t, "alter table check_t drop constraint uv")
		require.NoError(t, err)
		alter := p.GetDdl().GetAlterTable()
		require.Equal(t, planpb.AlterTable_INPLACE, alter.AlgorithmType)
		require.Equal(t, planpb.AlterTableDrop_INDEX, alter.Actions[0].GetDrop().Typ)
	})
	t.Run("generic foreign key drop stays inplace", func(t *testing.T) {
		mock := NewMockOptimizer(true, newPlanTestProcess(t))
		setMockEmpDeptForeignKeyAction(t, mock, planpb.ForeignKeyDef_RESTRICT, planpb.ForeignKeyDef_RESTRICT)
		fk := mock.ctxt.tables["emp"].Fkeys[0]
		p, err := runOneStmt(mock, t, "alter table emp drop constraint "+fk.Name)
		require.NoError(t, err)
		alter := p.GetDdl().GetAlterTable()
		require.Equal(t, planpb.AlterTable_INPLACE, alter.AlgorithmType)
		require.Equal(t, planpb.AlterTableDrop_FOREIGN_KEY, alter.Actions[0].GetDrop().Typ)
	})
	t.Run("ambiguous generic drop rejects", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,unique key ck(v),constraint ck check(v>=0))")
		_, err := runOneStmt(mock, t, "alter table check_t drop constraint ck")
		require.ErrorContains(t, err, "ambiguous constraint name")
		p, err := runOneStmt(mock, t, "alter table check_t drop check ck")
		require.NoError(t, err)
		require.Empty(t, p.GetDdl().GetAlterTable().CopyTableDef.Checks)
		require.Len(t, p.GetDdl().GetAlterTable().CopyTableDef.Indexes, 1)
	})
	t.Run("ordinary index is not a constraint", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,key ck(v),constraint ck check(v>=0))")
		p, err := runOneStmt(mock, t, "alter table check_t drop constraint ck")
		require.NoError(t, err)
		require.Empty(t, p.GetDdl().GetAlterTable().CopyTableDef.Checks)
		require.Len(t, p.GetDdl().GetAlterTable().CopyTableDef.Indexes, 1)
	})
	t.Run("old protocol cannot author check metadata", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,constraint ck check(v>=0))")
		setPlanTestGlobalVariable(t, mock.ctxt.GetProcess().GetService(), moruntime.MOProtocolVersion, int64(defines.MORPCVersion7-1))
		_, err := runOneStmt(mock, t, "alter table check_t drop check ck")
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	})
}

func TestAlterCheckPreservesFinalCopyIndexNames(t *testing.T) {
	for _, index := range []string{"key w(v)", "unique key w(v)"} {
		for _, clause := range []string{"drop column v,add unique(w)", "add unique(w),drop column v"} {
			t.Run(index+"/"+clause, func(t *testing.T) {
				for _, check := range []string{"", ",add check(w>0)"} {
					mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,w int,"+index+")")
					p, err := runOneStmt(mock, t, "alter table check_t "+clause+check)
					require.NoError(t, err)
					alter := p.GetDdl().GetAlterTable()
					require.Equal(t, planpb.AlterTable_COPY, alter.AlgorithmType)
					require.Len(t, alter.CopyTableDef.Indexes, 1)
					require.True(t, alter.CopyTableDef.Indexes[0].Unique)
					require.Equal(t, "w", alter.CopyTableDef.Indexes[0].IndexName)
				}
			})
		}
	}
}

func TestAlterConstraintUsesFinalCopyNamespace(t *testing.T) {
	for _, test := range []struct {
		name, definition, clause, index string
		primary, ambiguous              bool
	}{
		{"renamed source identity survives old name reuse", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "rename column a to x,add column a int,drop column a,drop constraint u", "", false, true},
		{"changed source identity survives old name reuse", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "change column a x int,add column a int,drop column a,drop constraint u", "", false, true},
		{"new inline unique does not replace renamed source identity", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "rename column a to x,add column a int unique,drop column a,drop constraint u", "", false, true},
		{"renamed inline identity survives old name reuse", "id int primary key,v int,constraint a check(v>0)", "add column a int unique,rename column a to x,add column a int,drop column a,drop constraint a", "", false, true},
		{"changed inline identity survives old name reuse", "id int primary key,v int,constraint a check(v>0)", "add column a int unique,change column a x int,add column a int,drop column a,drop constraint a", "", false, true},
		{"multiple renames retain source identity", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "rename column a to x,rename column x to y,add column a int,drop column a,drop constraint u", "", false, true},
		{"multiple new generations retain inline identity", "id int primary key,v int,constraint a check(v>0)", "add column a int unique,rename column a to x,add column a int unique,rename column a to y,add column a int,drop column a,drop constraint a", "", false, true},
		{"fresh identity cannot collide with a legal source name", "id int primary key,__mo_alter_col_0 int,v int,unique key u(__mo_alter_col_0),constraint u check(v>0)", "add column a int,drop column a,drop constraint u", "", false, true},
		{"dropping renamed original clears only its unique identity", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "rename column a to x,add column a int,drop column x,drop constraint u", "", false, false},
		{"dropping changed original clears only its unique identity", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "change column a x int,add column a int,drop column x,drop constraint u", "", false, false},
		{"explicit check drop preserves renamed unique with name reuse", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "rename column a to x,add column a int,drop column a,drop check u", "u", false, false},
		{"explicit check drop preserves changed unique with name reuse", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "change column a x int,add column a int,drop column a,drop check u", "u", false, false},
		{"source unique cannot be hidden by future column drop", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "drop constraint u,drop column a", "", false, true},
		{"inline unique cannot be hidden by future column drop", "id int primary key,v int,constraint w check(v>0)", "add column w int unique,drop constraint w,drop column w", "", false, true},
		{"source unique cannot be hidden by future index drop", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "drop constraint u,drop index u", "", false, true},
		{"inline unique cannot be hidden by future index drop", "id int primary key,v int,constraint w check(v>0)", "add column w int unique,drop constraint w,drop index w", "", false, true},
		{"earlier source column drop removes unique ambiguity", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "drop column a,drop constraint u", "", false, false},
		{"partial unique survives future final column drop", "id int primary key,a int,b int,v int,unique key u(a,b),constraint u check(v>0)", "drop column a,drop constraint u,drop column b", "", false, true},
		{"complete earlier unique column removal clears ambiguity", "id int primary key,a int,b int,v int,unique key u(a,b),constraint u check(v>0)", "drop column a,drop column b,drop constraint u", "", false, false},
		{"renamed unique survives future column drop", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "rename column a to x,drop constraint u,drop column x", "", false, true},
		{"changed unique survives future column drop", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "change column a x int,drop constraint u,drop column x", "", false, true},
		{"earlier renamed unique column removal clears ambiguity", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "rename column a to x,drop column x,drop constraint u", "", false, false},
		{"earlier changed unique column removal clears ambiguity", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "change column a x int,drop column x,drop constraint u", "", false, false},
		{"inline unique", "id int primary key,v int,constraint w check(v>0)", "add column w int unique,drop constraint w", "", false, true},
		{"modify unique", "id int primary key,v int,w int,constraint w check(v>0)", "modify column w int unique,drop constraint w", "", false, true},
		{"change unique", "id int primary key,v int,x int,constraint w check(v>0)", "change column x w int unique,drop constraint w", "", false, true},
		{"inline primary", "id int,v int,constraint `PRIMARY` check(v>0)", "add column w int primary key,drop constraint `PRIMARY`", "", false, true},
		{"source primary cannot be hidden by later inline replacement", "id int primary key,v int,constraint `PRIMARY` check(v>0)", "drop constraint `PRIMARY`,drop primary key,add column w int primary key default 1", "", false, true},
		{"earlier inline primary cannot be hidden by later replacement", "id int,v int,constraint `PRIMARY` check(v>0)", "add column x int primary key default 1,drop constraint `PRIMARY`,drop primary key,add column w int primary key default 1", "", false, true},
		{"table replacement preserves source primary ambiguity", "id int primary key,v int,constraint `PRIMARY` check(v>0)", "drop constraint `PRIMARY`,drop primary key,add column w int default 1,add primary key(w)", "", false, true},
		{"table replacement preserves earlier inline primary ambiguity", "id int,v int,constraint `PRIMARY` check(v>0)", "add column x int primary key default 1,drop constraint `PRIMARY`,drop primary key,add column w int default 1,add primary key(w)", "", false, true},
		{"explicit primary drop precedes generic check drop", "id int primary key,v int,constraint `PRIMARY` check(v>0)", "drop primary key,drop constraint `PRIMARY`,add column w int primary key default 1", "", true, false},
		{"explicit check drop precedes primary replacement", "id int primary key,v int,constraint `PRIMARY` check(v>0)", "drop check `PRIMARY`,drop primary key,add column w int primary key default 1", "", true, false},
		{"earlier inline primary can be explicitly dropped", "id int,v int,constraint `PRIMARY` check(v>0)", "add column x int primary key default 1,drop primary key,drop constraint `PRIMARY`,add column w int primary key default 1", "", true, false},
		{"earlier inline primary ambiguity survives partial composite drop", "id int,v int,constraint `PRIMARY` check(v>0)", "add primary key(id,v),drop column id,drop constraint `PRIMARY`", "", false, true},
		{"earlier table primary removed by dropping all columns", "id int,v int,constraint `PRIMARY` check(v>0)", "add primary key(id),drop column id,drop constraint `PRIMARY`", "", false, false},
		{"renamed primary column removal clears ambiguity", "id int primary key,v int,constraint `PRIMARY` check(v>0)", "rename column id to x,drop column x,drop constraint `PRIMARY`", "", false, false},
		{"changed primary column removal clears ambiguity", "id int primary key,v int,constraint `PRIMARY` check(v>0)", "change column id x int,drop column x,drop constraint `PRIMARY`", "", false, false},
		{"later column removal cannot hide current primary", "id int primary key,v int,constraint `PRIMARY` check(v>0)", "drop constraint `PRIMARY`,drop column id", "", false, true},
		{"modify primary", "id int,v int,w int,constraint `PRIMARY` check(v>0)", "modify column w int primary key,drop constraint `PRIMARY`", "", false, true},
		{"change primary", "id int,v int,x int,constraint `PRIMARY` check(v>0)", "change column x w int primary key,drop constraint `PRIMARY`", "", false, true},
		{"inline unique suffix", "id int primary key,v int,key w(v),constraint w_2 check(id>0)", "add column w int unique,drop constraint w_2", "", false, true},
		{"deferred unique before inline", "id int primary key,v int,constraint w_2 check(v>0)", "add unique(w),add column w int unique,drop constraint w_2", "", false, true},
		{"deferred unique after inline", "id int primary key,v int,constraint w_2 check(v>0)", "add column w int unique,add unique(w),drop constraint w_2", "", false, true},
		{"inline name survives later source column drop", "id int primary key,v int,key w(v),constraint w_2 check(id>0)", "add column w int unique,drop column v,drop constraint w_2", "", false, true},
		{"inline name reuses earlier source column drop", "id int primary key,v int,key w(v),constraint w_2 check(id>0)", "drop column v,add column w int unique,drop constraint w_2", "w", false, false},
		{"inline name survives later functional index drop", "id int primary key,v int,key w((v+1)),constraint w_2 check(v>0)", "add column w int unique,drop index w,drop constraint w_2", "", false, true},
		{"inline name reuses earlier functional index drop", "id int primary key,v int,key w((v+1)),constraint w_2 check(v>0)", "drop index w,add column w int unique,drop constraint w_2", "w", false, false},
		{"dropped inline unique no longer conflicts", "id int primary key,v int,constraint w check(v>0)", "add column w int unique,drop column w,drop constraint w", "", false, false},
		{"dropped inline primary no longer conflicts", "id int,v int,constraint `PRIMARY` check(v>0)", "add column w int primary key,drop column w,drop constraint `PRIMARY`", "", false, false},
		{"renamed inline unique keeps name", "id int primary key,v int,constraint w check(v>0)", "add column w int unique,rename column w to x,drop constraint w", "", false, true},
		{"generic check drop precedes inline unique", "id int primary key,v int,constraint w check(v>0)", "drop constraint w,add column w int unique", "w", false, false},
		{"explicit check drop retains inline unique", "id int primary key,v int,constraint w check(v>0)", "add column w int unique,drop check w", "w", false, false},
		{"explicit check drop retains inline primary", "id int,v int,constraint `PRIMARY` check(v>0)", "add column w int primary key,drop check `PRIMARY`", "", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t("+test.definition+")")
			source := mock.ctxt.tables["check_t"]
			before := proto.Clone(source).(*planpb.TableDef)
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "alter table check_t "+test.clause, 1)
			require.NoError(t, err)
			defer stmt.Free()
			astBefore := tree.String(stmt, dialect.MYSQL)
			for range 2 {
				p, err := BuildPlan(&mock.ctxt, stmt, false)
				if test.ambiguous {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
					require.ErrorContains(t, err, "ambiguous constraint name")
				} else {
					require.NoError(t, err)
					copy := p.GetDdl().GetAlterTable().CopyTableDef
					require.Empty(t, copy.Checks)
					if test.index != "" {
						require.Len(t, copy.Indexes, 1)
						require.Equal(t, test.index, copy.Indexes[0].IndexName)
						require.True(t, copy.Indexes[0].Unique)
					}
					if test.primary {
						require.Equal(t, "w", copy.Pkey.PkeyColName)
					}
				}
				require.Equal(t, before, source)
				require.Equal(t, astBefore, tree.String(stmt, dialect.MYSQL))
			}
		})
	}
	// Ordinary COPY DROP INDEX remains unsupported. Resolution must still
	// select the right constraint before the COPY owner rejects execution.
	for _, test := range []struct {
		name, definition, clause string
		want                     tree.AlterTableDropType
	}{
		{"earlier source index drop removes ambiguity", "id int primary key,a int,v int,unique key u(a),constraint u check(v>0)", "drop index u,drop constraint u", tree.AlterTableDropCheck},
		{"earlier inline index drop removes ambiguity", "id int primary key,v int,constraint w check(v>0)", "add column w int unique,drop index w,drop constraint w", tree.AlterTableDropCheck},
		{"unique drop precedes future column removal", "id int primary key,a int,v int,unique key u(a)", "drop constraint u,drop column a", tree.AlterTableDropIndex},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t("+test.definition+")")
			source := mock.ctxt.tables["check_t"]
			before := proto.Clone(source).(*planpb.TableDef)
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "alter table check_t "+test.clause, 1)
			require.NoError(t, err)
			defer stmt.Free()
			astBefore := tree.String(stmt, dialect.MYSQL)
			for range 2 {
				options, err := resolveAlterConstraintOptions(&mock.ctxt, source, stmt.(*tree.AlterTable).Options, analyzeAlterColumnBindings(source, stmt.(*tree.AlterTable).Options))
				require.NoError(t, err)
				for i, option := range stmt.(*tree.AlterTable).Options {
					if drop, ok := option.(*tree.AlterOptionDrop); ok && drop.Typ == tree.AlterTableDropConstraint {
						require.Equal(t, test.want, options[i].(*tree.AlterOptionDrop).Typ)
					}
				}
				_, err = BuildPlan(&mock.ctxt, stmt, false)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
				require.ErrorContains(t, err, "combined COPY DROP INDEX requires a functional index")
				require.Equal(t, before, source)
				require.Equal(t, astBefore, tree.String(stmt, dialect.MYSQL))
			}
		})
	}
	t.Run("dropped primary no longer conflicts with check", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,w int,constraint `PRIMARY` check(w>0))")
		p, err := runOneStmt(mock, t, "alter table check_t drop column id,drop constraint `PRIMARY`")
		require.NoError(t, err)
		require.Empty(t, p.GetDdl().GetAlterTable().CopyTableDef.Checks)
	})
	t.Run("declared primary with no column list still conflicts", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,constraint `PRIMARY` check(v>0))")
		mock.ctxt.tables["check_t"].Pkey.Names = nil
		_, err := runOneStmt(mock, t, "alter table check_t drop constraint `PRIMARY`")
		require.ErrorContains(t, err, "ambiguous constraint name")
	})
	t.Run("partially dropped primary still conflicts with check", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int,v int,w int,primary key(id,v),constraint `PRIMARY` check(w>0))")
		_, err := runOneStmt(mock, t, "alter table check_t drop column id,drop constraint `PRIMARY`")
		require.ErrorContains(t, err, "ambiguous constraint name")
	})
	for _, test := range []struct{ index, clause string }{
		{"key w(v)", "drop column v,add unique(w),drop constraint w_2"},
		{"unique key w(v)", "drop column v,add unique(w),drop constraint w_2"},
		{"key w(v)", "add unique(w),drop column v,drop constraint w_2"},
		{"key w(v)", "rename column v to z,drop column z,add unique(w),drop constraint w_2"},
		{"key w(v)", "rename column v to v,drop column v,add unique(w),drop constraint w_2"},
		{"key w((v+1))", "drop index w,add unique(w),drop constraint w_2"},
		{"key w((v+1))", "add unique(w),drop index w,drop constraint w_2"},
	} {
		t.Run(test.index+"/"+test.clause, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,w int,"+test.index+",constraint w_2 check(w>0))")
			source := mock.ctxt.tables["check_t"]
			before := proto.Clone(source).(*planpb.TableDef)
			p, err := runOneStmt(mock, t, "alter table check_t "+test.clause)
			require.NoError(t, err)
			alter := p.GetDdl().GetAlterTable()
			require.Empty(t, alter.CopyTableDef.Checks)
			require.Len(t, alter.CopyTableDef.Indexes, 1)
			require.True(t, alter.CopyTableDef.Indexes[0].Unique)
			require.Equal(t, "w", alter.CopyTableDef.Indexes[0].IndexName)
			require.Equal(t, before, source)
		})
	}
	t.Run("partially dropped unique still occupies name", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,w int,unique key w(v,w),constraint w_2 check(w>0))")
		_, err := runOneStmt(mock, t, "alter table check_t drop column v,add unique(w),drop constraint w_2")
		require.ErrorContains(t, err, "ambiguous constraint name")
	})
	t.Run("copy retains pending names across source index drop", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,w int,key w((v+1)),constraint w_3 check(w>0))")
		p, err := runOneStmt(mock, t, "alter table check_t add unique(w),drop index w,add unique(w),drop constraint w_3")
		require.NoError(t, err)
		require.Empty(t, p.GetDdl().GetAlterTable().CopyTableDef.Checks)
		indexes := p.GetDdl().GetAlterTable().CopyTableDef.Indexes
		require.Len(t, indexes, 2)
		require.Equal(t, "w", indexes[0].IndexName)
		require.Equal(t, "w_2", indexes[1].IndexName)
	})
}

// Generic and explicit spellings must reach the same physical-key admission.
func TestAlterConstraintUsesResolvedIndexAdmission(t *testing.T) {
	for _, test := range []struct {
		name, collation, clause string
		reject, copy            bool
	}{
		{"native explicit index", "utf8mb4_unicode_ci", "drop index u", true, false},
		{"native explicit key", "utf8mb4_unicode_ci", "drop key u", true, false},
		{"native generic unique", "utf8mb4_unicode_ci", "drop constraint u", true, false},
		{"native explicit primary", "utf8mb4_unicode_ci", "drop primary key", true, false},
		{"native generic primary", "utf8mb4_unicode_ci", "drop constraint `PRIMARY`", true, false},
		{"native generic check", "utf8mb4_unicode_ci", "drop constraint ck", false, true},
		{"legacy generic unique", "utf8mb4_bin", "drop constraint u", false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v varchar(8) collate "+test.collation+",n int,unique key u(v),constraint ck check(n>0))")
			source := mock.ctxt.tables["check_t"]
			before := proto.Clone(source).(*planpb.TableDef)
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, "alter table check_t "+test.clause, 1)
			require.NoError(t, err)
			defer stmt.Free()
			astBefore := tree.String(stmt, dialect.MYSQL)
			for range 2 {
				p, err := BuildPlan(&mock.ctxt, stmt, false)
				if test.reject {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
					require.ErrorContains(t, err, "index mutation on native Unicode unique tables")
				} else {
					require.NoError(t, err)
					alter := p.GetDdl().GetAlterTable()
					if test.copy {
						require.Equal(t, planpb.AlterTable_COPY, alter.AlgorithmType)
						require.Empty(t, alter.CopyTableDef.Checks)
						require.Len(t, alter.CopyTableDef.Indexes, 1)
						require.Equal(t, source.KeyFormat, alter.CopyTableDef.KeyFormat)
					} else {
						require.Equal(t, planpb.AlterTable_INPLACE, alter.AlgorithmType)
						require.Equal(t, planpb.AlterTableDrop_INDEX, alter.Actions[0].GetDrop().Typ)
						require.Equal(t, "u", alter.Actions[0].GetDrop().Name)
					}
				}
				require.Equal(t, before, source)
				require.Equal(t, astBefore, tree.String(stmt, dialect.MYSQL))
			}
		})
	}
}

func TestAlterCopyReplacementIndexBindings(t *testing.T) {
	for _, tc := range []struct {
		name, definition, clause string
		parts                    []string
	}{
		{"single unique", "id int primary key,v int,unique key u(v)", "drop column v,add column v int default 7", []string{"v"}},
		{"composite nullable", "id int primary key,a int,v int,unique key u(a,v)", "drop column v,add column v int default null", []string{"a", "v"}},
		{"replacement renamed", "id int primary key,v int,unique key u(v)", "drop column v,add column v int,rename column v to x", []string{"x"}},
		{"replacement changed", "id int primary key,v int,unique key u(v)", "drop column v,add column v int,change column v x bigint", []string{"x"}},
		{"fresh rename arrives", "id int primary key,v int,unique key u(v)", "drop column v,add column tmp int,rename column tmp to v", []string{"v"}},
		{"original rename does not replace", "id int primary key,v int,w int,unique key u(v)", "drop column v,rename column w to v", nil},
		{"renamed old binding removed", "id int primary key,v int,unique key u(v)", "rename column v to x,drop column x,add column v int", nil},
		{"renamed binding replaced", "id int primary key,v int,unique key u(v)", "rename column v to x,drop column x,add column x int", []string{"x"}},
		{"source survives name reuse", "id int primary key,v int,unique key u(v)", "rename column v to x,add column v int,drop column v", []string{"x"}},
		{"replacement removed", "id int primary key,v int,unique key u(v)", "drop column v,add column v int,drop column v", nil},
		{"replacement replaced again", "id int primary key,v int,unique key u(v)", "drop column v,add column v int,drop column v,add column v int", []string{"v"}},
		{"genuine composite removal", "id int primary key,a int,v int,unique key u(a,v)", "drop column v", []string{"a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t("+tc.definition+")")
			source := mock.ctxt.tables["check_t"]
			before := proto.Clone(source)
			p, err := runOneStmt(mock, t, "alter table check_t "+tc.clause)
			require.NoError(t, err)
			alter := p.GetDdl().GetAlterTable()
			require.True(t, proto.Equal(before, source), "planning must not mutate catalog metadata")
			if tc.parts == nil {
				require.Empty(t, alter.CopyTableDef.Indexes)
				return
			}
			require.Len(t, alter.CopyTableDef.Indexes, 1)
			index := alter.CopyTableDef.Indexes[0]
			require.Equal(t, "u", index.IndexName)
			require.Equal(t, tc.parts, index.Parts)
			require.False(t, alter.Options.SkipIndexesCopy["u"], "changed keys must rebuild")
			require.False(t, alter.Options.SkipUniqueIdxDedup["u"], "replacement values require dedup")
		})
	}
}

func TestAlterConstraintReplacementKeepsUniqueNamespace(t *testing.T) {
	for _, clause := range []string{
		"drop column v,add column v int,drop constraint u",
		"drop column v,add column tmp int,rename column tmp to v,drop constraint u",
	} {
		t.Run(clause, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,w int,unique key u(v),constraint u check(w>0))")
			_, err := runOneStmt(mock, t, "alter table check_t "+clause)
			require.ErrorContains(t, err, "ambiguous")
		})
	}
	t.Run("replacement removed releases unique namespace", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,w int,unique key u(v),constraint u check(w>0))")
		p, err := runOneStmt(mock, t, "alter table check_t drop column v,add column v int,drop column v,drop constraint u")
		require.NoError(t, err)
		require.Empty(t, p.GetDdl().GetAlterTable().CopyTableDef.Indexes)
		require.Empty(t, p.GetDdl().GetAlterTable().CopyTableDef.Checks)
	})
}

func TestAlterConstraintMergedReplacementBindingsReleaseNames(t *testing.T) {
	mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,a int,b int,w int,unique key u(a),unique key j(b),constraint u check(w>0))")
	p, err := runOneStmt(mock, t, "alter table check_t drop column a,add column a int,drop column b,rename column a to b,drop column b,drop constraint u")
	require.NoError(t, err)
	require.Empty(t, p.GetDdl().GetAlterTable().CopyTableDef.Indexes)
	require.Empty(t, p.GetDdl().GetAlterTable().CopyTableDef.Checks)
}

func TestAlterCopyReplacementKeepsCompleteIndexDefinitions(t *testing.T) {
	for _, tc := range []struct{ name, definition, clause string }{
		{"ordinary prefix", "id bigint primary key,v varchar(20),key ix(v(5))", "drop column v,add column v varchar(20)"},
		{"classic fulltext", "id bigint primary key,body text,fulltext key ft(body)", "drop column body,add column body text"},
		{"vector key and include payload", "id bigint primary key,v vecf32(3),tag int,index iv using ivfflat(v) lists=1 op_type 'vector_l2_ops' include(tag)", "drop column v,add column v vecf32(3),drop column tag,add column tag int"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t("+tc.definition+")")
			source := mock.ctxt.tables["check_t"]
			before := proto.Clone(source).(*TableDef)
			p, err := runOneStmt(mock, t, "alter table check_t "+tc.clause)
			require.NoError(t, err)
			alter := p.GetDdl().GetAlterTable()
			require.Len(t, alter.CopyTableDef.Indexes, len(before.Indexes))
			for i, index := range alter.CopyTableDef.Indexes {
				// The fixture supplies authoritative visibility=true. All other fields,
				// including aliases, prefix and INCLUDE metadata, must survive unchanged.
				before.Indexes[i].Visible = index.Visible
				require.True(t, proto.Equal(before.Indexes[i], index), "physical index %d", i)
				require.False(t, alter.Options.SkipIndexesCopy[index.IndexName])
			}
		})
	}
}

func TestAlterConstraintMergedIncludeBindingReleasesIndexName(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t(id bigint primary key,a int,b int,v vecf32(3),index u using ivfflat(v) lists=1 op_type 'vector_l2_ops' include(a),constraint u_2 check(id>0))")
			source := mock.ctxt.tables["check_t"]
			if legacy {
				for _, index := range source.Indexes {
					index.IncludedColumns = nil
				}
			}
			before := proto.Clone(source)
			p, err := runOneStmt(mock, t, "alter table check_t drop column a,add column a int,drop column b,rename column a to b,drop column b,add column u int unique,drop constraint u_2")
			require.NoError(t, err)
			require.True(t, proto.Equal(before, source))
			alter := p.GetDdl().GetAlterTable()
			require.Empty(t, alter.CopyTableDef.Checks)
			require.Len(t, alter.CopyTableDef.Indexes, 1)
			require.Equal(t, "u", alter.CopyTableDef.Indexes[0].IndexName)
			require.Equal(t, []string{"u"}, alter.CopyTableDef.Indexes[0].Parts)
		})
	}
}
