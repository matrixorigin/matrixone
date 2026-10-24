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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
		{"legacy generic unique", "utf32_bin", "drop constraint u", false, false},
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

func TestAlterDropColumnRemovesDependentChecks(t *testing.T) {
	for _, test := range []struct {
		name    string
		ddl     string
		clause  string
		want    []string
		wantErr string
	}{
		{
			name:   "check on the dropped column follows it",
			ddl:    "create table check_t(id int primary key,v int,constraint ck_v check(v>0))",
			clause: "drop column v",
		},
		{
			name:   "unrelated check survives",
			ddl:    "create table check_t(id int primary key,v int,w int,constraint ck_v check(v>0),constraint ck_w check(w>0))",
			clause: "drop column v",
			want:   []string{"ck_w"},
		},
		{
			// MySQL auto-drops only CHECKs referencing the dropped column alone;
			// a CHECK spanning other columns rejects the drop instead
			// (prepare_check_constraints_for_alter, ER_DEPENDENT_BY_CHECK_CONSTRAINT).
			name:    "multi column check referencing the dropped column rejects the drop",
			ddl:     "create table check_t(id int primary key,v int,w int,constraint ck_mix check(v>0 and w>0),constraint ck_w check(w>0))",
			clause:  "drop column v",
			wantErr: "Check constraint 'ck_mix' uses column 'v'",
		},
		{
			name:    "multi column check rejects dropping every referenced column together",
			ddl:     "create table check_t(id int primary key,v int,w int,constraint ck_mix check(v>0 and w>0))",
			clause:  "drop column v, drop column w",
			wantErr: "Check constraint 'ck_mix' uses column 'v'",
		},
		{
			name:    "function expression over kept columns still rejects the drop",
			ddl:     "create table check_t(id int primary key,v int,w int,constraint ck_fn check(abs(v)+w>0))",
			clause:  "drop column v",
			wantErr: "Check constraint 'ck_fn' uses column 'v'",
		},
		{
			name:   "checks on each dropped column follow them together",
			ddl:    "create table check_t(id int primary key,v int,w int,constraint ck_v check(v>0),constraint ck_w check(w>0),constraint ck_id check(id>0))",
			clause: "drop column v, drop column w",
			want:   []string{"ck_id"},
		},
		{
			name:   "function expression referencing the dropped column follows it",
			ddl:    "create table check_t(id int primary key,v int,w int,constraint ck_fn check(abs(v)>0),constraint ck_w check(abs(w)>0))",
			clause: "drop column v",
			want:   []string{"ck_w"},
		},
		{
			name:   "near name reference survives",
			ddl:    "create table check_t(id int primary key,v int,v2 int,constraint ck_v2 check(v2>0))",
			clause: "drop column v",
			want:   []string{"ck_v2"},
		},
		{
			name:   "re-added column does not resurrect the removed check",
			ddl:    "create table check_t(id int primary key,v int,constraint ck_v check(v>0))",
			clause: "drop column v, add column v int",
		},
		{
			name:   "explicit co-drop after the column clause",
			ddl:    "create table check_t(id int primary key,v int,constraint ck_v check(v>0))",
			clause: "drop column v, drop check ck_v",
		},
		{
			name:   "explicit co-drop before the column clause",
			ddl:    "create table check_t(id int primary key,v int,constraint ck_v check(v>0))",
			clause: "drop check ck_v, drop column v",
		},
		{
			// The generic spelling resolves the same CHECK identity and must
			// co-drop in either clause order, exactly like DROP CHECK above.
			name:   "generic constraint co-drop after the column clause",
			ddl:    "create table check_t(id int primary key,v int,constraint ck_v check(v>0))",
			clause: "drop column v, drop constraint ck_v",
		},
		{
			name:   "generic constraint co-drop before the column clause",
			ddl:    "create table check_t(id int primary key,v int,constraint ck_v check(v>0))",
			clause: "drop constraint ck_v, drop column v",
		},
		{
			// The rename runs before the drop, so the CHECK follows its column
			// to x; the re-added v is a fresh column. Dropping it must keep the
			// constraint bound to the surviving x.
			name:   "identity rename keeps the checked column constraint",
			ddl:    "create table check_t(id int primary key,v int,constraint ck_v check(v>0))",
			clause: "rename column v to x, add column v int, drop column v",
			want:   []string{"ck_v"},
		},
		{
			name:   "identity change keeps the checked column constraint",
			ddl:    "create table check_t(id int primary key,v int,constraint ck_v check(v>0))",
			clause: "change column v x int, add column v int, drop column v",
			want:   []string{"ck_v"},
		},
		{
			name:    "generated column dependency still rejects the drop",
			ddl:     "create table check_t(id int primary key,v int,g int generated always as (v+1) stored,constraint ck_v check(v>0))",
			clause:  "drop column v",
			wantErr: "generated column 'g' depends on it",
		},
	} {
		for _, legacy := range []bool{false, true} {
			t.Run(test.name+map[bool]string{false: "/structured", true: "/legacy"}[legacy], func(t *testing.T) {
				mock := newCheckAlterOptimizer(t, test.ddl)
				source := mock.ctxt.tables["check_t"]
				if legacy {
					source.Checks = nil
				}
				before := proto.Clone(source).(*planpb.TableDef)
				p, err := runOneStmt(mock, t, "alter table check_t "+test.clause)
				if test.wantErr != "" {
					require.ErrorContains(t, err, test.wantErr)
					require.Equal(t, before, source)
					return
				}
				require.NoError(t, err)
				alter := p.GetDdl().GetAlterTable()
				require.Equal(t, planpb.AlterTable_COPY, alter.AlgorithmType)
				require.Len(t, alter.CopyTableDef.Checks, len(test.want))
				for i, name := range test.want {
					require.Equal(t, name, alter.CopyTableDef.Checks[i].Name)
				}
				// The serialized copy CREATE is exactly what the COPY executor
				// replans and installs; it must bind against the final schema
				// and carry precisely the surviving constraints.
				rebuilt, err := runOneStmt(mock, t, alter.CreateTmpTableSql)
				require.NoError(t, err, "copy DDL must replan: %s", alter.CreateTmpTableSql)
				rebuiltChecks := rebuilt.GetDdl().GetCreateTable().TableDef.Checks
				require.Len(t, rebuiltChecks, len(test.want))
				for i, name := range test.want {
					require.Equal(t, name, rebuiltChecks[i].Name)
				}
				if len(test.want) == 0 {
					require.NotContains(t, alter.CreateTmpTableSql, "CHECK (")
				}
				require.Equal(t, before, source, "planning must not rewrite the catalog source")
			})
		}
	}
}

func TestAlterDropColumnCheckNameMatchingBoundaries(t *testing.T) {
	// A string literal or a qualified-looking token that only spells the
	// column name inside a constant must not remove the constraint.
	mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,s varchar(20),constraint ck_s check(s <> 'v'))")
	p, err := runOneStmt(mock, t, "alter table check_t drop column v")
	require.NoError(t, err)
	alter := p.GetDdl().GetAlterTable()
	require.Len(t, alter.CopyTableDef.Checks, 1)
	require.Equal(t, "ck_s", alter.CopyTableDef.Checks[0].Name)
	rebuilt, err := runOneStmt(mock, t, alter.CreateTmpTableSql)
	require.NoError(t, err)
	require.Len(t, rebuilt.GetDdl().GetCreateTable().TableDef.Checks, 1)
}

func TestAlterDropColumnReleasesCheckNamesForReplacement(t *testing.T) {
	// MySQL releases the name of an auto-dropped CHECK before admitting new
	// constraints (prepare_check_constraints_for_alter consults
	// dropped_cc_names), so a same-ALTER replacement succeeds in either clause
	// order and binds against the post-drop schema.
	for _, test := range []struct {
		name    string
		ddl     string
		clause  string
		want    []string
		wantErr string
	}{
		{
			name:   "replacement reuses the auto-dropped name",
			ddl:    "create table check_t(id int primary key,v int,w int,constraint ck check(v>0))",
			clause: "drop column v, add constraint ck check(w>0)",
			want:   []string{"ck"},
		},
		{
			name:   "release is independent of clause order",
			ddl:    "create table check_t(id int primary key,v int,w int,constraint ck check(v>0))",
			clause: "add constraint ck check(w>0), drop column v",
			want:   []string{"ck"},
		},
		{
			name:    "surviving names still reject the duplicate",
			ddl:     "create table check_t(id int primary key,v int,w int,constraint ck_w check(w>0))",
			clause:  "drop column v, add constraint ck_w check(w>0)",
			wantErr: "duplicate check constraint name 'ck_w'",
		},
		{
			name:    "multi column dependents are not released",
			ddl:     "create table check_t(id int primary key,v int,w int,constraint ck check(v>0 and w>0))",
			clause:  "drop column v, add constraint ck check(w>0)",
			wantErr: "duplicate check constraint name 'ck'",
		},
		{
			name:    "two replacement adds still collide",
			ddl:     "create table check_t(id int primary key,v int,w int,constraint ck check(v>0))",
			clause:  "drop column v, add constraint ck2 check(w>0), add constraint ck2 check(w>0)",
			wantErr: "duplicate check constraint name 'ck2'",
		},
		{
			// The old CHECK follows the rename to x and survives the drop, so
			// its name was never released; a same-ALTER replacement must still
			// fail the duplicate admission.
			name:    "identity survival keeps the name taken",
			ddl:     "create table check_t(id int primary key,v int,w int,constraint ck check(v>0))",
			clause:  "rename column v to x, add column v int, drop column v, add constraint ck check(x>0)",
			wantErr: "duplicate check constraint name 'ck'",
		},
		{
			// Release resolves the identity at the drop clause: the rename
			// redirected the CHECK to x, so dropping x auto-drops it and the
			// replacement may take its name.
			name:   "identity release follows the renamed checked column",
			ddl:    "create table check_t(id int primary key,v int,w int,constraint ck check(v>0))",
			clause: "rename column v to x, drop column x, add constraint ck check(w>0)",
			want:   []string{"ck"},
		},
	} {
		for _, legacy := range []bool{false, true} {
			t.Run(test.name+map[bool]string{false: "/structured", true: "/legacy"}[legacy], func(t *testing.T) {
				mock := newCheckAlterOptimizer(t, test.ddl)
				source := mock.ctxt.tables["check_t"]
				if legacy {
					source.Checks = nil
				}
				before := proto.Clone(source).(*planpb.TableDef)
				p, err := runOneStmt(mock, t, "alter table check_t "+test.clause)
				if test.wantErr != "" {
					require.ErrorContains(t, err, test.wantErr)
					require.Equal(t, before, source)
					return
				}
				require.NoError(t, err)
				alter := p.GetDdl().GetAlterTable()
				require.Equal(t, planpb.AlterTable_COPY, alter.AlgorithmType)
				require.Len(t, alter.CopyTableDef.Checks, len(test.want))
				for i, name := range test.want {
					require.Equal(t, name, alter.CopyTableDef.Checks[i].Name)
				}
				// The replacement must bind against the post-drop schema: the
				// serialized copy CREATE would fail to replan if the surviving
				// CHECK still referenced the dropped column.
				rebuilt, err := runOneStmt(mock, t, alter.CreateTmpTableSql)
				require.NoError(t, err, "copy DDL must replan: %s", alter.CreateTmpTableSql)
				rebuiltChecks := rebuilt.GetDdl().GetCreateTable().TableDef.Checks
				require.Len(t, rebuiltChecks, len(test.want))
				for i, name := range test.want {
					require.Equal(t, name, rebuiltChecks[i].Name)
				}
				require.Equal(t, before, source, "planning must not rewrite the catalog source")
			})
		}
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

func TestAlterDropColumnReplacementCheckBindsAuthoredColumn(t *testing.T) {
	// Execution wraps the temporary COPY CREATE in WithAlterTableDDLReplay
	// (pkg/sql/compile/alter.go). The persisted-expression replay preserves
	// inherited CHECKs; a same-statement replacement was authored in this
	// ALTER and must bind its own expression even when the dropped original
	// shares its name and a positionally identical bound expression.
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "structured", true: "legacy"}[legacy], func(t *testing.T) {
			mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int,w int,constraint ck check(v>0))")
			source := mock.ctxt.tables["check_t"]
			if legacy {
				source.Checks = nil
			}
			before := proto.Clone(source).(*planpb.TableDef)
			p, err := runOneStmt(mock, t, "alter table check_t drop column v, add column v int default 1, add constraint ck check(w>0)")
			require.NoError(t, err)
			alter := p.GetDdl().GetAlterTable()
			parent, err := WithAlterTableDDLReplay(t.Context(), mock.ctxt.DefaultDatabase(), alter.TableDef, alter.CopyTableDef)
			require.NoError(t, err)
			mock.ctxt.SetContext(parent)
			rebuilt, err := runOneStmt(mock, t, alter.CreateTmpTableSql)
			require.NoError(t, err, "copy DDL must replan: %s", alter.CreateTmpTableSql)
			def := rebuilt.GetDdl().GetCreateTable().TableDef
			require.Len(t, def.Checks, 1)
			require.Equal(t, "ck", def.Checks[0].Name)
			positions := make(map[string]int32)
			var visible int32
			for _, col := range def.Cols {
				if col.Hidden || col.Name == catalog.Row_ID {
					continue
				}
				positions[col.Name] = visible
				visible++
			}
			require.Equal(t, int32(3), visible)
			// The rebuilt definition is (id, w, v): the replacement was
			// authored over w and must bind w, never the recreated v.
			ref := def.Checks[0].Check.GetF().GetArgs()[0].GetCol()
			require.NotNil(t, ref)
			require.Equal(t, positions["w"], ref.ColPos, "replacement CHECK must bind the authored column w")
			require.Equal(t, before, source, "planning must not rewrite the catalog source")
		})
	}
}

func findCheckDivision(expr *planpb.Expr) *planpb.Expr {
	if f := expr.GetF(); f != nil {
		if id, _ := function.DecodeOverloadID(f.Func.Obj); id == function.DIV {
			return expr
		}
		for _, arg := range f.Args {
			if div := findCheckDivision(arg); div != nil {
				return div
			}
		}
	}
	return nil
}

func TestAlterRenameRetainedCheckPreservesBoundPrecision(t *testing.T) {
	// A CHECK retained through RENAME still belongs to the source definition:
	// the persisted-expression replay must preserve its creation-time bound
	// expression (decimal division scale fixed by div_precision_increment at
	// CREATE), not rebind it under the session setting active when the COPY
	// executes. Execution wraps the temporary CREATE in
	// WithAlterTableDDLReplay (pkg/sql/compile/alter.go).
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	ddl := "create table check_t(id int primary key,a decimal(10,2),b decimal(10,2),v int,constraint ck check(a/b>0.3333333))"
	createdStmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, ddl, 1)
	require.NoError(t, err)
	defer createdStmt.Free()
	created, err := BuildPlan(divPrecisionCompilerContext{CompilerContext: &mock.ctxt, increment: 10}, createdStmt, false)
	require.NoError(t, err)
	source := created.GetDdl().GetCreateTable().TableDef
	source.TblId = 25387
	source.Createsql = ddl
	mock.ctxt.tables[source.Name] = source
	mock.ctxt.objects[source.Name] = &ObjectRef{SchemaName: mock.ctxt.DefaultDatabase(), ObjName: source.Name}
	require.Len(t, source.Checks, 1)
	want := types.New(types.T_decimal128, 22, 12)
	require.Equal(t, want, makeTypeByPlan2Expr(findCheckDivision(source.Checks[0].Check)),
		"creation binding must use the creating session precision")

	// The ALTER runs under the default increment; the replayed expression must
	// not be rebound from the rewritten SQL text at this setting.
	before := proto.Clone(source).(*planpb.TableDef)
	p, err := runOneStmt(mock, t, "alter table check_t rename column a to x, drop column v")
	require.NoError(t, err)
	alter := p.GetDdl().GetAlterTable()
	parent, err := WithAlterTableDDLReplay(t.Context(), mock.ctxt.DefaultDatabase(), alter.TableDef, alter.CopyTableDef)
	require.NoError(t, err)
	mock.ctxt.SetContext(parent)
	rebuilt, err := runOneStmt(mock, t, alter.CreateTmpTableSql)
	require.NoError(t, err, "copy DDL must replan: %s", alter.CreateTmpTableSql)
	def := rebuilt.GetDdl().GetCreateTable().TableDef
	require.Len(t, def.Checks, 1)
	require.Equal(t, "ck", def.Checks[0].Name)
	require.Contains(t, def.Checks[0].OriginSql, "x", "the declared text follows the rename")
	require.Equal(t, want, makeTypeByPlan2Expr(findCheckDivision(def.Checks[0].Check)),
		"renamed retained CHECK must keep its creation-time bound division scale")
	require.Equal(t, before, source, "planning must not rewrite the catalog source")
}
