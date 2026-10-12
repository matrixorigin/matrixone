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
		{"drop check __mo_chk_1, add check(v>0)", []string{"__mo_chk_2", "__mo_chk_3"}},
		{"drop check __mo_chk_1, drop check __mo_chk_2", nil},
		{"drop check __mo_chk_1, add constraint __mo_chk_1 check(v>0)", []string{"__mo_chk_2", "__mo_chk_1"}},
		{"add constraint transient_ck check(v>0), drop constraint transient_ck", []string{"__mo_chk_1", "__mo_chk_2"}},
	} {
		for _, legacy := range []bool{false, true} {
			t.Run(test.clause+map[bool]string{false: "/structured", true: "/legacy"}[legacy], func(t *testing.T) {
				mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,v int, check(v>=0), check(v<100))")
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
	t.Run("dropped primary no longer conflicts with check", func(t *testing.T) {
		mock := newCheckAlterOptimizer(t, "create table check_t(id int primary key,w int,constraint `PRIMARY` check(w>0))")
		p, err := runOneStmt(mock, t, "alter table check_t drop column id,drop constraint `PRIMARY`")
		require.NoError(t, err)
		require.Empty(t, p.GetDdl().GetAlterTable().CopyTableDef.Checks)
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
