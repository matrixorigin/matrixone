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

package frontend

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestRebaseRewriteRulePagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().ClearExecResultBatches().AnyTimes()
	bh.EXPECT().ClearExecResultSet().AnyTimes()
	page := func(rows ...[]interface{}) *MysqlResultSet {
		r := &MysqlResultSet{}
		r.AddColumn(&MysqlColumn{})
		r.AddColumn(&MysqlColumn{})
		for _, row := range rows {
			r.AddRow(row)
		}
		return r
	}
	var firstRows [][]interface{}
	for i := 0; i < 128; i++ {
		firstRows = append(firstRows, []interface{}{int64(1), fmt.Sprintf("other.t%03d", i)})
	}
	firstRows[127][1] = `other.t\127`
	first := page(firstRows...)
	second := page([]interface{}{int64(2), " DB. t "})
	count := &MysqlResultSet{}
	count.AddColumn(&MysqlColumn{})
	count.AddRow([]interface{}{uint64(129)})
	gomock.InOrder(
		bh.EXPECT().Exec(gomock.Any(), "select count(*) from mo_catalog.mo_role_rule").Return(nil),
		bh.EXPECT().GetExecResultSet().Return([]interface{}{count}),
		bh.EXPECT().Exec(gomock.Any(), "select role_id, rule_name from mo_catalog.mo_role_rule order by role_id, rule_name limit 128").Return(nil),
		bh.EXPECT().GetExecResultSet().Return([]interface{}{first}),
		bh.EXPECT().Exec(gomock.Any(), "select role_id, rule_name from mo_catalog.mo_role_rule where role_id > 1 or (role_id = 1 and rule_name > cast(unhex('6f746865722e745c313237') as char)) order by role_id, rule_name limit 128").Return(nil),
		bh.EXPECT().GetExecResultSet().Return([]interface{}{second}),
	)
	sources := [4]rebaseBoundDatabase{{source: cloneDatabaseSource{srcResolveDBName: "db"}, defs: map[string]*plan.TableDef{"t": {}}}}
	err := rejectRebaseSourceRewriteRules(t.Context(), bh, sources, 1)
	require.ErrorContains(t, err, "Rebase does not support rewrite rules on source tables")
}

func TestRebaseRewriteRuleEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().ClearExecResultBatches().AnyTimes()
	bh.EXPECT().ClearExecResultSet().AnyTimes()
	count := &MysqlResultSet{}
	count.AddColumn(&MysqlColumn{})
	count.AddRow([]interface{}{uint64(0)})
	bh.EXPECT().Exec(gomock.Any(), "select count(*) from mo_catalog.mo_role_rule").Return(nil)
	bh.EXPECT().GetExecResultSet().Return([]interface{}{count})
	sources := [4]rebaseBoundDatabase{{source: cloneDatabaseSource{srcResolveDBName: "db"}, defs: map[string]*plan.TableDef{"t": {}}}}
	require.NoError(t, rejectRebaseSourceRewriteRules(t.Context(), bh, sources, 1))
}

func TestRebaseRewriteRuleMissingPage(t *testing.T) {
	ctrl := gomock.NewController(t)
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().ClearExecResultBatches().AnyTimes()
	bh.EXPECT().ClearExecResultSet().AnyTimes()
	count := &MysqlResultSet{}
	count.AddColumn(&MysqlColumn{})
	count.AddRow([]interface{}{uint64(1)})
	gomock.InOrder(
		bh.EXPECT().Exec(gomock.Any(), "select count(*) from mo_catalog.mo_role_rule").Return(nil),
		bh.EXPECT().GetExecResultSet().Return([]interface{}{count}),
		bh.EXPECT().Exec(gomock.Any(), "select role_id, rule_name from mo_catalog.mo_role_rule order by role_id, rule_name limit 128").Return(nil),
		bh.EXPECT().GetExecResultSet().Return([]interface{}{}),
	)
	sources := [4]rebaseBoundDatabase{{source: cloneDatabaseSource{srcResolveDBName: "db"}, defs: map[string]*plan.TableDef{"t": {}}}}
	err := rejectRebaseSourceRewriteRules(t.Context(), bh, sources, 1)
	require.ErrorContains(t, err, "invalid Rebase rewrite rule result")
}

func TestRebaseSQLStatementContract(t *testing.T) {
	for _, policy := range []string{"", " when conflict fail", " when conflict skip", " when conflict accept"} {
		for _, selection := range []string{"", " tables (`a`, `b`)"} {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
				"data branch rebase database dst from f{snapshot='fh'} against u{snapshot='uh'} between f{snapshot='bf'} and u{snapshot='bu'}"+selection+policy, 1)
			require.NoError(t, err)
			func() {
				defer stmt.Free()
				r, ok := stmt.(*tree.DataBranchRebase)
				require.True(t, ok)
				require.Equal(t, "dst", r.Destination.String())
				require.Equal(t, "f", r.Feature.Database.String())
				require.Equal(t, "u", r.Upstream.Database.String())
				if selection == "" {
					require.Empty(t, r.Tables)
				} else {
					require.Equal(t, tree.IdentifierList{"a", "b"}, r.Tables)
				}
				require.Equal(t, writePersistent, classifyStatementWrite(nil, stmt, ""))
				require.True(t, requiresPessimisticObjectLifecycleTxn(nil, stmt, ""))
				require.True(t, isDataBranchStatement(stmt))
				roundtrip, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tree.String(stmt, dialect.MYSQL), 1)
				require.NoError(t, err)
				roundtrip.Free()
			}()
		}
	}
	_, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
		"data branch rebase database dst from f{snapshot='fh'} against u{snapshot='uh'} between f{snapshot='bf'} and u{snapshot='bu'} tables ()", 1)
	require.Error(t, err)
}

func TestRebaseTableSelectionUsesDatabaseNameCaseRules(t *testing.T) {
	for _, tc := range []struct {
		lower int64
		want  string
	}{{0, "MiXeD"}, {1, "mixed"}, {2, "MiXeD"}} {
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL,
			"data branch rebase database dst from `MiXeD`{snapshot='fh'} against u{snapshot='uh'} between f{snapshot='bf'} and u{snapshot='bu'} tables (`MiXeD`)", tc.lower)
		require.NoError(t, err)
		func() {
			defer stmt.Free()
			r, ok := stmt.(*tree.DataBranchRebase)
			require.True(t, ok)
			require.Equal(t, tc.want, r.Feature.Database.String())
			require.Equal(t, tree.IdentifierList{tree.Identifier(tc.want)}, r.Tables)
		}()
	}
}

func TestRebaseTimestampContract(t *testing.T) {
	for _, snapshot := range []*plan.Snapshot{
		nil, {}, {TS: &timestamp.Timestamp{}},
		{TS: &timestamp.Timestamp{PhysicalTime: -1}},
		{TS: &timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}},
	} {
		require.Error(t, validateRebaseTimestamp(t.Context(), snapshot))
	}
	require.NoError(t, validateRebaseTimestamp(t.Context(), &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 100}}))
}

func TestRebaseRejectsEmptyBase(t *testing.T) {
	var sources [4]rebaseBoundDatabase
	for i := range sources {
		sources[i].source.snapshot = &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 100}}
	}
	tables, _, err := preflightRebaseTables(t.Context(), nil, sources, databranchutils.BranchReclaimDag{}, rebaseDatabaseRequest{})
	require.Nil(t, tables)
	require.ErrorContains(t, err, "Rebase requires at least one table to retain its base")
}

func TestRebaseBaseAncestry(t *testing.T) {
	dag := databranchutils.BranchReclaimDag{Info: map[uint64]databranchutils.BranchReclaimNode{
		2: {ParentTableID: 1, CloneTS: 10},
		3: {ParentTableID: 2, CloneTS: 20},
		4: {ParentTableID: 1, CloneTS: 10},
	}}
	require.True(t, rebaseBaseIsAncestor(dag, 1, 10, 3, 30))
	require.True(t, rebaseBaseIsAncestor(dag, 2, 15, 3, 30))
	require.False(t, rebaseBaseIsAncestor(dag, 1, 11, 3, 30))
	require.False(t, rebaseBaseIsAncestor(dag, 4, 10, 3, 30), "siblings are not ancestors")
	require.False(t, rebaseBaseIsAncestor(dag, 2, 21, 3, 30))
	require.False(t, rebaseBaseIsAncestor(dag, 0, 1, 3, 30))
	dag.Info[1] = databranchutils.BranchReclaimNode{ParentTableID: 3, CloneTS: 5}
	require.False(t, rebaseBaseIsAncestor(dag, 1, 10, 3, 30), "a matching node must not hide a cycle")
}

func TestRebaseBranchPinIdentity(t *testing.T) {
	dag := databranchutils.BranchReclaimDag{Info: map[uint64]databranchutils.BranchReclaimNode{
		2: {ParentTableID: 1, CloneTS: 10},
	}}
	valid := snapshotRecord{snapshotName: branchSnapshotName(2), kind: branchSnapshotKind, level: dataBranchLevel_Table, objId: 1, ts: 10}
	require.True(t, rebaseBranchPinMatches(dag, &valid, 1, 10))
	require.False(t, rebaseBranchPinMatches(dag, nil, 1, 10))
	for _, change := range []func(*snapshotRecord){
		func(p *snapshotRecord) { p.snapshotName = "__mo_branch_02" },
		func(p *snapshotRecord) { p.kind = "snapshot" },
		func(p *snapshotRecord) { p.level = dataBranchLevel_Database },
		func(p *snapshotRecord) { p.objId = 2 },
		func(p *snapshotRecord) { p.ts++ },
	} {
		pin := valid
		change(&pin)
		require.False(t, rebaseBranchPinMatches(dag, &pin, 1, 10))
	}
}
