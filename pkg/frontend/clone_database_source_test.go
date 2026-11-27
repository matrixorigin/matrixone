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

package frontend

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
)

func TestCollectCloneDatabaseSourceReusesFKDefinitions(t *testing.T) {
	const database = "source_db"
	for _, ts := range []int64{0, 42} {
		t.Run(fmt.Sprint(ts), func(t *testing.T) {
			ses := newFeatureLimitTestSession(t)
			ctx := defines.AttachAccountId(t.Context(), 7)
			var snapshot *plan.Snapshot
			var spec string
			if ts != 0 {
				snapshot = &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: ts}}
				spec = fmt.Sprintf(" {MO_TS = %d}", ts)
			}
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[buildTableInfoListSQL(database, "", ts, 7)] = newMrsForRestoreStringRows([]string{"name", "type", "kind", "view"}, [][]interface{}{
				{"child", "BASE TABLE", catalog.SystemOrdinaryRel, ""},
				{"parent", "BASE TABLE", catalog.SystemOrdinaryRel, ""},
			})
			for table, ddl := range map[string]string{
				"parent": "create table source_db.parent (id int primary key)",
				"child":  "create table source_db.child (id int primary key, parent_id int, foreign key (parent_id) references source_db.parent(id))",
			} {
				bh.sql2result["show create table `source_db`.`"+table+"`"+spec] = newMrsForRestoreStringRows([]string{"table", "sql"}, [][]interface{}{{table, ddl}})
				bh.sql2result[buildTableInfoListSQL(database, table, ts, 7)] = newMrsForRestoreStringRows([]string{"name", "type", "kind", "view"}, [][]interface{}{{table, "BASE TABLE", catalog.SystemOrdinaryRel, ""}})
			}
			fkSQL := "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys" + spec + " where db_name = 'source_db'"
			bh.sql2result[fkSQL] = newMrsForRestoreStringRows([]string{"db", "table", "ref_db", "ref_table"}, [][]interface{}{
				{database, "child", database, "missing_parent"},
				{database, "child", "other_db", "parent"},
			})
			collect := func(t *testing.T) *cloneDatabaseSource {
				t.Helper()
				source := &cloneDatabaseSource{srcResolveDBName: database, snapshot: snapshot, viewMap: make(map[string]*tableInfo)}
				require.NoError(t, source.collectTables(ctx, ses.GetService(), bh))
				return source
			}
			source := collect(t)
			require.Len(t, bh.executedSQLs, 4)
			parent := slices.Index(source.sortedFkTbls, genKey(database, "parent"))
			child := slices.Index(source.sortedFkTbls, genKey(database, "child"))
			require.GreaterOrEqual(t, parent, 0)
			require.Less(t, parent, child)
			require.Len(t, source.fkTableMap, 2)
			for _, info := range source.srcTblInfos {
				require.Same(t, info, source.fkTableMap[genKey(database, info.tblName)])
			}
			for _, account := range bh.executionAccountIDs {
				require.Equal(t, uint32(7), account)
			}

			next := collect(t)
			require.Len(t, bh.executedSQLs, 8)
			require.NotSame(t, source.srcTblInfos[0], next.srcTblInfos[0])
			require.Same(t, next.srcTblInfos[0], next.fkTableMap[genKey(database, "child")])

			childSQL := "show create table `source_db`.`child`" + spec
			bh.sql2result[childSQL] = newMrsForRestoreStringRows([]string{"table", "sql"}, [][]interface{}{{"child", "create table source_db.child (id int primary key)"}})
			bh.sql2result[fkSQL] = newMrsForRestoreStringRows([]string{"db", "table", "ref_db", "ref_table"}, nil)
			next = collect(t)
			require.Len(t, bh.executedSQLs, 12)
			require.Len(t, next.srcTblInfos, 2)
			require.Empty(t, next.sortedFkTbls)
			require.Empty(t, next.fkTableMap)

			want := moerr.NewInternalErrorNoCtx("schema lookup failed")
			bh.sql2err[childSQL] = want
			require.ErrorIs(t, next.collectTables(ctx, ses.GetService(), bh), want)
		})
	}
}

func TestCloneDatabaseSourceBranchTableCount(t *testing.T) {
	source := cloneDatabaseSource{
		srcTblInfos: []*tableInfo{
			{tblName: "regular"},
			{tblName: "foreign_key"},
			{tblName: "view", typ: view},
		},
	}

	require.Equal(t, int64(2), source.branchTableCount())
}

func TestLockDataBranchCloneDatabaseSourcesSkipsSourcesWithoutTables(t *testing.T) {
	ctx := context.WithValue(context.Background(), dataBranchCloneLockCtxKey{}, true)
	for _, source := range []cloneDatabaseSource{
		{},
		{srcTblInfos: []*tableInfo{{tblName: "view", typ: view}}},
	} {
		require.NoError(t, lockDataBranchCloneDatabaseSources(ctx, nil, nil, source))
	}
}

func TestCloneFkTableOrder(t *testing.T) {
	t.Run("acyclic dependencies retain topological order", func(t *testing.T) {
		parent := genKey("db", "parent")
		child := genKey("db", "child")
		order, hasCycle := cloneFkTableOrder(map[string][]string{
			child: {parent},
		})

		require.False(t, hasCycle)
		require.Equal(t, []string{parent, child}, order)
	})

	t.Run("cyclic dependencies use deterministic forward-reference order", func(t *testing.T) {
		a := genKey("db", "a")
		b := genKey("db", "b")
		order, hasCycle := cloneFkTableOrder(map[string][]string{
			a: {b},
			b: {a},
		})

		require.True(t, hasCycle)
		require.Equal(t, []string{a, b}, order)
	})
}

func TestCloneSnapshotTxnOperator(t *testing.T) {
	ctrl := gomock.NewController(t)
	outerTxn := mock_frontend.NewMockTxnOperator(ctrl)
	branchTxn := mock_frontend.NewMockTxnOperator(ctrl)
	ses := newFeatureLimitTestSession(t)
	ses.proc.Base.TxnOperator = outerTxn

	t.Run("normal clone keeps frontend transaction", func(t *testing.T) {
		bh := ses.InitBackExec(branchTxn, "", fakeDataSetFetcher2)
		require.Same(t, outerTxn, cloneSnapshotTxnOperator(ses, bh))
	})

	t.Run("data branch uses owning background transaction", func(t *testing.T) {
		bh := ses.InitBackExec(branchTxn, "", fakeDataSetFetcher2, &BackgroundExecOption{
			forcePessimisticRC: true,
		})
		require.Same(t, branchTxn, cloneSnapshotTxnOperator(ses, bh))
	})
}

func TestDataBranchCloneLockProcessUsesOwningBackgroundTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	outerTxn := mock_frontend.NewMockTxnOperator(ctrl)
	branchTxn := mock_frontend.NewMockTxnOperator(ctrl)
	ses := newFeatureLimitTestSession(t)
	ses.proc.Base.TxnOperator = outerTxn
	bh := ses.InitBackExec(branchTxn, "", fakeDataSetFetcher2, &BackgroundExecOption{
		forcePessimisticRC: true,
	})

	lockProc := newDataBranchCloneLockProcess(context.Background(), ses, bh)
	defer lockProc.Free()
	require.Same(t, branchTxn, lockProc.GetTxnOperator())
	require.Same(t, outerTxn, ses.proc.GetTxnOperator())
}
