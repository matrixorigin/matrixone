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

package compile

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestDropLifecycleForeignKeyDomain(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
	_, proc.Base.TxnOperator = newTestTxnClientAndOpWithPessimistic(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	c := &Compile{proc: proc, e: eng}
	root := mock_frontend.NewMockRelation(ctrl)
	def := &plan.TableDef{TblId: 1, Version: 0, Fkeys: []*plan.ForeignKeyDef{{ForeignTbl: 3}}, RefChildTbls: []uint64{2}}
	root.EXPECT().GetTableDef(proc.Ctx).DoAndReturn(func(context.Context) *plan.TableDef { return def }).AnyTimes()
	root.EXPECT().GetTableID(proc.Ctx).Return(uint64(1)).AnyTimes()
	root.EXPECT().GetDBID(proc.Ctx).Return(uint64(10)).AnyTimes()
	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relation(proc.Ctx, "t", nil).Return(root, nil).AnyTimes()
	db.EXPECT().GetDatabaseId(proc.Ctx).Return("10").AnyTimes()
	db.EXPECT().Relations(proc.Ctx).Return([]string{"t"}, nil).AnyTimes()
	eng.EXPECT().Database(proc.Ctx, "middle", proc.GetTxnOperator()).Return(db, nil).AnyTimes()
	parent := mock_frontend.NewMockRelation(ctrl)
	parent.EXPECT().GetTableID(proc.Ctx).Return(uint64(3)).AnyTimes()
	parent.EXPECT().GetDBID(proc.Ctx).Return(uint64(30)).AnyTimes()
	child := mock_frontend.NewMockRelation(ctrl)
	child.EXPECT().GetTableID(proc.Ctx).Return(uint64(2)).AnyTimes()
	child.EXPECT().GetDBID(proc.Ctx).Return(uint64(20)).AnyTimes()
	parentDB := "z_parent"
	var lookupErr error
	eng.EXPECT().GetRelationById(proc.Ctx, proc.GetTxnOperator(), uint64(3)).DoAndReturn(
		func(context.Context, client.TxnOperator, uint64) (string, string, engine.Relation, error) {
			return parentDB, "p", parent, lookupErr
		}).AnyTimes()
	eng.EXPECT().GetRelationById(proc.Ctx, proc.GetTxnOperator(), uint64(2)).Return("a_child", "c", child, nil).AnyTimes()
	q := &plan.DropTable{Database: "middle", Table: "t", TableId: 1, TableDef: def,
		ForeignTbl: []uint64{3, 0, 3}, FkChildTblsReferToMe: []uint64{2}}
	entries := []*plan.DropTable{q, q}
	names, before, err := c.loadDropLifecycleDomain(entries, "")
	require.NoError(t, err)
	require.Len(t, before, 3)
	wantNames := map[string]bool{"a_child": true, "middle": true, "z_parent": true}
	for _, name := range names {
		require.Equal(t, uint32(7), name.accountID)
		require.Equal(t, lock.LockMode_Shared, name.mode)
		delete(wantNames, name.name)
	}
	require.Empty(t, wantNames)

	// Database DROP uses the same domain and holds only its own D exclusively.
	// An added internal edge can leave the complete node set unchanged; receipt
	// comparison must still detect the new root FK set.
	dbNames, dbBefore, err := c.loadDropLifecycleDomain(nil, "middle")
	require.NoError(t, err)
	require.Equal(t, lifecycleDatabaseName{accountID: 7, name: "middle", mode: lock.LockMode_Exclusive}, dbNames[0])
	require.Equal(t, uint64(10), dbBefore[0].databaseID)
	def.RefChildTbls = []uint64{2, 3}
	_, dbAfter, err := c.loadDropLifecycleDomain(nil, "middle")
	require.NoError(t, err)
	require.Len(t, dbAfter, len(dbBefore))
	require.False(t, equalDropLifecycleDomain(dbBefore, dbAfter))
	def.RefChildTbls = []uint64{2}

	// Same physical parent but a new database/name route must invalidate the
	// previously admitted D set, even though the root's FK table IDs did not move.
	parentDB = "new_parent"
	_, after, err := c.loadDropLifecycleDomain(entries, "")
	require.NoError(t, err)
	require.False(t, equalDropLifecycleDomain(before, after))
	parentDB = "z_parent"
	// Own uncommitted definitions can remain Version0. A new FK still changes
	// the consumed set and must cause a whole-plan retry before target locks.
	def.RefChildTbls = append(def.RefChildTbls, 4)
	_, _, err = c.loadDropLifecycleDomain(entries, "")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), "%v", err)
	def.RefChildTbls = []uint64{2}
	q.TableId = 99
	_, _, err = c.loadDropLifecycleDomain(entries, "")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), "%v", err)
	q.TableId = 1

	lookupErr = moerr.NewNoSuchTable(proc.Ctx, parentDB, "p")
	_, after, err = c.loadDropLifecycleDomain(entries, "")
	require.NoError(t, err, "foreign_key_checks cleanup permits a missing old parent")
	require.NotContains(t, after, uint64(3))
	lookupErr = errors.New("catalog unavailable")
	_, _, err = c.loadDropLifecycleDomain(entries, "")
	require.ErrorIs(t, err, lookupErr)

	// A temporary alias must not acquire or inspect the permanent table's FK
	// domain. This also covers an absent DROP TEMPORARY IF EXISTS plan.
	proc.Session = &trackingTempTableSession{tables: map[string]string{"middle.t": "temp_t"}}
	names, after, err = c.loadDropLifecycleDomain(entries, "")
	require.NoError(t, err)
	require.Empty(t, names)
	require.Empty(t, after)
	proc.Session = nil
	c.stmt = &tree.DropTable{Temporary: true}
	names, _, err = c.loadDropLifecycleDomain(entries, "")
	require.NoError(t, err)
	require.Empty(t, names)
	ctx, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = ctx
	_, _, err = c.loadDropLifecycleDomain(entries, "")
	require.ErrorIs(t, err, context.Canceled)
}

func TestDropLifecycleBranchRootsExcludeForeignKeyDomain(t *testing.T) {
	domain := map[uint64]dropLifecycleIdentity{
		1: {database: "target"},
		2: {database: "target"},
		3: {database: "foreign"},
	}
	tables := []*plan.DropTable{
		{Database: "target", TableId: 2},
		{Database: "target", TableId: 1},
		{Database: "foreign", TableId: 3, IsView: true},
	}
	require.Equal(t, []uint64{1, 2}, dropLifecycleBranchRoots(domain, tables, ""))
	require.Equal(t, []uint64{1, 2}, dropLifecycleBranchRoots(domain, nil, "target"))
}

func TestRemoveCompactedBranchRowsKeepsSurvivingChildLinks(t *testing.T) {
	dag := databranchutils.BranchReclaimDag{
		Info: map[uint64]databranchutils.BranchReclaimNode{
			10: {ParentTableID: 1, Deleted: true},
			11: {ParentTableID: 10},
			12: {ParentTableID: 1, Deleted: true},
			13: {ParentTableID: 1},
		},
		Children: map[uint64][]uint64{1: {10, 12, 13}, 10: {11}},
	}
	removeCompactedBranchRows(&dag, []uint64{10, 12})
	require.Equal(t, map[uint64]databranchutils.BranchReclaimNode{
		11: {ParentTableID: 10}, 13: {ParentTableID: 1},
	}, dag.Info)
	require.Equal(t, map[uint64][]uint64{1: {13}, 10: {11}}, dag.Children)
}

func TestBroadDropLifecycleReceiptIsSynchronous(t *testing.T) {
	ctrl := gomock.NewController(t)
	stop := errors.New("target lookup reached without nested admission")
	var nested context.Context
	exec := &dropDDLExecutor{exec: func(ctx context.Context, _ string, opts executor.Options) (executor.Result, error) {
		nested = ctx
		require.True(t, opts.DisableIncrStatement())
		return executor.Result{}, stop
	}}
	c, eng := newDropDDLCompile(t, ctrl, exec)
	original := c.proc.Ctx
	eng.EXPECT().Database(gomock.Any(), "db", c.proc.GetTxnOperator()).Return(nil, stop).Times(1)
	q := &plan.DropTable{Database: "db", Table: "t", TableId: 1, TableDef: &plan.TableDef{TblId: 1}}
	err := c.withBroadDropLifecycle(func() error {
		admitted, err := c.borrowedDropLifecycle()
		require.NoError(t, err)
		require.True(t, admitted)
		// Real handler reaches only the target, not mo_catalog/C/G/D discovery.
		require.ErrorIs(t, dropTableScope(q).DropTable(c), stop)
		return c.runSqlWithAccountId("drop table db.t", 7)
	})
	require.ErrorIs(t, err, stop)
	require.Same(t, original, c.proc.Ctx)
	c.proc.Ctx = nested
	_, err = c.borrowedDropLifecycle()
	require.Error(t, err, "a retained child context expires at the outer callback boundary")
	c.proc.Ctx = original
	require.PanicsWithValue(t, "injected", func() {
		_ = c.withBroadDropLifecycle(func() error { panic("injected") })
	})
	require.Same(t, original, c.proc.Ctx)
	_, other := newTestTxnClientAndOpWithPessimistic(ctrl)
	err = c.withBroadDropLifecycle(func() error {
		owner := c.proc.Base.TxnOperator
		c.proc.Base.TxnOperator = other
		defer func() { c.proc.Base.TxnOperator = owner }()
		_, err := c.borrowedDropLifecycle()
		return err
	})
	require.Error(t, err)
}

func TestDropLifecycleKeepsLegacySIAdmission(t *testing.T) {
	ctrl := gomock.NewController(t)
	stop := errors.New("legacy SI lifecycle barrier")
	calls := 0
	exec := &dropDDLExecutor{exec: func(_ context.Context, sql string, _ executor.Options) (executor.Result, error) {
		calls++
		require.Equal(t, databranchutils.LineageOwnerLifecycleLockSQL(), sql)
		return executor.Result{}, stop
	}}
	c, _ := newDropDDLCompile(t, ctrl, exec)
	require.False(t, c.isLifecycleRC())
	require.ErrorIs(t, dropTableScope(&plan.DropTable{Database: "db", Table: "t", TableDef: &plan.TableDef{}}).DropTable(c), stop)
	s := &Scope{Plan: &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
		Definition: &plan.DataDefinition_DropDatabase{DropDatabase: &plan.DropDatabase{Database: "db"}},
	}}}}
	require.ErrorIs(t, s.DropDatabase(c), stop)
	require.Equal(t, 2, calls)
}
