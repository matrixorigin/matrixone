// Copyright 2024 Matrix Origin
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

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestLockReadQueryContextUsesCurrentSelectOwner(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	root.EnableLockRead()
	ctx := resource.ContextWithRoot(context.Background(), root)
	selectPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		StmtType: plan.Query_SELECT, Nodes: []*plan.Node{{NodeType: plan.Node_LOCK_OP}},
	}}}
	proc := testutil.NewProcess(t)
	selectCompile := NewCompile("test", "test", "", "", "", newStubEngine(), proc, &tree.Select{}, false, nil, time.Now())
	t.Cleanup(selectCompile.Release)
	selectCompile.pn = selectPlan
	selectCompile.SetResourceAttemptOwnerEligible()
	require.NoError(t, selectCompile.validateLockPlan(selectCompile.pn))
	query := selectCompile.lockReadQueryContext(ctx, selectCompile.resourceAttemptOwnerEligible)
	require.Same(t, root, resource.LockReadRootFromContext(query))

	for _, c := range []*Compile{
		{stmt: &tree.Update{}, pn: selectPlan, resourceAttemptOwnerEligible: true},
		{stmt: &tree.Delete{}, pn: selectPlan, resourceAttemptOwnerEligible: true},
		{stmt: (*tree.Select)(nil), pn: selectPlan, resourceAttemptOwnerEligible: true},
		{stmt: &tree.Select{}, pn: selectPlan, resourceAttemptOwnerEligible: true, isInternal: true},
		{stmt: &tree.Select{}, pn: selectPlan},
		{stmt: &tree.Select{}, resourceAttemptOwnerEligible: true},
		{stmt: &tree.Select{}, pn: &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{StmtType: plan.Query_SELECT}}}, resourceAttemptOwnerEligible: true},
		{stmt: &tree.Select{}, pn: &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
			StmtType: plan.Query_UPDATE, Nodes: selectPlan.GetQuery().Nodes,
		}}}, resourceAttemptOwnerEligible: true},
	} {
		require.NoError(t, c.validateLockPlan(c.pn))
		require.Nil(t, resource.LockReadRootFromContext(c.lockReadQueryContext(query, c.resourceAttemptOwnerEligible)))
	}
	require.Nil(t, resource.LockReadRootFromContext(selectCompile.lockReadQueryContext(context.Background(), true)))
	nextRoot := resource.NewRoot(resource.ConnExternal)
	nextRoot.EnableLockRead()
	next := resource.ContextWithRoot(query, nextRoot)
	require.Same(t, nextRoot, resource.LockReadRootFromContext(selectCompile.lockReadQueryContext(next, true)))

	proc.ReplaceTopCtx(next)
	require.NoError(t, selectCompile.Reset(proc, time.Now(), nil, "execute prepared_lock_read"))
	selectCompile.InitPipelineContextToExecuteQuery()
	require.Same(t, nextRoot, resource.LockReadRootFromContext(proc.Ctx))

	// Retry keeps the statement root and uses its own compiled plan generation.
	retry := &Compile{proc: proc, stmt: selectCompile.stmt, counterSet: new(perfcounter.CounterSet)}
	retry.initPipelineContextToRetryQuery(true)
	require.Nil(t, resource.LockReadRootFromContext(proc.Ctx))
	retryCtx, _ := process.GetQueryCtxFromProc(proc)
	require.Same(t, nextRoot, resource.RootFromContext(retryCtx))
	require.NoError(t, retry.validateLockPlan(selectPlan))
	retry.initPipelineContextToRetryQuery(true)
	require.Same(t, nextRoot, resource.LockReadRootFromContext(proc.Ctx))
	retry.initPipelineContextToRetryQuery(false)
	require.Nil(t, resource.LockReadRootFromContext(proc.Ctx))
	require.NoError(t, retry.validateLockPlan(nil))
	require.False(t, retry.lockingReadPlan)
}

func TestLockReadPrePipelineAcquisition(t *testing.T) {
	for _, tc := range []struct {
		name        string
		locking     bool
		tableID     uint64
		granularity lock.Granularity
	}{
		{"point-read metadata lock", true, catalog.MO_TABLES_ID, lock.Granularity_Row},
		{"shared table fallback", true, 42, lock.Granularity_Range},
		{"ordinary select metadata lock", false, catalog.MO_TABLES_ID, lock.Granularity_Row},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			root := resource.NewRoot(resource.ConnExternal)
			root.EnableLockRead()
			ctx := defines.AttachAccountId(resource.ContextWithRoot(t.Context(), root), catalog.System_Account)
			proc := testutil.NewProcess(t)
			proc.ReplaceTopCtx(ctx)
			txnClient, txnOp := newTestTxnClientAndOpWithModeIsolation(ctrl, txn.TxnMode_Pessimistic, txn.TxnIsolation_SI)
			proc.Base.TxnClient, proc.Base.TxnOperator = txnClient, txnOp
			op := txnOp.(*mock_frontend.MockTxnOperator)
			op.EXPECT().CreateTS().Return(timestamp.Timestamp{})
			op.EXPECT().AddWaitLock(tc.tableID, gomock.Any(), gomock.Any()).Return(uint64(1))
			op.EXPECT().RemoveWaitLock(uint64(1))
			svc := mock_lock.NewMockLockService(ctrl)
			svc.EXPECT().GetConfig().Return(lockservice.Config{MaxLockRowCount: 16}).AnyTimes()
			svc.EXPECT().GetServiceID().Return("").AnyTimes()
			svc.EXPECT().Lock(gomock.Any(), tc.tableID, gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ uint64, rows [][]byte, _ []byte, opts lock.LockOptions) (lock.Result, error) {
					require.NotEmpty(t, rows)
					require.Equal(t, tc.granularity, opts.Granularity)
					require.Equal(t, lock.LockMode_Shared, opts.Mode)
					return lock.Result{}, context.DeadlineExceeded
				})
			proc.Base.LockService = svc

			q := &plan.Query{StmtType: plan.Query_SELECT}
			if tc.locking {
				q.Nodes = []*plan.Node{{NodeType: plan.Node_LOCK_OP}}
			}
			c := &Compile{
				proc: proc, stmt: &tree.Select{},
				pn:                           &plan.Plan{Plan: &plan.Plan_Query{Query: q}},
				resourceAttemptOwnerEligible: true, lockMeta: NewLockMeta(), counterSet: new(perfcounter.CounterSet),
			}
			require.NoError(t, c.validateLockPlan(c.pn))
			defer c.lockMeta.clear(proc)
			if tc.granularity == lock.Granularity_Row {
				eng := mock_frontend.NewMockEngine(ctrl)
				database := mock_frontend.NewMockDatabase(ctrl)
				dbRel := mock_frontend.NewMockRelation(ctrl)
				tableRel := mock_frontend.NewMockRelation(ctrl)
				eng.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, txnOp).Return(database, nil)
				database.EXPECT().Relation(gomock.Any(), catalog.MO_DATABASE, gomock.Any()).Return(dbRel, nil)
				database.EXPECT().Relation(gomock.Any(), catalog.MO_TABLES, gomock.Any()).Return(tableRel, nil)
				dbRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(catalog.MO_DATABASE_ID))
				tableRel.EXPECT().GetTableID(gomock.Any()).Return(tc.tableID)
				tableRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{DbName: catalog.MO_CATALOG})
				tableRel.EXPECT().GetTableName().Return(catalog.MO_TABLES)
				c.e = eng
				c.lockMeta.appendMetaTables(&plan.ObjectRef{SchemaName: "db", ObjName: "point_read"})
			} else {
				c.lockTables = map[uint64]*plan.LockTarget{
					tc.tableID: {
						TableId: tc.tableID, Mode: lock.LockMode_Shared,
						PrimaryColTyp: plan.Type{Id: int32(types.T_int32)}, LockTable: true,
					},
				}
			}
			c.InitPipelineContextToExecuteQuery()
			_, err := c.prePipelineInitializer()
			require.ErrorIs(t, err, context.DeadlineExceeded)
			_, snapshot := root.SealWithLockRead(0)
			diagnostic := snapshot.Diagnostic([16]byte{}, 1, true)
			if tc.locking {
				require.True(t, snapshot.Qualifies(0, time.Second))
				require.Positive(t, diagnostic.AcquisitionNS)
				require.Equal(t, "acquisition", diagnostic.LastIncomplete)
			} else {
				require.False(t, snapshot.Qualifies(0, time.Second))
				require.Zero(t, diagnostic.Seen)
			}
		})
	}
}
