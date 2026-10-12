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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

type lifecycleAdmissionLockService struct {
	lockservice.LockService
	lockFn func(uint64, lock.LockOptions) (lock.Result, error)
}

func (s lifecycleAdmissionLockService) GetServiceID() string { return "" }
func (s lifecycleAdmissionLockService) GetConfig() lockservice.Config {
	return lockservice.Config{MaxLockRowCount: 1024}
}
func (s lifecycleAdmissionLockService) Lock(_ context.Context, table uint64, _ [][]byte, _ []byte, opts lock.LockOptions) (lock.Result, error) {
	return s.lockFn(table, opts)
}

func TestLifecycleAdmissionWaitPolicyFollowsRetainedOwnership(t *testing.T) {
	for _, tc := range []struct {
		name      string
		exclusive bool
		retained  bool
		policy    lock.WaitPolicy
	}{
		{"fresh_exclusive", true, false, lock.WaitPolicy_Wait},
		{"retained_exclusive", true, true, lock.WaitPolicy_FastFail},
		{"shared", false, false, lock.WaitPolicy_Wait},
		{"retained_shared", false, true, lock.WaitPolicy_FastFail},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, catalog.System_Account)
			proc.Base.TxnOperator = mock_frontend.NewMockTxnOperator(ctrl)
			op := proc.Base.TxnOperator
			// Use the real admission/lockop path, stopping at G before the
			// applied-frontier work. C must still be acquired first.
			mockOp := op.(*mock_frontend.MockTxnOperator)
			mockOp.EXPECT().Txn().Return(txn.TxnMeta{Mode: txn.TxnMode_Pessimistic, Isolation: txn.TxnIsolation_RC}).AnyTimes()
			mockOp.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
			mockOp.EXPECT().CreateTS().Return(timestamp.Timestamp{}).AnyTimes()
			mockOp.EXPECT().AddWaitLock(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(1)).AnyTimes()
			mockOp.EXPECT().RemoveWaitLock(uint64(1)).Times(2)
			mockOp.EXPECT().AddLockTable(gomock.Any()).Return(nil).Times(1)
			const registryID uint64 = 99
			var retained []lock.LockTable
			if tc.retained {
				retained = []lock.LockTable{{Table: 123, Valid: true}}
			}
			mockOp.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{LockTables: retained}, nil)
			eng := newStubEngine()
			eng.dbs[catalog.MO_CATALOG].rels[catalog.MO_TABLES].tableID = catalog.MO_TABLES_ID
			registry := newStubRelation(catalog.MO_FEATURE_REGISTRY)
			registry.tableID = registryID
			eng.dbs[catalog.MO_CATALOG].rels[catalog.MO_FEATURE_REGISTRY] = registry
			stop := errors.New("stop at lifecycle gate")
			var tables []uint64
			proc.Base.LockService = lifecycleAdmissionLockService{lockFn: func(table uint64, opts lock.LockOptions) (lock.Result, error) {
				tables = append(tables, table)
				require.True(t, opts.KeepRows)
				if table == registryID {
					require.Equal(t, tc.policy, opts.Policy)
					mode := lock.LockMode_Shared
					if tc.exclusive {
						mode = lock.LockMode_Exclusive
					}
					require.Equal(t, mode, opts.Mode)
					return lock.Result{}, stop
				}
				require.Equal(t, uint64(catalog.MO_TABLES_ID), table)
				require.Equal(t, lock.LockMode_Shared, opts.Mode)
				return lock.Result{LockedOn: lock.LockTable{Table: table, Valid: true, Version: 1}}, nil
			}}
			c := &Compile{proc: proc, e: eng}
			require.ErrorIs(t, c.admitLifecycleRC(nil, tc.exclusive), stop)
			require.Equal(t, []uint64{catalog.MO_TABLES_ID, registryID}, tables)
			require.Equal(t, lock.WaitPolicy_Wait, proc.GetWaitPolicy(), "override must remain request-local")
		})
	}
}

// Only unpublished logical identities may bypass the global replacement gate.
type creationViewWorkspace struct {
	client.Workspace
	created map[uint64]bool
}

func (w creationViewWorkspace) IsTableCreatedInTxn(id uint64) bool { return w.created[id] }

func TestTransactionLocalReplacementRequiresBothGenerations(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = op
	var workspace client.Workspace = creationViewWorkspace{created: map[uint64]bool{11: true, 12: true}}
	op.EXPECT().GetWorkspace().DoAndReturn(func() client.Workspace { return workspace }).AnyTimes()
	c := &Compile{proc: proc}
	require.True(t, c.transactionLocalTableIDs(12, 11), "repeated replacement of an unpublished table")
	require.False(t, c.transactionLocalTableIDs(12, 10), "local physical replacement of a published logical table")
	require.False(t, c.transactionLocalTableIDs(10, 11), "published active physical generation")
	workspace = nil
	require.False(t, c.transactionLocalTableIDs(12, 11), "unsupported workspace conservatively retains admission")
}

func TestBorrowedCreateNameRefreshBeforeEffects(t *testing.T) {
	for _, mode := range []string{"unknown_binding", "stable_binding", "changed_domain", "canceled", "ctas_refresh"} {
		t.Run(mode, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 0)
			op := mock_frontend.NewMockTxnOperator(ctrl)
			proc.Base.TxnOperator = op
			proc.SetIncrStatementDisabled(true)
			now := timestamp.Timestamp{PhysicalTime: 10}
			frontier := timestamp.Timestamp{PhysicalTime: 20}
			op.EXPECT().Txn().DoAndReturn(func() txn.TxnMeta {
				return txn.TxnMeta{Mode: txn.TxnMode_Pessimistic, Isolation: txn.TxnIsolation_RC, SnapshotTS: now}
			}).AnyTimes()
			op.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{LockTables: []lock.LockTable{{Table: 33, Valid: true}}}, nil).AnyTimes()
			op.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
			op.EXPECT().CreateTS().Return(timestamp.Timestamp{}).AnyTimes()
			op.EXPECT().AddWaitLock(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(1)).AnyTimes()
			op.EXPECT().RemoveWaitLock(uint64(1)).AnyTimes()
			op.EXPECT().AddLockTable(gomock.Any()).Return(nil).AnyTimes()
			binding := lock.LockTable{Table: catalog.MO_TABLES_ID, OriginTable: catalog.MO_TABLES_ID, Valid: true, Version: 1, ServiceID: "owner", AllocatorID: "allocator"}
			eng := newStubEngine()
			eng.dbs[catalog.MO_CATALOG].rels[catalog.MO_DATABASE].tableID = catalog.MO_DATABASE_ID
			eng.dbs[catalog.MO_CATALOG].rels[catalog.MO_TABLES].tableID = catalog.MO_TABLES_ID
			locks := 0
			proc.Base.LockService = lifecycleAdmissionLockService{lockFn: func(table uint64, opts lock.LockOptions) (lock.Result, error) {
				locks++
				require.Equal(t, lock.WaitPolicy_FastFail, opts.Policy, "retained caller locks cannot wait")
				grant := binding
				grant.Table = table
				grant.OriginTable = table
				return lock.Result{LockedOn: grant, Timestamp: timestamp.Timestamp{PhysicalTime: 1}}, nil
			}}
			advances := 0
			workspace := mock_frontend.NewMockWorkspace(ctrl)
			workspace.EXPECT().WriteOffset().Return(uint64(7)).AnyTimes()
			workspace.EXPECT().AdvanceSnapshot(gomock.Any(), frontier).DoAndReturn(func(context.Context, timestamp.Timestamp) error { advances++; now = frontier; return nil }).AnyTimes()
			op.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
			barrier := &testCreateIndexLogtailBarrier{Engine: eng, acquire: func(context.Context) (timestamp.Timestamp, error) { return frontier, nil }}
			q := &plan.CreateTable{Database: "db", TableDef: &plan.TableDef{Name: "t"}}
			if mode == "stable_binding" {
				q.FkNameBinding = &binding
			}
			original := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{DdlType: plan.DataDefinition_CREATE_TABLE, Definition: &plan.DataDefinition_CreateTable{CreateTable: q}}}}
			var source tree.Statement
			var planner *plan2.MockCompilerContext
			if mode == "ctas_refresh" {
				var err error
				source, err = parsers.ParseOne(t.Context(), dialect.MYSQL, "create table db.t as select a + 1 from select_test.bind_select", 1)
				require.NoError(t, err)
				defer source.Free()
				planner = plan2.NewMockCompilerContext(true, testutil.NewProcess(t))
				original, err = plan2.BuildPlan(planner, source, false)
				require.NoError(t, err)
			}
			builds := 0
			rebuilt := original
			c := NewCompile("test", "db", "create table db.t", "", "", barrier, proc, source, false, nil, time.Now())
			defer c.Release()
			c.SetBuildPlanFunc(func(ctx context.Context) (*plan.Plan, error) {
				builds++
				require.Equal(t, frontier, now, "rebuild follows workspace frontier")
				if mode == "ctas_refresh" {
					var err error
					rebuilt, err = plan2.BuildPlan(planner, source, false)
					return rebuilt, err
				}
				if mode == "changed_domain" {
					rebuilt = &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{DdlType: plan.DataDefinition_CREATE_TABLE, Definition: &plan.DataDefinition_CreateTable{CreateTable: &plan.CreateTable{Database: "db", TableDef: &plan.TableDef{Name: "different"}}}}}}
				}
				return rebuilt, nil
			})
			if mode == "canceled" {
				ctx, cancel := context.WithCancel(proc.Ctx)
				cancel()
				proc.Ctx = ctx
				proc.ReplaceTopCtx(ctx)
			}
			err := c.Compile(proc.Ctx, original, nil)
			got := c.GetPlan()
			switch mode {
			case "canceled":
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, builds)
				require.Zero(t, advances)
				require.Zero(t, locks)
			case "changed_domain":
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrLockConflict))
				require.Equal(t, 1, builds)
			case "stable_binding":
				require.NoError(t, err)
				require.Same(t, original, got)
				require.Zero(t, builds)
				require.Zero(t, advances)
			default:
				require.NoError(t, err)
				require.Same(t, rebuilt, got)
				require.Equal(t, 1, builds)
				require.Equal(t, 1, advances)
				require.True(t, c.createNamePinsAdmitted)
			}
			if mode == "ctas_refresh" {
				require.Equal(t, "a + 1", got.GetDdl().GetCreateTable().TableDef.Cols[0].Name)
				require.Equal(t, original.GetDdl().GetCreateTable().CreateAsSelectSql, got.GetDdl().GetCreateTable().CreateAsSelectSql)
			}
			// No workspace rollback, statement increment or token cancellation method
			// is expected on these mocks: planning is not an execution retry.
		})
	}
}
