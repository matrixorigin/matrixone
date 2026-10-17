// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCheckConstraintAdmissionRevalidatesAfterAllClaims(t *testing.T) {
	for _, outcome := range []string{"available", "committed_sibling", "lock_error", "frontier_error", "snapshot_error", "catalog_error"} {
		t.Run(outcome, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 0)
			op := mock_frontend.NewMockTxnOperator(ctrl)
			proc.Base.TxnOperator = op
			op.EXPECT().Txn().Return(txn.TxnMeta{Mode: txn.TxnMode_Pessimistic, Isolation: txn.TxnIsolation_RC}).AnyTimes()
			op.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
			op.EXPECT().CreateTS().Return(timestamp.Timestamp{}).AnyTimes()
			op.EXPECT().AddWaitLock(gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(1)).AnyTimes()
			op.EXPECT().RemoveWaitLock(uint64(1)).AnyTimes()
			op.EXPECT().AddLockTable(gomock.Any()).Return(nil).AnyTimes()
			eng := newStubEngine()
			db := newStubDatabase("db")
			eng.dbs["db"] = db
			eng.dbs[catalog.MO_CATALOG].rels[catalog.MO_TABLES].tableID = catalog.MO_TABLES_ID
			injected := errors.New(outcome)
			if outcome == "catalog_error" {
				eng.dbErr = injected
			}
			var events []string
			proc.Base.LockService = lifecycleAdmissionLockService{lockFn: func(table uint64, opts lock.LockOptions) (lock.Result, error) {
				require.Equal(t, uint64(catalog.MO_TABLES_ID), table)
				require.Equal(t, lock.LockMode_Exclusive, opts.Mode)
				require.Equal(t, lock.WaitPolicy_Wait, opts.Policy)
				require.True(t, opts.KeepRows)
				events = append(events, "lock")
				if outcome == "lock_error" {
					return lock.Result{}, injected
				}
				return lock.Result{LockedOn: lock.LockTable{Table: table, Valid: true}}, nil
			}}
			frontier := timestamp.Timestamp{PhysicalTime: 20}
			workspace := mock_frontend.NewMockWorkspace(ctrl)
			workspace.EXPECT().AdvanceSnapshot(gomock.Any(), frontier).DoAndReturn(func(context.Context, timestamp.Timestamp) error {
				events = append(events, "snapshot")
				if outcome == "snapshot_error" {
					return injected
				}
				if outcome == "committed_sibling" {
					// The competing owner is visible only after applying the
					// post-grant frontier, not in the caller's original snapshot.
					db.rels["sibling"] = &stubRelation{tableDef: &plan.TableDef{Checks: []*plan.CheckDef{{Name: "a"}}}}
				}
				return nil
			}).AnyTimes()
			op.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
			barrier := &testCreateIndexLogtailBarrier{Engine: eng, acquire: func(context.Context) (timestamp.Timestamp, error) {
				events = append(events, "frontier")
				if outcome == "frontier_error" {
					return timestamp.Timestamp{}, injected
				}
				return frontier, nil
			}}
			c := &Compile{proc: proc, e: barrier}
			err := c.validateIntroducedCheckConstraintNames("db", "new_table", nil, []*plan.CheckDef{{Name: "Z"}, {Name: "A"}})
			switch outcome {
			case "available":
				require.NoError(t, err)
				require.Equal(t, []string{"lock", "lock", "frontier", "snapshot"}, events)
			case "committed_sibling":
				require.ErrorContains(t, err, "Duplicate check constraint name")
				require.Equal(t, []string{"lock", "lock", "frontier", "snapshot"}, events)
			default:
				require.ErrorIs(t, err, injected)
			}
		})
	}
}

func TestCheckConstraintReplayAndInheritedNamesDoNotClaim(t *testing.T) {
	proc := testutil.NewProcess(t)
	c := &Compile{proc: proc}
	checks := []*plan.CheckDef{{Name: "legacy"}}
	// No engine/operator is installed: either shortcut must avoid discovery,
	// locks and frontier changes entirely.
	require.NoError(t, c.validateIntroducedCheckConstraintNames("db", "t", checks, checks))
	proc.Ctx = context.WithValue(proc.Ctx, defines.TrustedCheckConstraintNames{}, true)
	require.NoError(t, c.validateIntroducedCheckConstraintNames("db", "t", nil, checks))
}

func TestCheckConstraintClaimRequiresRefreshableTransactionalOwner(t *testing.T) {
	for _, meta := range []txn.TxnMeta{
		{Mode: txn.TxnMode_Optimistic, Isolation: txn.TxnIsolation_RC},
		{Mode: txn.TxnMode_Optimistic, Isolation: txn.TxnIsolation_SI},
		{Mode: txn.TxnMode_Pessimistic, Isolation: txn.TxnIsolation_SI},
	} {
		t.Run(meta.Mode.String()+"/"+meta.Isolation.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			op.EXPECT().Txn().Return(meta).AnyTimes()
			proc.Base.TxnOperator = op
			c := &Compile{proc: proc}
			require.NoError(t, c.claimCheckConstraintNames("db", nil))
			require.ErrorContains(t, c.claimCheckConstraintNames("db", []*plan.CheckDef{{Name: "ck"}}), "require pessimistic RC")
			c.disableLock = true
			require.NoError(t, c.claimCheckConstraintNames("db", []*plan.CheckDef{{Name: "ck"}}))
		})
	}
}

func TestIntroducedCheckConstraints(t *testing.T) {
	old := []*plan.CheckDef{nil, {Name: "legacy"}, {Name: "T_chk_1"}}
	newCheck := &plan.CheckDef{Name: "ck_new"}
	next := []*plan.CheckDef{nil, {Name: "LEGACY"}, {Name: "t_chk_1"}, newCheck}
	require.Equal(t, []*plan.CheckDef{newCheck}, introducedCheckConstraints(old, next))
	require.Equal(t, next[1:], introducedCheckConstraints(nil, next))
	require.Empty(t, introducedCheckConstraints(old, nil))
}
