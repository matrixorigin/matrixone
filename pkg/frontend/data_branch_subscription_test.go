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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

// stubBranchSubscriptionMeta replaces the classifier's production resolver
// for one test and returns the recorded SubscriptionMeta (or error).
func stubBranchSubscriptionMeta(t *testing.T, meta *plan.SubscriptionMeta, err error) {
	t.Helper()
	stub := gostub.Stub(&resolveBranchSubscriptionMeta,
		func(context.Context, string, FeSession, TxnOperator, BackgroundExec) (*plan.SubscriptionMeta, error) {
			return meta, err
		})
	t.Cleanup(stub.Reset)
}

// TestClassifySubscriptionOperandCoversVisibilityModel is design §9 U1: the
// classification boundary of design §4.1.
func TestClassifySubscriptionOperandCoversVisibilityModel(t *testing.T) {
	ctx := context.Background()

	t.Run("ordinary database stays local", func(t *testing.T) {
		stubBranchSubscriptionMeta(t, nil, nil)
		bh := &backgroundExecTest{}
		bh.init()
		operand, outOfScope, err := classifySubscriptionOperand(ctx, nil, nil, bh, "localdb", "t1")
		require.NoError(t, err)
		require.False(t, outOfScope)
		require.True(t, operand.local)
	})

	t.Run("live subscription with in-scope table routes with P_pub P_src D", func(t *testing.T) {
		meta := &plan.SubscriptionMeta{
			AccountName: "pub_account",
			Name:        "pub1",
			AccountId:   7,
			DbName:      "tdb",
			Tables:      "*",
		}
		stubBranchSubscriptionMeta(t, meta, nil)
		bh := &backgroundExecTest{}
		bh.init()
		operand, outOfScope, err := classifySubscriptionOperand(ctx, nil, nil, bh, "sub3", "t1")
		require.NoError(t, err)
		require.False(t, outOfScope)
		require.False(t, operand.local)
		require.Equal(t, "pub_account", operand.pubAccount)
		require.Equal(t, "pub1", operand.pubName)
		require.Equal(t, uint32(7), operand.physAccount)
		require.Equal(t, "tdb", operand.physDb)
		require.Same(t, meta, operand.meta)
	})

	t.Run("persisted foreign-source meta keeps P_src distinct from P_pub", func(t *testing.T) {
		// Publication owned by sys over account 2's database: the publication
		// identity and the physical source owner differ (design §3, C10).
		meta := &plan.SubscriptionMeta{
			AccountName: "sys",
			Name:        "pub_fs",
			AccountId:   2,
			DbName:      "fsrc_db",
			Tables:      "base,v_x",
		}
		stubBranchSubscriptionMeta(t, meta, nil)
		bh := &backgroundExecTest{}
		bh.init()
		operand, outOfScope, err := classifySubscriptionOperand(ctx, nil, nil, bh, "s_fsrc", "base")
		require.NoError(t, err)
		require.False(t, outOfScope)
		require.Equal(t, "sys", operand.pubAccount)
		require.Equal(t, uint32(2), operand.physAccount)
		require.Equal(t, "fsrc_db", operand.physDb)
	})

	t.Run("table-limited publication admits only listed tables", func(t *testing.T) {
		meta := &plan.SubscriptionMeta{
			AccountName: "pub_account",
			Name:        "pub1",
			AccountId:   7,
			DbName:      "tdb",
			Tables:      "t1,t2",
		}
		stubBranchSubscriptionMeta(t, meta, nil)
		bh := &backgroundExecTest{}
		bh.init()
		operand, outOfScope, err := classifySubscriptionOperand(ctx, nil, nil, bh, "sub3", "t2")
		require.NoError(t, err)
		require.False(t, outOfScope)
		require.False(t, operand.local)
	})

	t.Run("out-of-scope table reports the visibility cause", func(t *testing.T) {
		meta := &plan.SubscriptionMeta{
			AccountName: "pub_account",
			Name:        "pub1",
			AccountId:   7,
			DbName:      "tdb",
			Tables:      "t1",
		}
		stubBranchSubscriptionMeta(t, meta, nil)
		bh := &backgroundExecTest{}
		bh.init()
		operand, outOfScope, err := classifySubscriptionOperand(ctx, nil, nil, bh, "sub3", "t2")
		require.NoError(t, err)
		require.True(t, outOfScope)
		require.Nil(t, operand)

		visErr := newBranchOperandOutsidePublicationScopeError(ctx, "sub3", "t2")
		require.ErrorContains(t, visErr, "outside the publication's scope")
		require.NotContains(t, visErr.Error(), "does not exist",
			"the visibility cause must stay distinguishable from the engine's no-such-table error")
	})

	t.Run("liveness failures propagate unchanged", func(t *testing.T) {
		injected := errors.New("subscription sub3 is invalid: publication pub_account.pub1 is not valid")
		stubBranchSubscriptionMeta(t, nil, injected)
		bh := &backgroundExecTest{}
		bh.init()
		_, _, err := classifySubscriptionOperand(ctx, nil, nil, bh, "sub3", "t1")
		require.ErrorIs(t, err, injected)
	})

	t.Run("database-only classification skips the table scope check", func(t *testing.T) {
		meta := &plan.SubscriptionMeta{
			AccountName: "pub_account",
			Name:        "pub1",
			AccountId:   7,
			DbName:      "tdb",
			Tables:      "t1",
		}
		stubBranchSubscriptionMeta(t, meta, nil)
		bh := &backgroundExecTest{}
		bh.init()
		operand, outOfScope, err := classifySubscriptionOperand(ctx, nil, nil, bh, "sub3", "")
		require.NoError(t, err)
		require.False(t, outOfScope)
		require.False(t, operand.local)
	})
}

// TestRejectSubscriptionBranchWriteDestinationCoversC5 is the destination
// half of design §4.1 consumer 3 / §4.3 (I3): any non-local MERGE/PICK
// destination fails before a write.
func TestRejectSubscriptionBranchWriteDestinationCoversC5(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newSes(nil, ctrl)
	defer ses.Close()

	t.Run("subscription destination is rejected", func(t *testing.T) {
		stubBranchSubscriptionMeta(t, &plan.SubscriptionMeta{
			AccountName: "pub_account",
			Name:        "pub1",
			AccountId:   7,
			DbName:      "tdb",
			Tables:      "*",
		}, nil)
		err := rejectSubscriptionBranchWriteDestination(
			ctx, ses, &backgroundExecTest{}, "MERGE", makeBranchTableName("sub3", "t1", nil))
		require.ErrorContains(t, err, "cannot perform DATA BRANCH MERGE into subscription database sub3.t1")
	})

	t.Run("out-of-scope subscription destination is rejected identically", func(t *testing.T) {
		stubBranchSubscriptionMeta(t, &plan.SubscriptionMeta{
			AccountName: "pub_account",
			Name:        "pub1",
			AccountId:   7,
			DbName:      "tdb",
			Tables:      "t1",
		}, nil)
		err := rejectSubscriptionBranchWriteDestination(
			ctx, ses, &backgroundExecTest{}, "PICK", makeBranchTableName("sub3", "t2", nil))
		require.ErrorContains(t, err, "cannot perform DATA BRANCH PICK into subscription database sub3.t2")
	})

	t.Run("local destination stays admitted", func(t *testing.T) {
		stubBranchSubscriptionMeta(t, nil, nil)
		bh := &backgroundExecTest{}
		bh.init()
		require.NoError(t, rejectSubscriptionBranchWriteDestination(
			ctx, ses, bh, "MERGE", makeBranchTableName("localdb", "t1", nil)))
	})

	t.Run("classification errors propagate", func(t *testing.T) {
		injected := errors.New("subscription dropped")
		stubBranchSubscriptionMeta(t, nil, injected)
		err := rejectSubscriptionBranchWriteDestination(
			ctx, ses, &backgroundExecTest{}, "MERGE", makeBranchTableName("sub3", "t1", nil))
		require.ErrorIs(t, err, injected)
	})
}

// TestBranchPrivilegeClassificationReportsVisibilityCause is design §9 U2:
// design §4.1.1 — the classification step runs before the synthetic SELECT
// probe, so an out-of-scope or dead subscription operand keeps its real
// cause instead of surfacing the probe's masking "table does not exist".
func TestBranchPrivilegeClassificationReportsVisibilityCause(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newSes(nil, ctrl)
	defer ses.Close()

	bh := &backgroundExecTest{}
	bh.init()
	stub := gostub.Stub(&branchPrivilegeBackExecutor,
		func(context.Context, *Session, ...*BackgroundExecOption) (BackgroundExec, func(error) error, error) {
			return bh, nil, nil
		})
	t.Cleanup(stub.Reset)

	t.Run("out-of-scope operand surfaces the scope error before any plan build", func(t *testing.T) {
		stubBranchSubscriptionMeta(t, &plan.SubscriptionMeta{
			AccountName: "pub_account",
			Name:        "pub1",
			AccountId:   7,
			DbName:      "tdb",
			Tables:      "t1",
		}, nil)
		_, err := requireBranchReadOnTableName(ctx, ses, makeBranchTableName("sub3", "t2", nil))
		require.ErrorContains(t, err, "outside the publication's scope")
		require.NotContains(t, err.Error(), "does not exist",
			"the synthetic SELECT's masking error must not hide the visibility cause")
		require.Empty(t, bh.executedSQLs,
			"the classification verdict must precede the synthetic SELECT probe")
	})

	t.Run("revoked subscription propagates the liveness error before any plan build", func(t *testing.T) {
		injected := errors.New("subscription sub3 is invalid")
		stubBranchSubscriptionMeta(t, nil, injected)
		_, err := requireBranchReadOnTableName(ctx, ses, makeBranchTableName("sub3", "t1", nil))
		require.ErrorIs(t, err, injected)
		require.Empty(t, bh.executedSQLs)
	})
}

// TestBranchIdentityPlanValues is design §9 U3 for the pure plan-side
// pieces: operand identity unpacking, context derivation, candidate
// ordering, and CP-key separation per physical owner.
func TestBranchIdentityPlanValues(t *testing.T) {
	base := defines.AttachAccountId(context.Background(), uint32(1))

	t.Run("identity context keeps the statement context for the same account", func(t *testing.T) {
		ident := localOperandIdentity(base, 1)
		require.Same(t, base, identityCtx(base, ident))
	})

	t.Run("identity context attaches the physical owner for routed operands", func(t *testing.T) {
		ownerCtx := defines.AttachAccountId(context.Background(), uint32(7))
		ident := branchIdentity{account: 7, ctx: ownerCtx}
		derived := identityCtx(base, ident)
		account, err := defines.GetAccountId(derived)
		require.NoError(t, err)
		require.Equal(t, uint32(7), account)
	})

	t.Run("candidates collapse equal accounts and keep the walking order", func(t *testing.T) {
		ident7 := branchIdentity{account: 7, ctx: base}
		ident1 := branchIdentity{account: 1, ctx: base}
		require.Len(t, dedupeBranchIdentities(ident7, ident7), 1,
			"a local-local pair performs one lookup per node, byte-identical to the pre-change behavior")
		ordered := dedupeBranchIdentities(ident7, ident1)
		require.Equal(t, []branchIdentity{ident7, ident1}, ordered,
			"the walking side's endpoint comes first, then the opposite endpoint's")
	})

	t.Run("the lower-bound cache distinguishes two owners of one database name", func(t *testing.T) {
		cache := map[dbCreatedTimeLowerBoundKey]types.TS{}
		ts7 := types.BuildTS(17, 0)
		cache[dbCreatedTimeLowerBoundKey{accountID: 7, databaseID: 42, database: "tdb", snapshot: ts7}] = ts7
		_, ok := cache[dbCreatedTimeLowerBoundKey{accountID: 1, databaseID: 42, database: "tdb", snapshot: ts7}]
		require.False(t, ok, "the same database id under another owner must not alias the cached bound")
		require.NotEqual(t, packMoDatabaseCPKey(7, "tdb"), packMoDatabaseCPKey(1, "tdb"),
			"the mo_database CP key embeds the physical owner")
	})
}

// TestReopenDataBranchNodeByIDCandidates is design §9 U3 for the reopen
// order of design §6.4: injected lookup failures prove the winner's tenant
// pin and the preserved all-missing degradation.
func TestReopenDataBranchNodeByIDCandidates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	snapshotTS := types.BuildTS(100, 0)
	missing := moerr.NewNoSuchTableNoCtx("d", "t")

	newLookupStub := func(t *testing.T, hits map[uint32]engine.Relation, calls *[]uint32) {
		t.Helper()
		stub := gostub.Stub(&reopenDataBranchRelationByID,
			func(ctx context.Context, ses *Session, bh BackgroundExec, tableID uint64, snapshot *plan.Snapshot) (engine.Relation, error) {
				require.NotNil(t, snapshot.Tenant, "every candidate probe is tenant-pinned")
				*calls = append(*calls, snapshot.Tenant.TenantID)
				if rel, ok := hits[snapshot.Tenant.TenantID]; ok {
					return rel, nil
				}
				return nil, missing
			})
		t.Cleanup(stub.Reset)
	}

	t.Run("the first resolving candidate wins with its tenant pin", func(t *testing.T) {
		rel := mock_frontend.NewMockRelation(ctrl)
		var calls []uint32
		newLookupStub(t, map[uint32]engine.Relation{7: rel}, &calls)
		ident7 := branchIdentity{account: 7, ctx: ctx}
		ident1 := branchIdentity{account: 1, ctx: ctx}
		got, winner, err := reopenDataBranchNodeByID(ctx, nil, nil,
			[]branchIdentity{ident7, ident1}, 42, snapshotTS)
		require.NoError(t, err)
		require.Same(t, rel, got)
		require.Equal(t, ident7, winner)
		require.Equal(t, []uint32{7}, calls, "the winner short-circuits the remaining candidates")
	})

	t.Run("a miss under the first candidate falls through to the second", func(t *testing.T) {
		rel := mock_frontend.NewMockRelation(ctrl)
		var calls []uint32
		newLookupStub(t, map[uint32]engine.Relation{7: rel}, &calls)
		ident1 := branchIdentity{account: 1, ctx: ctx}
		ident7 := branchIdentity{account: 7, ctx: ctx}
		got, winner, err := reopenDataBranchNodeByID(ctx, nil, nil,
			[]branchIdentity{ident1, ident7}, 42, snapshotTS)
		require.NoError(t, err)
		require.Same(t, rel, got)
		require.Equal(t, ident7, winner)
		require.Equal(t, []uint32{1, 7}, calls)
	})

	t.Run("missing under every candidate returns the last error", func(t *testing.T) {
		var calls []uint32
		newLookupStub(t, map[uint32]engine.Relation{}, &calls)
		ident1 := branchIdentity{account: 1, ctx: ctx}
		ident7 := branchIdentity{account: 7, ctx: ctx}
		_, _, err := reopenDataBranchNodeByID(ctx, nil, nil,
			[]branchIdentity{ident1, ident7}, 42, snapshotTS)
		require.ErrorIs(t, err, missing,
			"the caller applies the preserved same-transaction / zero-history degradation")
		require.Equal(t, []uint32{1, 7}, calls)
	})

	t.Run("a non-missing error under the first candidate stops the walk unchanged", func(t *testing.T) {
		storage := moerr.NewInternalErrorNoCtx("storage read failed")
		var calls []uint32
		stub := gostub.Stub(&reopenDataBranchRelationByID,
			func(ctx context.Context, ses *Session, bh BackgroundExec, tableID uint64, snapshot *plan.Snapshot) (engine.Relation, error) {
				calls = append(calls, snapshot.Tenant.TenantID)
				if snapshot.Tenant.TenantID == 7 {
					return nil, storage
				}
				return nil, missing
			})
		t.Cleanup(stub.Reset)
		ident7 := branchIdentity{account: 7, ctx: ctx}
		ident1 := branchIdentity{account: 1, ctx: ctx}
		_, _, err := reopenDataBranchNodeByID(ctx, nil, nil,
			[]branchIdentity{ident7, ident1}, 42, snapshotTS)
		require.ErrorIs(t, err, storage,
			"a storage, transaction, or cancellation failure must propagate unchanged")
		require.Equal(t, []uint32{7}, calls,
			"the wrong-owner candidate is never probed after a real failure")
	})

	t.Run("a wrong-owner absence never overwrites the true owner's non-missing error", func(t *testing.T) {
		var calls []uint32
		storage := moerr.NewInternalErrorNoCtx("storage read failed under the true owner")
		stub := gostub.Stub(&reopenDataBranchRelationByID,
			func(ctx context.Context, ses *Session, bh BackgroundExec, tableID uint64, snapshot *plan.Snapshot) (engine.Relation, error) {
				calls = append(calls, snapshot.Tenant.TenantID)
				if snapshot.Tenant.TenantID == 1 {
					return nil, missing
				}
				return nil, storage
			})
		t.Cleanup(stub.Reset)
		ident1 := branchIdentity{account: 1, ctx: ctx}
		ident7 := branchIdentity{account: 7, ctx: ctx}
		_, _, err := reopenDataBranchNodeByID(ctx, nil, nil,
			[]branchIdentity{ident1, ident7}, 42, snapshotTS)
		require.ErrorIs(t, err, storage,
			"the later candidate's absence must not mask the true owner's failure as a missing relation")
		require.Equal(t, []uint32{1, 7}, calls)
	})
}

// TestResolveSameTransactionDataBranchRelationFailureIntegrity pins the
// zero-history fold of resolveSameTransactionDataBranchRelation to verified
// absence: only when BOTH probes report a missing relation and the node
// predates the endpoint's creation commit does the caller degrade to the
// child schema; a masked storage failure must never proceed without
// available ancestor history.
func TestResolveSameTransactionDataBranchRelationFailureIntegrity(t *testing.T) {
	ctx := context.Background()
	missing := moerr.NewNoSuchTableNoCtx("d", "t")
	storage := moerr.NewInternalErrorNoCtx("storage read failed")
	nodeSnapshot := types.BuildTS(100, 0)
	endpointCTS := types.BuildTS(1000, 0)

	stubLookups := func(t *testing.T, endpointErrs map[uint32]error) {
		t.Helper()
		stub := gostub.Stub(&reopenDataBranchRelationByID,
			func(ctx context.Context, ses *Session, bh BackgroundExec, tableID uint64, snapshot *plan.Snapshot) (engine.Relation, error) {
				require.Equal(t, endpointCTS.ToTimestamp().PhysicalTime, snapshot.TS.PhysicalTime,
					"the second probe runs at the endpoint creation commit timestamp")
				if err, ok := endpointErrs[snapshot.Tenant.TenantID]; ok {
					return nil, err
				}
				return nil, missing
			})
		t.Cleanup(stub.Reset)
	}

	t.Run("both probes missing and node predating endpoint degrades to zero history", func(t *testing.T) {
		stubLookups(t, map[uint32]error{})
		_, _, _, zeroHistory, err := resolveSameTransactionDataBranchRelation(
			ctx, nil, nil,
			[]branchIdentity{{account: 7, ctx: ctx}, {account: 1, ctx: ctx}},
			42, nodeSnapshot, endpointCTS, missing)
		require.NoError(t, err)
		require.True(t, zeroHistory,
			"a verified absence at both timestamps keeps the child-parent schema fold")
	})

	t.Run("a non-missing historical error never folds into zero history", func(t *testing.T) {
		stubLookups(t, map[uint32]error{})
		_, _, _, zeroHistory, err := resolveSameTransactionDataBranchRelation(
			ctx, nil, nil,
			[]branchIdentity{{account: 7, ctx: ctx}, {account: 1, ctx: ctx}},
			42, nodeSnapshot, endpointCTS, storage)
		require.ErrorIs(t, err, storage,
			"DIFF/MERGE aborts on the true owner's storage failure instead of proceeding without history")
		require.False(t, zeroHistory)
	})

	t.Run("a non-missing endpoint error also blocks the fold", func(t *testing.T) {
		stubLookups(t, map[uint32]error{7: storage})
		_, _, _, zeroHistory, err := resolveSameTransactionDataBranchRelation(
			ctx, nil, nil,
			[]branchIdentity{{account: 7, ctx: ctx}, {account: 1, ctx: ctx}},
			42, nodeSnapshot, endpointCTS, missing)
		require.ErrorIs(t, err, missing,
			"the historical error is preserved so the walk aborts instead of degrading")
		require.False(t, zeroHistory)
	})
}

// TestBuildBranchOperandThreadsTheOperandTransaction pins review finding 1
// at the execution boundary: buildBranchOperand classifies under the exact
// operand operator the caller resolved (the ts-pinned view for a hinted
// operand), never re-deriving the live statement transaction.
func TestBuildBranchOperandThreadsTheOperandTransaction(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newSes(nil, ctrl)
	defer ses.Close()

	var gotTxn TxnOperator
	recordStub := gostub.Stub(&resolveBranchSubscriptionMeta,
		func(_ context.Context, _ string, _ FeSession, op TxnOperator, _ BackgroundExec) (*plan.SubscriptionMeta, error) {
			gotTxn = op
			return nil, nil
		})
	t.Cleanup(recordStub.Reset)

	t.Run("the pinned operand operator reaches the classifier on the local path", func(t *testing.T) {
		pinned := mock_frontend.NewMockTxnOperator(ctrl)
		bh := &backgroundExecTest{}
		bh.init()
		operand, err := buildBranchOperand(ctx, ses, bh, pinned, "localdb", "t1")
		require.NoError(t, err)
		require.False(t, operand.isSubscription)
		require.True(t, gotTxn == pinned,
			"the classifier must observe the caller-supplied operand transaction")
	})

	t.Run("a second operand's operator is not aliased with the first", func(t *testing.T) {
		first := mock_frontend.NewMockTxnOperator(ctrl)
		second := mock_frontend.NewMockTxnOperator(ctrl)
		bh := &backgroundExecTest{}
		bh.init()
		_, err := buildBranchOperand(ctx, ses, bh, first, "localdb", "t1")
		require.NoError(t, err)
		require.True(t, gotTxn == first)
		_, err = buildBranchOperand(ctx, ses, bh, second, "localdb", "t1")
		require.NoError(t, err)
		require.True(t, gotTxn == second,
			"each operand classifies under its own ts-pinned view")
	})

	t.Run("a routed operand threads the same operator through subscription classification", func(t *testing.T) {
		pinned := mock_frontend.NewMockTxnOperator(ctrl)
		routeStub := gostub.Stub(&resolveBranchSubscriptionMeta,
			func(_ context.Context, _ string, _ FeSession, op TxnOperator, _ BackgroundExec) (*plan.SubscriptionMeta, error) {
				gotTxn = op
				return &plan.SubscriptionMeta{
					AccountName: "pub_account", Name: "pub1",
					AccountId: 7, DbName: "tdb", Tables: "*",
				}, nil
			})
		t.Cleanup(routeStub.Reset)
		bh := &backgroundExecTest{}
		bh.init()
		operand, err := buildBranchOperand(ctx, ses, bh, pinned, "sub3", "t1")
		require.NoError(t, err)
		require.True(t, operand.isSubscription)
		require.Equal(t, uint32(7), operand.physAccount)
		require.True(t, gotTxn == pinned)
	})
}

// TestBranchClassificationUsesPinnedTransactionView pins review finding 1 at
// the privilege boundary of design §4.1.1: an operand with a resolved ts
// hint is classified under the statement transaction cloned at that
// timestamp, so visibility and authorization describe the historical
// binding; without a hint the live statement transaction is used.
func TestBranchClassificationUsesPinnedTransactionView(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newSes(nil, ctrl)
	defer ses.Close()

	liveTxn := mock_frontend.NewMockTxnOperator(ctrl)
	ses.GetTxnHandler().SetShareTxn(liveTxn)
	pinnedTxn := mock_frontend.NewMockTxnOperator(ctrl)
	ts := &timestamp.Timestamp{PhysicalTime: 1700000000000000000}
	pinnedSnapshot := &plan.Snapshot{TS: ts}

	var gotTxn TxnOperator
	newRecordingResolver := func(t *testing.T, answer func(string) (*plan.SubscriptionMeta, error)) {
		t.Helper()
		stub := gostub.Stub(&resolveBranchSubscriptionMeta,
			func(_ context.Context, db string, _ FeSession, op TxnOperator, _ BackgroundExec) (*plan.SubscriptionMeta, error) {
				gotTxn = op
				return answer(db)
			})
		t.Cleanup(stub.Reset)
	}

	t.Run("no ts hint classifies under the live statement transaction", func(t *testing.T) {
		newRecordingResolver(t, func(string) (*plan.SubscriptionMeta, error) { return nil, nil })
		bh := &backgroundExecTest{}
		bh.init()
		require.NoError(t, requireBranchClassificationVisible(ctx, ses, bh, nil, "localdb", "t1"))
		require.True(t, gotTxn == liveTxn,
			"the current operator must classify unpinned operands, byte-identical to the pre-fix behavior")
	})

	t.Run("a resolved ts hint classifies under the pinned clone", func(t *testing.T) {
		newRecordingResolver(t, func(string) (*plan.SubscriptionMeta, error) { return nil, nil })
		liveTxn.EXPECT().CloneSnapshotOp(*ts).Return(pinnedTxn)
		bh := &backgroundExecTest{}
		bh.init()
		require.NoError(t, requireBranchClassificationVisible(ctx, ses, bh, pinnedSnapshot, "localdb", "t1"))
		require.True(t, gotTxn == pinnedTxn,
			"a ts-pinned operand is classified in its snapshot namespace")
	})

	t.Run("an out-of-scope verdict under the pinned clone keeps the visibility cause", func(t *testing.T) {
		newRecordingResolver(t, func(string) (*plan.SubscriptionMeta, error) {
			return &plan.SubscriptionMeta{
				AccountName: "pub_account", Name: "pub1",
				AccountId: 7, DbName: "tdb", Tables: "t1",
			}, nil
		})
		liveTxn.EXPECT().CloneSnapshotOp(*ts).Return(pinnedTxn)
		bh := &backgroundExecTest{}
		bh.init()
		err := requireBranchClassificationVisible(ctx, ses, bh, pinnedSnapshot, "sub3", "t2")
		require.ErrorContains(t, err, "outside the publication's scope")
		require.True(t, gotTxn == pinnedTxn)
	})

	t.Run("a liveness failure under the pinned clone propagates unchanged", func(t *testing.T) {
		injected := errors.New("subscription sub3 is invalid")
		newRecordingResolver(t, func(string) (*plan.SubscriptionMeta, error) { return nil, injected })
		liveTxn.EXPECT().CloneSnapshotOp(*ts).Return(pinnedTxn)
		bh := &backgroundExecTest{}
		bh.init()
		err := requireBranchClassificationVisible(ctx, ses, bh, pinnedSnapshot, "sub3", "t1")
		require.ErrorIs(t, err, injected)
		require.True(t, gotTxn == pinnedTxn)
	})
}
