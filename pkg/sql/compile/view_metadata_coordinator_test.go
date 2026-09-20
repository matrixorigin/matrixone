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
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestViewRecoveryStateContracts(t *testing.T) {
	base := ViewRecoveryState{Version: 1, ViewRecoveryClaim: ViewRecoveryClaim{Epoch: 1, Generation: 2, ClaimID: 3, LeaseEpoch: 4, Owner: "worker"}}
	require.NoError(t, base.validate())
	require.NoError(t, base.check(base.ViewRecoveryClaim, false))
	require.Error(t, base.check(base.ViewRecoveryClaim, true))
	for _, change := range []func(*ViewRecoveryState){
		func(s *ViewRecoveryState) { s.Version = 2 },
		func(s *ViewRecoveryState) { s.Completed = 3 },
		func(s *ViewRecoveryState) { s.WorkRows = viewRecoveryMaxWork + 1 },
		func(s *ViewRecoveryState) { s.Owner = strings.Repeat("x", 129) },
		func(s *ViewRecoveryState) { s.Epoch = 0 },
		func(s *ViewRecoveryState) { s.Generation = 0 },
		func(s *ViewRecoveryState) { s.ClaimID = 0 },
		func(s *ViewRecoveryState) { s.Scope = ViewRecoveryScope{Relation: "v"} },
		func(s *ViewRecoveryState) { s.Scope = ViewRecoveryScope{All: true, AccountID: 7} },
		func(s *ViewRecoveryState) { s.Scope.Database = strings.Repeat("x", 5001) },
	} {
		s := base
		change(&s)
		require.Error(t, s.validate())
	}
	for slot := 0; slot < 3; slot++ {
		s := base
		s.evidence(slot)
		require.NoError(t, s.validate())
		encoded, err := json.Marshal(s.Outbox[slot])
		require.NoError(t, err)
		require.Less(t, len(encoded), 4096)
		first := s.Outbox[slot]
		s.LeaseEpoch++
		s.Owner = "successor"
		s.evidence(slot)
		require.Equal(t, first, s.Outbox[slot], "physical takeover must replay the same logical receipt")
		s.Outbox[slot].RequiredGeneration++
		require.Error(t, s.validate())
	}
	for _, change := range []func(*ViewRecoveryClaim){
		func(c *ViewRecoveryClaim) { c.Epoch++ }, func(c *ViewRecoveryClaim) { c.Generation++ },
		func(c *ViewRecoveryClaim) { c.ClaimID++ }, func(c *ViewRecoveryClaim) { c.LeaseEpoch++ }, func(c *ViewRecoveryClaim) { c.Owner = "other" },
	} {
		claim := base.ViewRecoveryClaim
		change(&claim)
		require.Error(t, base.check(claim, false))
	}
}

func TestViewRecoveryReceiptRequiresDurableProof(t *testing.T) {
	s := ViewRecoveryState{Version: 1, ViewRecoveryClaim: ViewRecoveryClaim{Epoch: 2, Generation: 3, ClaimID: 7, LeaseEpoch: 1, Owner: "worker"}}
	for slot := 0; slot < 3; slot++ {
		s.evidence(slot)
		receipt := s.Outbox[slot]
		require.False(t, viewRecoveryReceiptProven(receipt, nil))
		proof := &pb.CatalogMetadataBarrierState{MembershipEpoch: 2, RequiredGeneration: 3, EvidenceInitialized: true, Arbitration: &pb.CatalogMetadataArbitration{ClaimID: 7}}
		require.False(t, viewRecoveryReceiptProven(receipt, proof))
		switch slot {
		case 0:
			proof.Arbitration.RequiredReceipt = receipt
		case 1:
			proof.Arbitration.StartedReceipt = receipt
		case 2:
			proof.Arbitration.CompletedReceipt = receipt
		}
		require.True(t, viewRecoveryReceiptProven(receipt, proof))
		proof.EvidenceInitialized = false
		require.False(t, viewRecoveryReceiptProven(receipt, proof))
		proof.EvidenceInitialized = true
		proof.MembershipEpoch = 3
		require.False(t, viewRecoveryReceiptProven(receipt, proof))
		proof.RequiredGeneration = 4
		require.True(t, viewRecoveryReceiptProven(receipt, proof), "strictly newer committed epoch and generation retire old evidence")
	}
}

func TestViewRecoveryCatalogStateDecode(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, data := range []string{`{"version":1}`, `{"version":2}`, `{"version":1,"unknown":1}`, `{"version":1} {}`, strings.Repeat(" ", viewRecoveryMaxStateBytes+1), `broken`} {
		result := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}, proc.Mp())
		result.NewBatchWithRowCount(1)
		for i, value := range []string{data, "7", "1", "0", "0"} {
			require.NoError(t, executor.AppendStringRows(result, i, []string{value}))
		}
		txn := executor.NewMemTxnExecutor(func(string) (executor.Result, error) { return result.GetResult(), nil }, nil)
		_, revision, expired, err := loadViewRecovery(txn)
		if data == `{"version":1}` {
			require.NoError(t, err)
			require.Equal(t, uint64(7), revision)
			require.True(t, expired)
		} else {
			require.Error(t, err)
		}
	}
	txn := executor.NewMemTxnExecutor(func(string) (executor.Result, error) { return executor.Result{}, nil }, nil)
	_, _, _, err := loadViewRecovery(txn)
	require.Error(t, err)
}

func TestViewRecoveryMutationRejectsStaleAndOverflow(t *testing.T) {
	scope := ViewRecoveryScope{Database: "db", Relation: "v"}
	state := ViewRecoveryState{Version: 1, ViewRecoveryClaim: ViewRecoveryClaim{Epoch: 2, Generation: 3}, Scope: scope}
	txn := executor.NewMemTxnExecutor(func(string) (executor.Result, error) {
		t.Fatal("rejected mutation must not issue SQL")
		return executor.Result{}, nil
	}, nil)
	for _, tuple := range [][2]uint64{{0, 0}, {1, 2}, {2, 2}, {3, 3}} {
		_, err := requireViewRecovery(tuple[0], tuple[1], scope)(txn, &state, false)
		require.Error(t, err)
	}
	_, err := requireViewRecovery(2, 3, scope)(txn, &state, false)
	require.NoError(t, err)
	_, err = requireViewRecovery(2, 3, ViewRecoveryScope{All: true})(txn, &state, false)
	require.Error(t, err)
	state.evidence(0)
	_, err = requireViewRecovery(3, 4, scope)(txn, &state, false)
	require.Error(t, err)
	c := ViewMetadataCoordinator{}
	require.Error(t, c.Require(context.Background(), 1, 1, scope))
	_, err = c.Publish(context.Background(), nil)
	require.Error(t, err)
	_, err = c.Page(context.Background(), ViewRecoveryClaim{})
	require.Error(t, err)
	require.Error(t, c.Complete(context.Background(), ViewRecoveryClaim{}))
	_, err = viewRecoveryUint("-1")
	require.Error(t, err)
	_, err = viewRecoveryUint("18446744073709551616")
	require.Error(t, err)
	n, err := viewRecoveryUint("18446744073709551615")
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), n)
	require.True(t, moerr.IsMoErrCode(state.check(ViewRecoveryClaim{}, false), moerr.ErrTxnNeedRetryWithDefChanged))
}

type snapshotCatalogExecutor struct {
	viewMetadataCleanupRecordingExecutor
	txns []client.TxnOperator
}

func (e *snapshotCatalogExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	e.txns = append(e.txns, opts.Txn())
	return e.viewMetadataCleanupRecordingExecutor.Exec(ctx, sql, opts)
}

func TestRecoverySubscriptionSnapshotDoesNotAliasCurrentCache(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	current := mock_frontend.NewMockTxnOperator(ctrl)
	first := mock_frontend.NewMockTxnOperator(ctrl)
	second := mock_frontend.NewMockTxnOperator(ctrl)
	current.EXPECT().Txn().Return(pbtxn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 1000}}).AnyTimes()
	current.EXPECT().CloneSnapshotOp(timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 7}).Return(first)
	current.EXPECT().CloneSnapshotOp(timestamp.Timestamp{PhysicalTime: 456, LogicalTime: 7}).Return(second)
	proc.Base.TxnOperator = current
	exec := &snapshotCatalogExecutor{}
	installViewMetadataTestExecutor(t, proc, exec)
	ctx := &recoveryCompilerContext{compilerContext: &compilerContext{ctx: proc.Ctx, proc: proc}}
	_, err := ctx.GetSubscriptionMeta("sub", nil)
	require.NoError(t, err)
	snapshot := &planpb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 7}, Tenant: &planpb.SnapshotTenant{TenantID: 9}}
	_, err = ctx.GetSubscriptionMeta("sub", snapshot)
	require.NoError(t, err)
	_, err = ctx.GetSubscriptionMeta("sub", snapshot)
	require.NoError(t, err)
	snapshot.TS.PhysicalTime = 456
	_, err = ctx.GetSubscriptionMeta("sub", snapshot)
	require.NoError(t, err)
	_, err = ctx.GetSubscriptionMeta("sub@9/123/7", nil)
	require.NoError(t, err)
	require.Len(t, exec.sqls, 4)
	require.NotContains(t, exec.sqls[0], "MO_TS")
	require.NotContains(t, exec.sqls[1], "MO_TS")
	require.Contains(t, exec.sqls[1], "sub_account_id=9")
	require.Same(t, current, exec.txns[0])
	require.Same(t, first, exec.txns[1])
	require.Same(t, second, exec.txns[2])
	require.Same(t, current, exec.txns[3])
	require.Contains(t, exec.sqls[3], "sub@9/123/7")
}
