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
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// The real SQL/MVCC fixture covers persistence. This seam deterministically
// covers storage errors and exhausted counters without billions of operations.
func recoveryCoordinatorFaultFixture(t *testing.T, s ViewRecoveryState, revision uint64, expired bool, fail string) (ViewMetadataCoordinator, *[]string) {
	t.Helper()
	proc := testutil.NewProcess(t)
	statements := []string{}
	sql := executor.NewMemExecutor(func(q string) (executor.Result, error) {
		statements = append(statements, q)
		if fail != "" && strings.Contains(q, fail) {
			return executor.Result{}, errors.New("injected catalog fault")
		}
		if strings.HasPrefix(q, "select state,") {
			data, err := json.Marshal(s)
			require.NoError(t, err)
			r := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}, proc.Mp())
			r.NewBatchWithRowCount(1)
			deadline := "0"
			if expired {
				deadline = "1"
			}
			for i, v := range []string{string(data), strconv.FormatUint(revision, 10), deadline, strconv.FormatUint(s.catalogMutation, 10)} {
				require.NoError(t, executor.AppendStringRows(r, i, []string{v}))
			}
			return r.GetResult(), nil
		}
		return executor.Result{AffectedRows: 1}, nil
	})
	return ViewMetadataCoordinator{SQL: sql}, &statements
}

type recoveryFaultTransport struct {
	proof          *pb.CatalogMetadataBarrierState
	err            error
	applies, reads int
}

func (r *recoveryFaultTransport) ApplyCatalogReceipt(context.Context, *pb.CatalogMetadataReceipt) (*pb.CatalogMetadataBarrierState, error) {
	r.applies++
	return r.proof, r.err
}
func (r *recoveryFaultTransport) ReadCatalogBarrier(context.Context) (*pb.CatalogMetadataBarrierState, error) {
	r.reads++
	return r.proof, r.err
}

type serialRecoverySQL struct {
	inner   executor.SQLExecutor
	mu      sync.Mutex
	attempt chan struct{}
}

func (s *serialRecoverySQL) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	return s.inner.Exec(ctx, sql, opts)
}

func (s *serialRecoverySQL) ExecTxn(ctx context.Context, fn func(executor.TxnExecutor) error, opts executor.Options) error {
	if s.attempt != nil {
		close(s.attempt)
		s.attempt = nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inner.ExecTxn(ctx, fn, opts)
}

type blockingCompletionTransport struct {
	entered chan struct{}
	release chan struct{}
	proof   *pb.CatalogMetadataBarrierState
}

func (t *blockingCompletionTransport) ApplyCatalogReceipt(ctx context.Context, _ *pb.CatalogMetadataReceipt) (*pb.CatalogMetadataBarrierState, error) {
	close(t.entered)
	select {
	case <-t.release:
		return t.proof, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestRecoveryScopeCannotDiscardUnfinishedWork(t *testing.T) {
	account := ViewRecoveryScope{AccountID: 7}
	database := ViewRecoveryScope{AccountID: 7, Database: "db"}
	table := ViewRecoveryScope{AccountID: 7, Database: "db", Relation: "v"}
	for _, pair := range [][2]ViewRecoveryScope{{{All: true}, account}, {account, database}, {database, table}, {table, table}} {
		require.True(t, viewRecoveryScopeContains(pair[0], pair[1]))
	}
	for _, pair := range [][2]ViewRecoveryScope{{account, {All: true}}, {database, account}, {table, database}, {table, {AccountID: 8, Database: "db", Relation: "v"}}, {table, {AccountID: 7, Database: "db", Relation: "other"}}} {
		require.False(t, viewRecoveryScopeContains(pair[0], pair[1]))
	}
}

func TestCoordinatorClaimAndMutationFences(t *testing.T) {
	base := ViewRecoveryState{Version: 1, ViewRecoveryClaim: ViewRecoveryClaim{Epoch: 1, Generation: 1, ClaimID: 7, LeaseEpoch: 9, Owner: "old"}}
	for _, test := range []struct {
		name     string
		change   func(*ViewRecoveryState)
		expired  bool
		revision uint64
		fail     string
	}{
		{name: "held lease"},
		{name: "dirty catalog", change: func(s *ViewRecoveryState) { s.catalogMutation = 1 }, expired: true},
		{name: "lease overflow", change: func(s *ViewRecoveryState) { s.LeaseEpoch = math.MaxUint64 }, expired: true},
		{name: "revision overflow", revision: math.MaxUint64, expired: true},
		{name: "failed publication", fail: "update mo_catalog.mo_view_recovery", expired: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := base
			if test.change != nil {
				test.change(&s)
			}
			c, _ := recoveryCoordinatorFaultFixture(t, s, test.revision, test.expired, test.fail)
			claim, err := c.Claim(context.Background(), 1, 1, 7, "new")
			require.Error(t, err)
			require.Empty(t, claim)
		})
	}
	c, _ := recoveryCoordinatorFaultFixture(t, base, 0, true, "")
	claim, err := c.Claim(context.Background(), 1, 1, 7, "new")
	require.NoError(t, err)
	require.Equal(t, uint64(10), claim.LeaseEpoch)
	_, err = c.Claim(context.Background(), 1, 1, 8, "new")
	require.Error(t, err)
	_, err = c.Claim(context.Background(), 1, 1, 7, "")
	require.Error(t, err)
	sameOwner, _ := recoveryCoordinatorFaultFixture(t, base, 0, false, "")
	claimedAgain, err := sameOwner.Claim(context.Background(), 1, 1, 7, "old")
	require.NoError(t, err)
	require.Greater(t, claimedAgain.LeaseEpoch, base.LeaseEpoch)
	dirty := base
	dirty.catalogMutation = 1
	c, _ = recoveryCoordinatorFaultFixture(t, dirty, 0, false, "")
	require.Error(t, c.Complete(context.Background(), base.ViewRecoveryClaim))
	ok, err := c.IsCurrent(context.Background(), 1, 1, 0, 10)
	require.NoError(t, err)
	require.False(t, ok)
	_, err = c.Page(context.Background(), base.ViewRecoveryClaim)
	require.Error(t, err)
}

func TestFullRecoveryQueueCanInstallReplacementGeneration(t *testing.T) {
	state := ViewRecoveryState{Version: 1, ViewRecoveryClaim: ViewRecoveryClaim{Epoch: 1, Generation: 1}, Scope: ViewRecoveryScope{All: true}, WorkRows: viewRecoveryMaxWork}
	c, statements := recoveryCoordinatorFaultFixture(t, state, 0, false, "")
	underlying := c.SQL
	proc := testutil.NewProcess(t)
	c.SQL = executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if strings.Contains(sql, "from mo_catalog.mo_view_recovery_work where generation<2") {
			r := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}, proc.Mp())
			r.NewBatchWithRowCount(4)
			for column, values := range [][]string{{"1", "1", "1", "1"}, {"node", "node", "node", "node"}, {"0", "0", "0", "0"}, {"10", "11", "12", "13"}} {
				require.NoError(t, executor.AppendStringRows(r, column, values))
			}
			return r.GetResult(), nil
		}
		return underlying.Exec(context.Background(), sql, executor.Options{})
	})
	require.NoError(t, c.Require(context.Background(), 2, 2, ViewRecoveryScope{All: true}))
	require.Contains(t, (*statements)[len(*statements)-1], `"generation":2`)
	require.Contains(t, (*statements)[len(*statements)-1], `"work_rows":65536`)
}

func TestCompletionPublishRechecksMutationAfterRead(t *testing.T) {
	s := ViewRecoveryState{Version: 1, ViewRecoveryClaim: ViewRecoveryClaim{Epoch: 2, Generation: 3, ClaimID: 7, LeaseEpoch: 1, Owner: "worker"}, Completed: 3}
	s.evidence(2)
	accepted := &pb.CatalogMetadataBarrierState{MembershipEpoch: 2, RequiredGeneration: 3, EvidenceInitialized: true, Arbitration: &pb.CatalogMetadataArbitration{ClaimID: 7, CompletedReceipt: cloneViewRecoveryReceipt(s.Outbox[2])}}
	proc := testutil.NewProcess(t)
	loads := 0
	sql := executor.NewMemExecutor(func(q string) (executor.Result, error) {
		if strings.HasPrefix(q, "select state,") {
			loads++
			data, err := json.Marshal(s)
			require.NoError(t, err)
			mutation := "0"
			if loads > 1 {
				mutation = "1"
			}
			r := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}, proc.Mp())
			r.NewBatchWithRowCount(1)
			for i, value := range []string{string(data), "0", "0", mutation} {
				require.NoError(t, executor.AppendStringRows(r, i, []string{value}))
			}
			return r.GetResult(), nil
		}
		return executor.Result{AffectedRows: 1}, nil
	})
	transport := &recoveryFaultTransport{proof: accepted}
	progress, err := (ViewMetadataCoordinator{SQL: sql}).Publish(context.Background(), transport)
	require.NoError(t, err)
	require.True(t, progress)
	require.Equal(t, 2, loads)
	require.Zero(t, transport.applies, "a mutation committed after the initial Read must fence COMPLETE submission")
	require.Equal(t, 1, transport.reads)
}

func TestCompletionPublishSerializesCatalogMutation(t *testing.T) {
	s := ViewRecoveryState{Version: 1, ViewRecoveryClaim: ViewRecoveryClaim{Epoch: 2, Generation: 3, ClaimID: 7, LeaseEpoch: 1, Owner: "worker"}, Completed: 3}
	s.evidence(2)
	accepted := &pb.CatalogMetadataBarrierState{MembershipEpoch: 2, RequiredGeneration: 3, EvidenceInitialized: true, Arbitration: &pb.CatalogMetadataArbitration{ClaimID: 7, CompletedReceipt: cloneViewRecoveryReceipt(s.Outbox[2])}}
	coordinator, _ := recoveryCoordinatorFaultFixture(t, s, 0, false, "")
	serial := &serialRecoverySQL{inner: coordinator.SQL}
	coordinator.SQL = serial
	transport := &blockingCompletionTransport{entered: make(chan struct{}), release: make(chan struct{}), proof: accepted}
	publishDone := make(chan error, 1)
	go func() {
		_, err := coordinator.Publish(context.Background(), transport)
		publishDone <- err
	}()
	<-transport.entered

	// Require models the catalog mutation boundary: it announces its transaction
	// attempt before waiting for the same serialized transaction owner.
	attempt := make(chan struct{})
	serial.attempt = attempt
	mutationDone := make(chan error, 1)
	go func() { mutationDone <- coordinator.Require(context.Background(), 3, 4, ViewRecoveryScope{All: true}) }()
	<-attempt
	select {
	case err := <-mutationDone:
		require.FailNow(t, "catalog mutation crossed the COMPLETE publication fence", "error: %v", err)
	default:
	}
	close(transport.release)
	require.NoError(t, <-publishDone)
	// The fixture intentionally does not persist its first transaction's JSON;
	// only completion ordering is the oracle for the competing transaction.
	<-mutationDone
}

func TestCoordinatorOutboxReplayAndRetirement(t *testing.T) {
	s := ViewRecoveryState{Version: 1, ViewRecoveryClaim: ViewRecoveryClaim{Epoch: 2, Generation: 3, ClaimID: 7, LeaseEpoch: 1, Owner: "worker"}, Completed: 3}
	s.evidence(2)
	accepted := &pb.CatalogMetadataBarrierState{MembershipEpoch: 2, RequiredGeneration: 3, EvidenceInitialized: true, Arbitration: &pb.CatalogMetadataArbitration{ClaimID: 7, CompletedReceipt: s.Outbox[2]}}
	ctx := context.Background()
	c, _ := recoveryCoordinatorFaultFixture(t, s, 0, false, "")
	lost := &recoveryFaultTransport{err: errors.New("lost receipt response")}
	progress, err := c.Publish(ctx, lost)
	require.Error(t, err)
	require.False(t, progress)
	missing := &recoveryFaultTransport{}
	_, err = c.Publish(ctx, missing)
	require.Error(t, err)
	replay := &recoveryFaultTransport{proof: accepted}
	progress, err = c.Publish(ctx, replay)
	require.NoError(t, err)
	require.True(t, progress)
	require.Equal(t, 1, replay.applies)
	s.catalogMutation = 1
	c, statements := recoveryCoordinatorFaultFixture(t, s, 0, false, "")
	proof := &recoveryFaultTransport{proof: accepted}
	progress, err = c.Publish(ctx, proof)
	require.NoError(t, err)
	require.True(t, progress)
	require.Zero(t, proof.applies, "dirty completion must not be submitted")
	require.Equal(t, 1, proof.reads)
	require.Contains(t, (*statements)[len(*statements)-1], `"outbox":[null,null,null]`)
	future := &recoveryFaultTransport{proof: &pb.CatalogMetadataBarrierState{MembershipEpoch: 3, RequiredGeneration: 4, EvidenceInitialized: true, Arbitration: &pb.CatalogMetadataArbitration{}}}
	progress, err = c.Publish(ctx, future)
	require.NoError(t, err)
	require.True(t, progress)
	require.Zero(t, future.applies)
	require.Equal(t, 1, future.reads)
}
