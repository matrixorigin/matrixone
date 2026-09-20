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
