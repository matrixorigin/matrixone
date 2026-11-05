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

package multi_update

import (
	"runtime"
	"sync"
	"testing"
	"time"

	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/stretchr/testify/require"
)

// The round-54 review finding (PR #30988 head 622a803baf, mo-bot job
// c081744340e4e323db79a2f3): a cached UpdateWriteS3 operator persists
// data, unwinds before metadata publication, and is reset; its
// transaction then finalizes while the prepared plan sits idle waiting
// for reuse. The sinker's capacity hook the completed statement's
// workspace installed captures that workspace's TRANSACTION, and the
// delegate's Reset preserved the sinker AND its hook — so the idle plan
// kept the finalized workspace's remaining history, its backing arrays
// and its process references reachable until another Prepare or Free,
// pinning completed long-transaction memory independently of any
// object-admission bound. Reset is the idle boundary every execution
// crosses: it must drop the execution-scoped hook, and the next
// execution's Prepare re-arm (round-35) must reinstall it.

// TestMultiUpdateResetDropsCompletedExecutionCapacityHook pins the
// contract's clearing half and its reinstall half on the delegate's own
// persistent sinkers: Reset drops the completed execution's hook while
// the sinker — its buffer pools, arenas and tracked persisted names —
// survives for reuse, exactly as before.
func TestMultiUpdateResetDropsCompletedExecutionCapacityHook(t *testing.T) {
	proc1, stub1 := issue25590ReuseProcRound(t)
	proc2, stub2 := issue25590ReuseProcRound(t)

	update := issue25590S3UpdateOp()

	// Execution 1 under workspace 1: the sinker's lazy creation binds
	// workspace 1's hook and the first spill consults it.
	require.NoError(t, update.Prepare(proc1))
	require.NoError(t, issue25590SpillOneObjectViaSinkers(t, update, proc1))
	require.True(t, update.ctr.s3Writer.insertSinkers[0].HasUncommittedCapacityHook(),
		"execution 1 armed its persistent sinker with workspace 1's capacity hook")
	require.Equal(t, 1, stub1.hookCallCount(),
		"execution 1's spill gated through workspace 1's hook")
	namesAfterExec1 := len(update.ctr.s3Writer.insertSinkers[0].PersistedNames())

	// The between-executions Reset — the idle boundary the cached plan
	// sits at while its transaction finalizes.
	require.NoError(t, update.ctr.s3Writer.reset(proc1))

	require.NotNil(t, update.ctr.s3Writer.insertSinkers[0],
		"the persistent sinker itself stays alive for reuse")
	retained1, hasRetains1 := stub1.retained()
	require.True(t, hasRetains1,
		"reset hands the sinkers' persisted names to the completed workspace's cleanup ownership, unchanged by the clear")
	require.Len(t, retained1, namesAfterExec1,
		"reset's name handoff stays exactly as before")
	require.False(t, update.ctr.s3Writer.insertSinkers[0].HasUncommittedCapacityHook(),
		"reset drops the execution-scoped capacity hook: an idle cached plan must not keep the completed workspace reachable (issue #25590 round-54 review)")

	// Execution 2: the next Prepare re-arms the persistent sinker with
	// the CURRENT workspace before any spill runs — the round-35 rebind
	// contract this clear complements — and the reused sinker spills
	// under the CURRENT workspace alone.
	require.NoError(t, update.Prepare(proc2))
	require.True(t, update.ctr.s3Writer.insertSinkers[0].HasUncommittedCapacityHook(),
		"Prepare reinstalls the current execution's workspace hook")
	require.NoError(t, issue25590SpillOneObjectViaSinkers(t, update, proc2))
	require.Equal(t, 1, stub2.hookCallCount(),
		"the reused sinker gated execution 2's spill through the CURRENT workspace")
	require.Equal(t, 1, stub1.hookCallCount(),
		"the finalized first workspace stayed unreachable across the idle window")

	require.NoError(t, update.ctr.s3Writer.free(proc2))
}

// issue25590IdleWorkspaceProbe stands in for everything the completed
// workspace retains — its remaining history, backing arrays and process
// references: the sinker's hook closure is the only edge that may keep
// it alive after the completed execution unwinds.
type issue25590IdleWorkspaceProbe struct {
	mu    sync.Mutex
	gated int
}

func (p *issue25590IdleWorkspaceProbe) spillGated() {
	p.mu.Lock()
	p.gated++
	p.mu.Unlock()
}

func (p *issue25590IdleWorkspaceProbe) gatingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gated
}

// issue25590IdleRetentionWorkspace arms exactly one execution-scoped
// binding: the first SinkerCapacityOption call captures the probe in
// the returned hook and then drops the workspace's own reference, so
// after reset the probe's only possible keeper is the sinker's hook.
type issue25590IdleRetentionWorkspace struct {
	*mock_frontend.MockWorkspace
	probe *issue25590IdleWorkspaceProbe
}

func (s *issue25590IdleRetentionWorkspace) SinkerCapacityOption() ioutil.SinkerOption {
	probe := s.probe
	s.probe = nil
	return ioutil.WithUncommittedCapacityHook(func(string) error {
		probe.spillGated()
		return nil
	})
}

// TestMultiUpdateResetReleasesCompletedWorkspaceRetention is the
// reclamation oracle for the same finding: with the delegate and its
// sinker still alive — the reuse contract the fix must not weaken — the
// completed workspace stand-in must become unreachable the moment
// Reset drops the execution-scoped hook. A finalizer proves
// collection with the sinker kept alive, so the pre-fix tree cannot
// pass it: the retained hook holds the closure's captured workspace.
func TestMultiUpdateResetReleasesCompletedWorkspaceRetention(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	ws := &issue25590IdleRetentionWorkspace{MockWorkspace: mock_frontend.NewMockWorkspace(ctrl)}
	probe := &issue25590IdleWorkspaceProbe{}
	ws.probe = probe
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(ws).AnyTimes()
	proc.Base.TxnOperator = txnOp

	update := issue25590S3UpdateOp()

	require.NoError(t, update.Prepare(proc))
	require.NoError(t, issue25590SpillOneObjectViaSinkers(t, update, proc))
	require.True(t, update.ctr.s3Writer.insertSinkers[0].HasUncommittedCapacityHook(),
		"the first execution armed its sinker with the completed workspace's hook")
	require.Equal(t, 1, probe.gatingCount(),
		"the spill gated through the hook closure that captured the workspace stand-in")
	require.Nil(t, ws.probe,
		"the workspace stub holds no reference of its own — only the sinker's hook can keep the stand-in alive")

	require.NoError(t, update.ctr.s3Writer.reset(proc))
	require.False(t, update.ctr.s3Writer.insertSinkers[0].HasUncommittedCapacityHook(),
		"reset dropped the hook binding the completed workspace")

	collected := make(chan struct{})
	runtime.SetFinalizer(probe, func(*issue25590IdleWorkspaceProbe) { close(collected) })
	probe = nil
	require.True(t, issue25590IdleWorkspaceCollected(t, collected),
		"the completed workspace stand-in was collected while the delegate and its sinker stay alive — reset released the execution-scoped retention (issue #25590 round-54 review)")

	require.NoError(t, update.ctr.s3Writer.free(proc))
}

// issue25590IdleWorkspaceCollected forces repeated garbage collections
// under a bounded deadline until either the finalizer signals collection
// or the deadline expires. The pre-fix tree runs the full budget and
// fails; a fixed tree collects within one or two collections.
func issue25590IdleWorkspaceCollected(
	t *testing.T, collected <-chan struct{},
) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		select {
		case <-collected:
			return true
		case <-time.After(20 * time.Millisecond):
		}
	}
	return false
}
