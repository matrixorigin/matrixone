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

package compile

// Design r15 (issue #25590 round-45 reviews): a persistence-producing
// scope is NEVER dispatched to a below-adoption receiver. Three review
// rounds falsified every capacity-keyed ladder (the admitted run's
// retained tail is workload-dependent, and any token-like admission
// double-charges the capacity boundary), so the enforced
// size-independent finite bound is ZERO admitted legacy obligations:
// the refusal runs at the coordinator's remoteRun AFTER the round-44
// receiver confirm and BEFORE sendPipeline, with the definite
// ErrNotSupported capability error — the exact mirror of the D3'
// refuseRemoteWriteFromIncapableCoordinator. This file pins the
// contract the design's §7 r15 row names: the classification matrix,
// the dispatch-path integration (zero pipeline bytes leave for a
// refused legacy run; the payload send is the UT-plane reach witness
// the BVT plane expresses through inject_send_pipeline), the sustained
// zero-admission companion (arbitrarily many attempts, sequential and
// barrier-concurrent, all refuse deterministically and the payload
// count stays zero — nothing is ever dispatched, so nothing can
// persist or book anywhere), and the capable-receiver positive control
// whose payload send is REQUIRED after the receiver advertisement is
// restored.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// The r15 refusal gate: both direction thresholds, the absent/zero
// fail-closed default, the scope classification boundary (plain query
// scopes and same-mode-plain multi-update never consult it), the
// nested tree, the empty null input, and the designed default
// disposition — statement-only rollback, exactly like the D3' refusal.
func TestIssue25590RefuseRemoteWriteToNonRetainingFragmentR15(t *testing.T) {
	ctx := context.Background()

	write := &Scope{}
	mu := multi_update.NewArgument()
	mu.Action = multi_update.UpdateWriteS3
	write.RootOp = mu

	ordinary := &Scope{}
	ordinary.RootOp = connector.NewArgument()
	ordinary.RootOp.GetOperatorBase().AppendChild(value_scan.NewArgument())

	// A below-advertisement receiver is refused with the designed class
	// and message — the absent/zero default of an unconfirmed stream and
	// every real below-adoption value behave identically, state- and
	// input-independent: the recorded L−1+K unbounded-tail evidence
	// (round 45) found no ledger state that could bound an admitted
	// legacy run, so the finite bound is zero admissions.
	err := refuseRemoteWriteToNonRetainingFragment(ctx, 0, write)
	require.Error(t, err)
	require.ErrorContains(t, err, "receiver protocol capability")
	require.False(t, moerr.MustRollbackTxn(err),
		"the designed default disposition is statement-only rollback: ErrNotSupported is outside moerr's whole-transaction rollback list")
	require.Error(t, refuseRemoteWriteToNonRetainingFragment(ctx, defines.MORPCVersion129, write))
	require.Error(t, refuseRemoteWriteToNonRetainingFragment(ctx, defines.MORPCVersion130, write))
	require.Error(t, refuseRemoteWriteToNonRetainingFragment(ctx, defines.MORPCVersion131, write),
		"main's shipped latest stays below the first adoption build for dispatch")

	// At or above the first adoption build, the producer-mode run
	// dispatches and keeps the intake admission gate exactly as
	// recorded (r44's stream fork still governs its received batches).
	require.NoError(t, refuseRemoteWriteToNonRetainingFragment(ctx, coordinatorIntakeAdoptionVersion, write))
	require.NoError(t, refuseRemoteWriteToNonRetainingFragment(ctx, coordinatorIntakeAdoptionVersion+7, write))

	// A producer nested in PreScopes stays a producer for the refusal.
	parent := &Scope{}
	plainWriter := &Scope{}
	plain := multi_update.NewArgument()
	plain.Action = multi_update.UpdateWriteTable
	plainWriter.RootOp = plain
	parent.PreScopes = append(parent.PreScopes, write)
	require.Error(t, refuseRemoteWriteToNonRetainingFragment(ctx, defines.MORPCVersion131, parent),
		"a nested registered producer keeps the scope a write")

	// A plain-mode multi-update (UpdateWriteTable: the ordinary-rows
	// shape the relay recognizer must reject too) never consults the
	// refusal — the mode, not the operator family, is the provenance.
	require.NoError(t, refuseRemoteWriteToNonRetainingFragment(ctx, defines.MORPCVersion131, plainWriter),
		"the same operator family in a non-persistence mode is ordinary traffic again")

	// A plain query fragment over any receiver keeps base semantics —
	// mixed-cluster read traffic never consults the refusal.
	require.NoError(t, refuseRemoteWriteToNonRetainingFragment(ctx, 0, ordinary))
	require.NoError(t, refuseRemoteWriteToNonRetainingFragment(ctx, defines.MORPCVersion131, ordinary))

	// Empty null input boundary.
	require.NoError(t, refuseRemoteWriteToNonRetainingFragment(ctx, 0, nil))
}

// r15StreamStats aggregates the dispatch-scene counters across every
// stream the mock pipeline client mints: the confirmation sends, the
// PIPELINE BYTES that ever leave (the UT-plane reach witness), and the
// stream closes the public RemoteRun owns.
type r15StreamStats struct {
	checks         atomic.Int64
	payloads       atomic.Int64
	closes         atomic.Int64
	confirmVersion int64
}

// r15InstallPipelineClient installs a test pipeline client that mints
// a fresh mock stream per RemoteRun — the same runtime-global override
// the temporal and expression protocol tests use — so sequential and
// concurrent dispatch attempts share only the stats.
func r15InstallPipelineClient(t *testing.T, c *Compile, stats *r15StreamStats) {
	t.Helper()
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	old, had := rt.GetGlobalVariables(moruntime.PipelineClient)
	client := &testPipelineClient{genStream: func(context.Context, string) (morpc.Stream, error) {
		replies := make(chan morpc.Message, 2)
		stream := mock_morpc.NewMockStream(gomock.NewController(t))
		stream.EXPECT().ID().Return(uint64(17)).AnyTimes()
		stream.EXPECT().Receive().Return(replies, nil)
		stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, m morpc.Message) error {
				switch m.(*pipeline.Message).GetCmd() {
				case pipeline.Method_PipelineProtocolCheck:
					stats.checks.Add(1)
					replies <- &pipeline.Message{
						Id: 17, Cmd: pipeline.Method_PipelineProtocolCheck,
						Sid: pipeline.Status_Last, ProtocolVersion: stats.confirmVersion,
					}
				case pipeline.Method_PipelineMessage:
					// A refused legacy run must never reach here — the
					// payload count is the zero-dispatch oracle the
					// review's P3 names at the SQL plane via the
					// inject_send_pipeline receive-log marker.
					stats.payloads.Add(1)
					replies <- &pipeline.Message{Id: 17, Sid: pipeline.Status_MessageEnd}
				default:
					t.Errorf("unexpected stream command %v", m.(*pipeline.Message).GetCmd())
				}
				return nil
			}).AnyTimes()
		stream.EXPECT().Close(true).DoAndReturn(
			func(bool) error { stats.closes.Add(1); return nil }).AnyTimes()
		return stream, nil
	}}
	rt.SetGlobalVariables(moruntime.PipelineClient, client)
	t.Cleanup(func() {
		if had {
			rt.SetGlobalVariables(moruntime.PipelineClient, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.PipelineClient, client)
		}
	})
}

// r15ProducerScope builds the dispatched persistence-producing Remote
// scope: a multi-update in the S3 metadata mode — and NOTHING else, so
// the same scope is also the decided EMPTY-INPUT producer statement:
// the runtime input is invisible at dispatch (the plan's mode is the
// only fact the coordinator holds), which is exactly why the empty
// statement joins the refusal.
func r15ProducerScope(t *testing.T, c *Compile) *Scope {
	t.Helper()
	mu := multi_update.NewArgument()
	mu.Action = multi_update.UpdateWriteS3
	c.proc.Base.TxnOperator = fakeTxnOperator{}
	return &Scope{
		Magic:         Remote,
		Proc:          c.proc,
		NodeInfo:      engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:        mu,
		ScopeAnalyzer: NewScopeAnalyzer(),
	}
}

// The dispatch-path integration: a producer-mode scope whose stream
// confirmed a below-adoption receiver refuses at remoteRun — the
// confirmation runs once, ZERO pipeline bytes leave (the receiver
// never receives the encoded scope, so nothing can persist or relay
// anything behind any admission state), the statement gets the
// definite capability error whose default disposition is
// statement-only rollback, and the public RemoteRun owns cleanup.
func TestIssue25590RemoteRunRefusesNonRetainingProducerScopeR15(t *testing.T) {
	for _, advertised := range []int64{0, defines.MORPCVersion129, defines.MORPCVersion131} {
		t.Run(map[int64]string{
			0:                       "unconfirmed zero fails closed",
			defines.MORPCVersion129: "older main build",
			defines.MORPCVersion131: "shipped latest",
		}[advertised], func(t *testing.T) {
			c, _ := expressionProtocolTestCompile(t)
			stats := &r15StreamStats{confirmVersion: advertised}
			r15InstallPipelineClient(t, c, stats)
			scope := r15ProducerScope(t, c)
			err := scope.RemoteRun(c)
			require.Error(t, err)
			require.ErrorContains(t, err, "receiver protocol capability")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			require.False(t, moerr.MustRollbackTxn(err),
				"the refusal is statement-only rollback: the explicit transaction stays open")
			require.Equal(t, int64(1), stats.checks.Load(),
				"the receiver confirm ran exactly once")
			require.Zero(t, stats.payloads.Load(),
				"the refusal returned before sendPipeline: no pipeline bytes ever left")
		})
	}
}

// The positive control: the identical persistence-producing scope
// dispatches the moment the receiver advertisement reaches the first
// adoption build — the capable leg keeps the recorded intake gate, and
// the payload send is REQUIRED (the UT-plane reach witness; the BVT
// plane requires the inject_send_pipeline receive-log marker on this
// leg — design §7 r15 row, the design re-review's P3).
func TestIssue25590RemoteRunProducerPassesWithCapableReceiverR15(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	stats := &r15StreamStats{confirmVersion: coordinatorIntakeAdoptionVersion}
	r15InstallPipelineClient(t, c, stats)
	scope := r15ProducerScope(t, c)
	err := scope.RemoteRun(c)
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.checks.Load(), "the confirm ran first")
	require.Equal(t, int64(1), stats.payloads.Load(),
		"the capable-receiver control dispatched and executed the pipeline payload")
	require.Positive(t, stats.closes.Load(), "RemoteRun owns successful cleanup")
}

// The sustained zero-admission companion (the delivered form of the
// round-45 RED evidence): arbitrarily many persistence-producing
// dispatch attempts against a below-adoption receiver — a long
// sequential campaign and a barrier-concurrent burst sharing one
// scene — all refuse deterministically and the payload count stays
// ZERO: no fragment is ever dispatched, so no ledger state, memory
// population or storage object can grow with the attempts at any
// admission bound, exactly the finite size-independent bound the
// design's third round converged on.
func TestIssue25590LegacyProducerDispatchSustainedZeroAdmissionR15(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	stats := &r15StreamStats{confirmVersion: defines.MORPCVersion131}
	r15InstallPipelineClient(t, c, stats)

	// The sequential campaign: 32 attempts, the same shape as the
	// round-45 reproduction's legacy leg, each against its own stream.
	const attempts = 32
	for i := 0; i < attempts; i++ {
		scope := r15ProducerScope(t, c)
		err := scope.RemoteRun(c)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported),
			"sequential attempt %d refuses at dispatch: %v", i, err)
	}
	require.Equal(t, int64(attempts), stats.checks.Load())
	require.Zero(t, stats.payloads.Load(),
		"32 sustained legacy attempts dispatched nothing: the retained population cannot grow")

	// The barrier-concurrent burst: the re-review's concurrency oracle
	// restated for the zero-admission policy — every in-flight
	// producer-mode dispatch refuses, whatever the (absent) ledger
	// interplay.
	const concurrency = 8
	barrier := make(chan struct{})
	errs := make([]error, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			scope := r15ProducerScope(t, c)
			<-barrier
			errs[idx] = scope.RemoteRun(c)
		}(i)
	}
	close(barrier)
	wg.Wait()
	for i, err := range errs {
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported),
			"concurrent attempt %d refuses deterministically: %v", i, err)
	}
	require.Equal(t, int64(attempts+concurrency), stats.checks.Load())
	require.Zero(t, stats.payloads.Load(),
		"the total dispatched payload count across the whole campaign is exactly zero")
	require.Equal(t, int64(attempts+concurrency), stats.closes.Load(),
		"every refused dispatch owns its stream cleanup")
}
