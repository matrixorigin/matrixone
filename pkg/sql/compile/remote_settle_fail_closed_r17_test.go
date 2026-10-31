// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

// The focused red/green pin of the r17 fail-closed settlement (issue
// #25590 round-52 review): an ADMITTED producer run that carries NO
// batch flow must NEVER settle transferred - the fragment keeps its
// local owner and its own terminal deletes the residue. Before the fix
// the settlement transferred such a clean run with no adoption proof
// (the receiver consulted only the optional batch-flow drain), which
// is exactly the hole the disable-stream-reuse configuration exposes:
// the run shipped, the barrier did not exist, and the recording side's
// delayed intake then raced an already-released fragment. This leg is
// deliberately SELF-CONTAINED - it builds its own fixture and calls the
// production settlement entry only - so the identical test runs on the
// pre-fix tree (red) and the fixed tree (green) without depending on
// any test-file helper whose signature changed between the two.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

// r17FailClosedMOCluster satisfies clusterservice.MOCluster with empty
// enumeration, the minimum disttae.NewTxnWorkSpace resolves.
type r17FailClosedMOCluster struct{}

func (r17FailClosedMOCluster) GetCNService(
	clusterservice.Selector, func(metadata.CNService) bool) {
}

func (r17FailClosedMOCluster) GetTNService(
	clusterservice.Selector, func(metadata.TNService) bool) {
}

func (r17FailClosedMOCluster) GetAllTNServices() []metadata.TNService { return nil }

func (r17FailClosedMOCluster) GetCNServiceWithoutWorkingState(
	clusterservice.Selector, func(metadata.CNService) bool) {
}

func (r17FailClosedMOCluster) ForceRefresh(sync bool)                               {}
func (r17FailClosedMOCluster) Close()                                               {}
func (r17FailClosedMOCluster) DebugUpdateCNLabel(string, map[string][]string) error { return nil }
func (r17FailClosedMOCluster) DebugUpdateCNWorkState(string, int) error             { return nil }
func (r17FailClosedMOCluster) RemoveCN(string)                                      {}
func (r17FailClosedMOCluster) AddCN(metadata.CNService)                             {}
func (r17FailClosedMOCluster) UpdateCN(metadata.CNService)                          {}

// TestIssue25590AdmittedNoFlowSettleStaysLocal proves the round-52
// invariant against the production settlement: a clean admitted run with
// no barrier keeps its local owner. Pre-fix the same leg settled
// transferred=true and released the retained name with no adoption
// proof anywhere.
func TestIssue25590AdmittedNoFlowSettleStaysLocal(t *testing.T) {
	ctx := context.Background()
	witness := "issue25590-r17-failclosed-witness"
	fs, err := fileservice.NewMemoryFS("issue25590-r17", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: witness,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("w")}},
	}))

	proc := testutil.NewProcess(t)
	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	t.Cleanup(closeFn)
	colexec.NewServer("")
	moruntime.ServiceRuntime("").SetGlobalVariables(
		moruntime.ClusterService, r17FailClosedMOCluster{})
	eng := disttae.NewEngineForTest(fs)
	fragmentWs := disttae.NewTxnWorkSpace(eng, proc)
	txnOp.AddWorkspace(fragmentWs)
	fragmentWs.BindTxnOp(txnOp)
	proc.Base.TxnOperator = txnOp
	proc.Ctx = ctx

	// The fragment persisted and retained one witness object.
	fragmentWs.RetainUnrecordedS3Objects([]string{witness},
		"the r17 fail-closed fixture: the fragment's persisted object")
	pending, reserved := fragmentWs.UnsettledCleanupOwnershipForTest()
	require.Equal(t, []string{witness}, pending,
		"pre-settle state: the fragment owns its retained name")
	require.Empty(t, reserved)

	// The run's scope set ADMITS adoption (a registered S3 update
	// producer) but the stream carries NO batch-flow barrier: the exact
	// configuration the disable-stream-reuse knob produces on a clean
	// producer stream, and the exact hole the round-52 finding walked.
	mu := multi_update.NewArgument()
	mu.Action = multi_update.UpdateWriteS3
	run := &Scope{}
	run.RootOp = mu
	runCompile := &Compile{proc: proc}
	runCompile.scopes = []*Scope{run}
	receiver := &messageReceiverOnServer{
		messageCtx:      ctx,
		connectionCtx:   ctx,
		streamLifecycle: nil, // no negotiated flow and no late registration
	}

	runErr := error(nil)
	require.NoError(t, settleRemoteFragmentWorkspaceAfterDrain(receiver, runCompile, &runErr))
	require.NoError(t, runErr, "the clean run ends cleanly - only the ownership class is under test")

	// Fail-closed: nothing may transfer without a drained barrier.
	pending, reserved = fragmentWs.UnsettledCleanupOwnershipForTest()
	require.Empty(t, pending,
		"the ownership left the transaction - settled locally, not stranded")
	require.Empty(t, reserved)
	queued, live, inFlight := eng.InspectUnpublishedPendingForTest()
	require.Equal(t, []string{witness}, queued,
		"an admitted run with NO barrier keeps its local owner: the fragment engine's queue receives the retained name")
	require.Zero(t, live, "the owned-ledger membership retired at the handoff to the queue")
	require.Zero(t, inFlight)

	// The keep terminal's own drain deletes the residue physically, with
	// a balanced ledger at the end - no orphan, no accumulation.
	require.NoError(t, eng.DrainUnpublishedPendingForTest(ctx))
	_, statErr := fs.StatFile(ctx, witness)
	require.Error(t, statErr,
		"after the drain the kept witness is physically deleted by its own owner")
	queued, live, inFlight = eng.InspectUnpublishedPendingForTest()
	require.Empty(t, queued, "the ledger closes exactly at zero")
	require.Zero(t, live)
	require.Zero(t, inFlight)
}
