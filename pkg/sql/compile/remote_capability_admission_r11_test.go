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

// D3'/D3'' message-level legs (issue #25590 design r11): the
// coordinator advertises its OWN build protocol on every run request,
// the receiver stores the advertisement per execution request, and a
// persistence-producing fragment refuses a below-adoption coordinator
// BEFORE pipeline execution with a definite ErrNotSupported error —
// the class whose default frontend disposition is statement-only
// rollback (it sits outside moerr's whole-transaction list). The
// settlement consults the STORED request value: a below-advertisement
// request can never take the transferred classification, fail-closed
// exactly like the run case's refusal.

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

// r11MockMOCluster satisfies clusterservice.MOCluster with empty
// enumeration — just enough for disttae.NewTxnWorkSpace to resolve the
// cluster-service global in a compile-package fixture (the testutil
// mock lives in a package that imports compile and cannot be reused
// from here).
type r11MockMOCluster struct{}

func (r11MockMOCluster) GetCNService(
	clusterservice.Selector, func(metadata.CNService) bool) {
}

func (r11MockMOCluster) GetTNService(
	clusterservice.Selector, func(metadata.TNService) bool) {
}

func (r11MockMOCluster) GetAllTNServices() []metadata.TNService { return nil }

func (r11MockMOCluster) GetCNServiceWithoutWorkingState(
	clusterservice.Selector, func(metadata.CNService) bool) {
}

func (r11MockMOCluster) ForceRefresh(sync bool)                               {}
func (r11MockMOCluster) Close()                                               {}
func (r11MockMOCluster) DebugUpdateCNLabel(string, map[string][]string) error { return nil }
func (r11MockMOCluster) DebugUpdateCNWorkState(string, int) error             { return nil }
func (r11MockMOCluster) RemoveCN(string)                                      {}
func (r11MockMOCluster) AddCN(metadata.CNService)                             {}
func (r11MockMOCluster) UpdateCN(metadata.CNService)                          {}

// The adoption-line thresholds, both directions plus the absent/zero
// fail-closed default (the D3” receiver input contract).
func TestIssue25590CoordinatorAdmissionThresholdsR11(t *testing.T) {
	require.Equal(t, defines.MORPCVersion133, coordinatorIntakeAdoptionVersion,
		"V_adopt is the branch's named adoption constant")
	require.False(t, requestAdoptsIntakeAdmission(0),
		"an absent/zero advertisement (the older-coordinator default) is incapable")
	require.False(t, requestAdoptsIntakeAdmission(defines.MORPCVersion129),
		"an older main build stays below the first adoption build")
	require.False(t, requestAdoptsIntakeAdmission(defines.MORPCVersion130),
		"another older main build stays below the first adoption build")
	require.False(t, requestAdoptsIntakeAdmission(defines.MORPCVersion132),
		"main's shipped latest stays below the first adoption build")
	require.True(t, requestAdoptsIntakeAdmission(coordinatorIntakeAdoptionVersion),
		"the first adoption build adopts")
	require.True(t, requestAdoptsIntakeAdmission(coordinatorIntakeAdoptionVersion+7),
		"every later build adopts")
	require.Greater(t, defines.MORPCLatestVersion, defines.MORPCVersion132,
		"the fresh-build advertisement is above main's shipped latest")
}

// The D3' refusal gate: both directions, the scope boundary, the error
// class, and the designed default disposition.
func TestIssue25590RefuseRemoteWriteFromIncapableCoordinatorR11(t *testing.T) {
	ctx := context.Background()

	write := &Scope{}
	mu := multi_update.NewArgument()
	mu.Action = multi_update.UpdateWriteS3
	write.RootOp = mu

	ordinary := &Scope{}
	ordinary.RootOp = connector.NewArgument()
	ordinary.RootOp.GetOperatorBase().AppendChild(value_scan.NewArgument())

	// A below-advertisement producer run is refused with the designed
	// class and message — the absent/zero default of an old build and
	// the real pre-merge value behave the same.
	err := refuseRemoteWriteFromIncapableCoordinator(ctx, 0, []*Scope{write})
	require.Error(t, err)
	require.ErrorContains(t, err, "coordinator protocol capability")
	require.False(t, moerr.MustRollbackTxn(err),
		"the designed default disposition is statement-only rollback: ErrNotSupported is outside moerr's whole-transaction rollback list")
	require.Error(t, refuseRemoteWriteFromIncapableCoordinator(ctx, defines.MORPCVersion129, []*Scope{write}),
		"an older main build is below the adoption line for writes")
	require.Error(t, refuseRemoteWriteFromIncapableCoordinator(ctx, defines.MORPCVersion130, []*Scope{write}),
		"another older main build is below the adoption line for writes")
	require.Error(t, refuseRemoteWriteFromIncapableCoordinator(ctx, defines.MORPCVersion132, []*Scope{write}),
		"main's shipped latest is below the adoption line for writes")

	// A producer nested in PreScopes stays a producer for the refusal.
	parent := &Scope{}
	parent.PreScopes = append(parent.PreScopes, write)
	require.Error(t, refuseRemoteWriteFromIncapableCoordinator(ctx, 0, []*Scope{parent}),
		"a nested registered producer keeps the fragment a write")

	// At or above the adoption build, a producer run executes.
	require.NoError(t, refuseRemoteWriteFromIncapableCoordinator(ctx, defines.MORPCVersion133, []*Scope{write}))

	// An ordinary query fragment from a below-advertisement coordinator
	// still executes — the adoption line never gates read traffic.
	require.NoError(t, refuseRemoteWriteFromIncapableCoordinator(ctx, 0, []*Scope{ordinary}))

	// Empty null input boundary: no scopes, nothing registered.
	require.NoError(t, refuseRemoteWriteFromIncapableCoordinator(ctx, 0, nil))
}

// The client stamps the coordinator's own runtime protocol version on
// every run request, fail-closed when no runtime answers the service —
// the same global the shipped mo_ctl SetProtocolVersion knob flips.
func TestIssue25590ClientStampsAdvertisementFromCoordinatorRuntime(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	require.NotNil(t, rt)
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)

	sender := &messageSenderOnClient{proc: proc}

	// The fail-closed boundary: a sender with no process, and a sender
	// whose service no runtime answers, stamp the zero default.
	mNil := &pipeline.Message{}
	noProcSender := &messageSenderOnClient{}
	noProcSender.setCoordinatorProtocolAdvertisement(mNil)
	require.Zero(t, mNil.CoordinatorProtocolVersion,
		"a sender without a process advertises the fail-closed zero default")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
	m1 := &pipeline.Message{}
	sender.setCoordinatorProtocolAdvertisement(m1)
	require.Equal(t, defines.MORPCVersion131, m1.CoordinatorProtocolVersion,
		"the old-coordinator pin flows into the advertisement")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion133)
	m2 := &pipeline.Message{}
	sender.setCoordinatorProtocolAdvertisement(m2)
	require.Equal(t, defines.MORPCVersion133, m2.CoordinatorProtocolVersion,
		"a fresh build advertises its own version")

	// The wire carries the additive field: a marshal/unmarshal
	// roundtrip preserves the advertisement, and an unset field
	// decodes as the zero default.
	wire, err := m2.Marshal()
	require.NoError(t, err)
	decoded := &pipeline.Message{}
	require.NoError(t, decoded.Unmarshal(wire))
	require.Equal(t, defines.MORPCVersion133, decoded.GetCoordinatorProtocolVersion())

	wireZero, err := (&pipeline.Message{}).Marshal()
	require.NoError(t, err)
	decodedZero := &pipeline.Message{}
	require.NoError(t, decodedZero.Unmarshal(wireZero))
	require.Zero(t, decodedZero.GetCoordinatorProtocolVersion())
}

// The receiver stores the advertisement per execution request — a
// fresh receiver per pipeline message is the reset — exactly at the
// construction the production receive loop uses.
func TestIssue25590ReceiverStoresPerRequestAdvertisementR11(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	clientSession := mock_morpc.NewMockClientSession(ctrl)
	clientSession.EXPECT().SessionCtx().Return(context.Background()).AnyTimes()

	receiver, err := newMessageReceiverOnServer(
		context.Background(),
		"cn-address",
		&pipeline.Message{
			Cmd:                        pipeline.Method_PipelineProtocolCheck,
			CoordinatorProtocolVersion: defines.MORPCVersion133,
		},
		clientSession,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion133, receiver.coordinatorProtocolVersion,
		"the receiver stores the request's advertisement")

	receiver2, err := newMessageReceiverOnServer(
		context.Background(),
		"cn-address",
		&pipeline.Message{Cmd: pipeline.Method_PipelineProtocolCheck},
		clientSession,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Zero(t, receiver2.coordinatorProtocolVersion,
		"an older coordinator that never sent the field decodes as the fail-closed zero")
}

// The settlement consults the STORED request advertisement: a
// below-adoption producer run can never reach the transferred
// classification — its settle returns before the batch flow is ever
// consulted — while a capable request on the same fixture reaches the
// transfer path's drain, whose context failure is observable in runErr.
// The two legs differ ONLY in the stored advertisement value.
func TestIssue25590SettlementConsultsStoredRequestAdvertisementR11(t *testing.T) {
	ctx := context.Background()
	// NewTxnWorkSpace resolves the cluster-service global on its engine.
	moruntime.ServiceRuntime("").SetGlobalVariables(
		moruntime.ClusterService, r11MockMOCluster{})
	defer moruntime.ServiceRuntime("").SetGlobalVariables(
		moruntime.ClusterService, r11MockMOCluster{})

	write := &Scope{}
	mu := multi_update.NewArgument()
	mu.Action = multi_update.UpdateWriteS3
	write.RootOp = mu

	newFixture := func(t *testing.T) (*Compile, *pipelineBatchFlow, context.Context, context.CancelFunc) {
		proc := testutil.NewProcess(t)
		eng := &disttae.Engine{}
		txnOp, closeFn := client.NewTestTxnOperator(ctx)
		t.Cleanup(closeFn)
		// testutil.NewProcess AND NewTestTxnOperator both REPLACE the
		// default service runtime, so every global registration — the
		// colexec server for Rollback's terminal cleanup and the
		// cluster-service mock for NewTxnWorkSpace — must happen AFTER
		// the last replacement.
		colexec.NewServer("")
		moruntime.ServiceRuntime("").SetGlobalVariables(
			moruntime.ClusterService, r11MockMOCluster{})
		ws := disttae.NewTxnWorkSpace(eng, proc)
		txnOp.AddWorkspace(ws)
		ws.BindTxnOp(txnOp)
		proc.Base.TxnOperator = txnOp
		proc.Ctx = ctx
		runCompile := &Compile{proc: proc}
		runCompile.scopes = []*Scope{write}
		// A flow with one outstanding reserved batch: its drain can
		// only fail through the request contexts, which is exactly the
		// transferred route's observable.
		flow := newPipelineBatchFlow(pipelineBatchCreditCount, pipelineBatchCreditBytes)
		reserveCtx, reserveCancel := context.WithCancel(ctx)
		defer reserveCancel()
		_, err := flow.reserve(reserveCtx, context.Background(), 1)
		require.NoError(t, err)
		messageCtx, cancelMessage := context.WithCancel(ctx)
		cancelMessage() // the request is already cancelled on arrival
		return runCompile, flow, messageCtx, cancelMessage
	}

	// BELOW leg: the stored advertisement of an old coordinator
	// — 132, main's shipped latest — fail-closes the settle BEFORE
	// the transferred route.
	runCompileBelow, flowBelow, messageCtxBelow, _ := newFixture(t)
	receiverBelow := messageReceiverOnServer{
		coordinatorProtocolVersion: defines.MORPCVersion132,
		messageCtx:                 messageCtxBelow,
		connectionCtx:              context.Background(),
		streamLifecycle:            &pipelineStreamLifecycle{batchFlow: flowBelow},
	}
	runErr := error(nil)
	err := settleRemoteFragmentWorkspaceAfterDrain(&receiverBelow, runCompileBelow, &runErr)
	require.NoError(t, err, "the failed-open Finalize(false) on the fresh workspace returns nil")
	require.NoError(t, runErr,
		"the below-advertisement settle returned BEFORE the batch flow was consulted (fail-closed, nothing transferred)")

	// Above leg, same shape, only the advertisement differs: the settle
	// REACHES the transferred route and consults the batch flow, whose
	// cancelled request context surfaces as the runErr error.
	runCompileAbove, flowAbove, messageCtxAbove, _ := newFixture(t)
	receiverAbove := messageReceiverOnServer{
		coordinatorProtocolVersion: defines.MORPCVersion133,
		messageCtx:                 messageCtxAbove,
		connectionCtx:              context.Background(),
		streamLifecycle:            &pipelineStreamLifecycle{batchFlow: flowAbove},
	}
	runErrAbove := error(nil)
	err = settleRemoteFragmentWorkspaceAfterDrain(&receiverAbove, runCompileAbove, &runErrAbove)
	require.NoError(t, err, "the settle still finalizes; the drain error travels by runErr pointer")
	require.Error(t, runErrAbove,
		"the capable leg consulted the batch flow — the only route that can — and its cancelled request context surfaced through runErr")
}

// The run case's D3' wiring: the refusal sits between the scope set
// and every pipeline step — the check a maintenance test can pin by
// calling the same helper the run case calls with the SAME stored
// input. (The run case's own wiring is exercised end to end by the
// public 2-CN legs; this unit pins the helper contract.)
func TestIssue25590RunCaseRefusalInputContractR11(t *testing.T) {
	ctx := context.Background()

	write := &Scope{}
	mu := multi_update.NewArgument()
	mu.Action = multi_update.UpdateWriteS3
	write.RootOp = mu

	// The run case passes receiver.getMessageContext() and the STORED
	// request value; those are the same inputs fixed here.
	require.NoError(t, refuseRemoteWriteFromIncapableCoordinator(ctx, defines.MORPCVersion133, []*Scope{write}))
	err := refuseRemoteWriteFromIncapableCoordinator(ctx, 0, []*Scope{write})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported),
		"the refusal is the designed ErrNotSupported class")
}
