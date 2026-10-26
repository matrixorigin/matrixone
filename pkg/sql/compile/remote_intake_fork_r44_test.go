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

// Round-44 review follow-up (issue #25590, PR #30988): the intake
// refusal of a received remote-fragment batch needs the fragment's own
// failure-side capability before it may book nothing. These legs pin
// the fork contracts at the compile plane: the capability predicate and
// the confirm-requirement gate, the intake's selection between the
// capable-fragment adoption (books nothing on refusal - the fragment
// retains) and the non-retaining adoption (retains the received names
// fail-open on the recording workspace), the receive loops' carriage of
// the confirmed stream advertisement into the intake, and the protocol
// confirmation's return of the receiver's own advertised version.

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/stretchr/testify/require"
)

// The round-44 capability line, the receiver direction of the D3”
// gate: zero fails closed toward recording-side retention, the first
// adoption build sits exactly on the line, and every later fragment
// retains refused runs itself.
func TestIssue25590FragmentRetentionThresholdsR44(t *testing.T) {
	require.False(t, fragmentRetainsRefusedIntakeObjects(0),
		"an unconfirmed stream reads fails closed toward recording-side retention")
	require.False(t, fragmentRetainsRefusedIntakeObjects(defines.MORPCVersion132),
		"main's shipped latest does not retain refused runs - the review's legacy fragment")
	require.True(t, fragmentRetainsRefusedIntakeObjects(coordinatorIntakeAdoptionVersion),
		"the first admission build owns its refused runs through writer retention and the failure terminal")
	require.True(t, fragmentRetainsRefusedIntakeObjects(coordinatorIntakeAdoptionVersion+7),
		"every later build retains refused runs itself")
}

// remoteRun must confirm the receiver's protocol on the stream that
// will carry the pipeline for EVERY scope whose own tree registers a
// remote-persistence producer, independently of any expression floor -
// the intake fork needs the confirmed advertisement when the recording
// engine's bound refuses a batch. Plain query fragments with no floor
// keep the pre-round-44 behavior.
func TestIssue25590RemoteRunConfirmRequirementR44(t *testing.T) {
	plain := &Scope{}
	plain.RootOp = connector.NewArgument()
	plain.RootOp.GetOperatorBase().AppendChild(value_scan.NewArgument())

	muS3 := multi_update.NewArgument()
	muS3.Action = multi_update.UpdateWriteS3
	muS3Scope := &Scope{}
	muS3Scope.RootOp = muS3

	insS3 := insert.NewArgument()
	insS3.ToWriteS3 = true
	insS3Scope := &Scope{}
	insS3Scope.RootOp = insS3

	delRemote := deletion.NewArgument()
	delRemote.RemoteDelete = true
	delScope := &Scope{}
	delScope.RootOp = delRemote

	insExternal := insert.NewArgument()
	insExternal.ToWriteS3 = true
	insExternal.ToExternal = true
	insExternalScope := &Scope{}
	insExternalScope.RootOp = insExternal

	muTable := &Scope{}
	muTable.RootOp = multi_update.NewArgument() // UpdateWriteTable: no metadata edge

	nested := &Scope{}
	nested.RootOp = connector.NewArgument()
	nested.PreScopes = append(nested.PreScopes, muS3Scope)

	require.False(t, remoteRunRequiresFragmentProtocolConfirm(0, plain),
		"a plain query scope with no floor keeps the pre-existing no-confirm behavior")
	require.True(t, remoteRunRequiresFragmentProtocolConfirm(defines.MORPCVersion132, plain),
		"an expression floor already surges the existing confirm")
	require.True(t, remoteRunRequiresFragmentProtocolConfirm(0, muS3Scope))
	require.True(t, remoteRunRequiresFragmentProtocolConfirm(0, insS3Scope))
	require.True(t, remoteRunRequiresFragmentProtocolConfirm(0, delScope))
	require.True(t, remoteRunRequiresFragmentProtocolConfirm(0, nested),
		"the requirement follows the registered producer through nested PreScopes")
	require.False(t, remoteRunRequiresFragmentProtocolConfirm(0, insExternalScope),
		"an external-stage insert stream carries no persisted cleanup obligation")
	require.False(t, remoteRunRequiresFragmentProtocolConfirm(0, muTable),
		"a workspace multi-update writes no S3 object metadata")
}

// The intake fork itself: below the recording bound both directions
// adopt identically (the review's capacity-available control survives
// at both versions), while at a saturated bound the capable fragment's
// intake refusal books nothing and the non-retaining fragment's
// refusal retains the received names on the recording workspace.
func TestIssue25590IntakeForkSelectsFragmentCapabilityR44(t *testing.T) {
	t.Run("capable below the bound adopts", func(t *testing.T) {
		s, ws, restore := r29IntakeFixture(t)
		defer restore()
		r29PrepareBeacon(t, ws, "filler-capable-below")
		relay, _ := r29RelayBatch(t, s.Proc.Mp(), 1, false)
		defer relay.Clean(s.Proc.Mp())
		mu := multi_update.NewArgument()
		mu.Action = multi_update.UpdateWriteS3
		s.RootOp = mu
		require.NoError(t, AdoptRemoteRunIntakeMetadata(s, relay, coordinatorIntakeAdoptionVersion))
		require.Empty(t, ws.PendingObjectCleanupNamesForTest(),
			"an adopted capable stream books reservations, not pending cleanup")
		require.Error(t, r29ProbeAdoption(t, ws, "probe-capable-below"),
			"the adoption consumed the bound's last slot")
	})

	t.Run("non-retaining below the bound adopts identically", func(t *testing.T) {
		s, ws, restore := r29IntakeFixture(t)
		defer restore()
		r29PrepareBeacon(t, ws, "filler-legacy-below")
		relay, _ := r29RelayBatch(t, s.Proc.Mp(), 1, false)
		defer relay.Clean(s.Proc.Mp())
		mu := multi_update.NewArgument()
		mu.Action = multi_update.UpdateWriteS3
		s.RootOp = mu
		require.NoError(t, AdoptRemoteRunIntakeMetadata(s, relay, defines.MORPCVersion132),
			"a legacy fragment with capacity available runs exactly like leg P3' proved - the mixed matrix's intact cell")
		require.Error(t, r29ProbeAdoption(t, ws, "probe-legacy-below"),
			"the legacy adoption also consumes the bound's last slot")
	})

	t.Run("capable at the bound books nothing on refusal", func(t *testing.T) {
		s, ws, restore := r29IntakeFixture(t)
		defer restore()
		r29PrepareBeacon(t, ws, "filler-capable-sat-a")
		require.NoError(t, ws.AdoptRemoteTransferredObjectNames([]string{"filler-capable-sat-b"}),
			"the second filler saturates the pinned bound")
		relay, _ := r29RelayBatch(t, s.Proc.Mp(), 1, false)
		defer relay.Clean(s.Proc.Mp())
		mu := multi_update.NewArgument()
		mu.Action = multi_update.UpdateWriteS3
		s.RootOp = mu
		err := AdoptRemoteRunIntakeMetadata(s, relay, coordinatorIntakeAdoptionVersion)
		require.ErrorContains(t, err, "workspace admission paused",
			"the saturated recording engine refuses the intake - the bound itself is unchanged")
		require.Empty(t, ws.PendingObjectCleanupNamesForTest(),
			"a retaining fragment keeps the failure-side ownership: the recording side books nothing")
		require.Error(t, r29ProbeAdoption(t, ws, "probe-capable-sat"),
			"the refused capable intake books nothing: the bound stays saturated exactly as before")
	})

	t.Run("non-retaining at the bound retains the received names", func(t *testing.T) {
		s, ws, restore := r29IntakeFixture(t)
		defer restore()
		r29PrepareBeacon(t, ws, "filler-legacy-sat-a")
		require.NoError(t, ws.AdoptRemoteTransferredObjectNames([]string{"filler-legacy-sat-b"}),
			"the second filler saturates the pinned bound")
		relay, relayNames := r29RelayBatch(t, s.Proc.Mp(), 1, false)
		defer relay.Clean(s.Proc.Mp())
		mu := multi_update.NewArgument()
		mu.Action = multi_update.UpdateWriteS3
		s.RootOp = mu
		err := AdoptRemoteRunIntakeMetadata(s, relay, defines.MORPCVersion132)
		require.ErrorContains(t, err, "workspace admission paused",
			"the refusal still fails the run - the admission bound is not weakened for a legacy fragment")
		require.Equal(t, relayNames, ws.PendingObjectCleanupNamesForTest(),
			"the non-retaining fragment's refused batch is retained on the recording workspace - the received obligation is never discarded")
	})

	t.Run("ordinary streams are version-agnostic", func(t *testing.T) {
		s, _, restore := r29IntakeFixture(t)
		defer restore()
		relay, _ := r29RelayBatch(t, s.Proc.Mp(), 1, true)
		defer relay.Clean(s.Proc.Mp())
		s.RootOp = multi_update.NewArgument() // UpdateWriteTable: ordinary rows
		require.NoError(t, AdoptRemoteRunIntakeMetadata(s, relay, defines.MORPCVersion132),
			"a legacy fragment's ordinary rows reach no recognizer under either fork")
	})
}

// The production receive loops carry their stream's confirmed receiver
// advertisement into the intake at the refusal boundary: a 132 stream's
// refused relay batch leaves the retained names on the recording
// workspace, a 132 stream's refused batch books nothing.
func TestIssue25590ReceiveLoopCarriesFragmentAdvertisementR44(t *testing.T) {
	drive := func(protocol int64, wantPending bool) {
		s, ws, restore := r29IntakeFixture(t)
		defer restore()
		r29PrepareBeacon(t, ws, "filler-loop-sat-a")
		require.NoError(t, ws.AdoptRemoteTransferredObjectNames([]string{"filler-loop-sat-b"}),
			"the second filler saturates the pinned bound")
		relay, relayNames := r29RelayBatch(t, s.Proc.Mp(), 1, false)
		mu := multi_update.NewArgument()
		mu.Action = multi_update.UpdateWriteS3
		s.RootOp = mu
		sender, acks := r29StubSender(t, s.Proc, []morpc.Message{
			r29WireBatch(t, relay), r29EndMessage(),
		})
		sender.confirmedFragmentProtocol = protocol
		err := receiveMessageFromCnServerIfOnlyRun(s, sender)
		require.ErrorContains(t, err, "workspace admission paused",
			"the saturated intake fails the loop before the ACK under either advertisement")
		require.Zero(t, acks.count(),
			"no ACK is sent for a batch the intake refused")
		if wantPending {
			require.Equal(t, relayNames, ws.PendingObjectCleanupNamesForTest(),
				"a below-advertisement stream's refused batch is retained by the loop's own intake")
		} else {
			require.Empty(t, ws.PendingObjectCleanupNamesForTest(),
				"a first-admission stream's refused batch books nothing on the recording side")
		}
	}
	t.Run("confirmed below-adoption stream retains", func(t *testing.T) {
		drive(defines.MORPCVersion132, true)
	})
	t.Run("confirmed first-admission stream books nothing", func(t *testing.T) {
		drive(coordinatorIntakeAdoptionVersion, false)
	})
}

// The protocol confirmation returns the RECEIVER's advertised version —
// the wire fact the confirm gate exists to surface (round 44) - while
// keeping the exact pre-existing refusal semantics for floor misses.
func TestIssue25590ConfirmReturnsFragmentAdvertisementR44(t *testing.T) {
	check := func(minimum, replyVersion int64) (int64, error) {
		stream := &fakeStreamSender{}
		receiveCh := make(chan morpc.Message, 1)
		receiveCh <- &pipeline.Message{
			Id: 0, Cmd: pipeline.Method_PipelineProtocolCheck,
			Sid: pipeline.Status_Last, ProtocolVersion: replyVersion,
		}
		sender := &messageSenderOnClient{
			ctx: context.Background(), streamSender: stream, receiveCh: receiveCh,
		}
		version, err := sender.confirmProtocolOnStream(minimum)
		require.Equal(t, 1, stream.sentCnt,
			"the confirmation runs exactly once on the stream")
		request := stream.sent[0].(*pipeline.Message)
		require.Equal(t, pipeline.Method_PipelineProtocolCheck, request.GetCmd())
		require.Equal(t, minimum, request.GetProtocolVersion(),
			"the floor rides the existing request field")
		return version, err
	}
	version, err := check(0, defines.MORPCVersion132)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion132, version,
		"the receiver's own advertisement returns to the caller - the intake fork's input")

	version, err = check(defines.MORPCVersion132, coordinatorIntakeAdoptionVersion)
	require.NoError(t, err)
	require.Equal(t, coordinatorIntakeAdoptionVersion, version)

	_, err = check(defines.MORPCVersion132, defines.MORPCVersion100)
	require.Error(t, err,
		"the floor refusal keeps its pre-round-44 behavior")

	stream := &fakeStreamSender{}
	closed := make(chan morpc.Message)
	close(closed)
	sender := &messageSenderOnClient{
		ctx: context.Background(), streamSender: stream, receiveCh: closed,
	}
	_, err = sender.confirmProtocolOnStream(0)
	require.Error(t, err,
		"a closed stream still fails the confirmation")
}
