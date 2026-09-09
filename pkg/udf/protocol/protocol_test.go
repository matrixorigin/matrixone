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

package protocol

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testTuple() FencingTuple {
	return FencingTuple{
		AccountID: 1, StatementID: "statement", GroupID: "group",
		GroupEpoch: 2, InvocationID: "invocation", LeaseEpoch: 3,
	}
}

func TestControlRoundTripIsBoundedAndVersioned(t *testing.T) {
	wire, err := MarshalControl(Control{Kind: "OpenInvocation", Tuple: testTuple(), Payload: []byte(`{"mode":"VECTOR"}`)})
	require.NoError(t, err)
	got, err := UnmarshalControl(wire)
	require.NoError(t, err)
	require.Equal(t, Version, got.Version)
	require.Equal(t, "OpenInvocation", got.Kind)
	require.JSONEq(t, `{"mode":"VECTOR"}`, string(got.Payload))

	_, err = UnmarshalControl(bytes.Repeat([]byte{'x'}, MaxControlBytes+1))
	require.ErrorIs(t, err, ErrProtocol)
	_, err = UnmarshalControl(append(wire, []byte(" trailing")...))
	require.ErrorIs(t, err, ErrProtocol)
	_, err = MarshalControl(Control{Kind: "", Tuple: testTuple()})
	require.ErrorIs(t, err, ErrProtocol)
}

func TestSequenceKeepsHalfCloseIndependentFromResults(t *testing.T) {
	var sequence Sequence
	require.NoError(t, sequence.AcceptInput(1))
	require.NoError(t, sequence.AcceptInput(2))
	require.NoError(t, sequence.EndInput(2))
	require.NoError(t, sequence.AcceptResult(1))
	require.False(t, sequence.ReadyToFinish())
	require.NoError(t, sequence.AcknowledgeResults(1))
	require.NoError(t, sequence.AcceptResult(2))
	require.NoError(t, sequence.AcknowledgeResults(2))
	require.True(t, sequence.ReadyToFinish())

	require.ErrorIs(t, sequence.AcceptInput(3), ErrSequence)
	require.ErrorIs(t, sequence.AcknowledgeResults(3), ErrSequence)
	require.NoError(t, sequence.EndInput(2))
}

func TestOutputSnapshotFreezesBeforeWorkerCanRewrite(t *testing.T) {
	worker := []byte("trusted-before-barrier")
	snapshot, err := FreezeOutput(worker, 1024)
	require.NoError(t, err)
	worker[0] = 'X'

	require.NoError(t, snapshot.Validate(len("trusted-before-barrier"), snapshot.Digest()))
	require.Equal(t, "trusted-before-barrier", string(snapshot.Bytes()))

	empty, err := FreezeOutput(nil, 1024)
	require.NoError(t, err)
	require.NoError(t, empty.Validate(0, empty.Digest()))
}

func TestExecutionGroupReleasesOnlyAfterCloseAndAllMembersTerminal(t *testing.T) {
	var releases atomic.Int32
	group, err := NewExecutionGroup("g", 1, 2, func() error {
		releases.Add(1)
		return nil
	})
	require.NoError(t, err)
	a, err := group.BeginOpen("a")
	require.NoError(t, err)
	require.NoError(t, a.Commit())
	b, err := group.BeginOpen("b")
	require.NoError(t, err)
	require.NoError(t, b.Commit())

	require.NoError(t, group.MemberTerminal("a"))
	require.NoError(t, group.MemberTerminal("a"))
	_, err = group.BeginOpen("a")
	require.ErrorIs(t, err, ErrDuplicate)
	require.NoError(t, group.Close(ReasonInputEOF))
	require.Equal(t, int32(0), releases.Load())
	require.NoError(t, group.MemberTerminal("b"))
	require.Equal(t, int32(1), releases.Load())
	require.Equal(t, GroupReleased, group.State())

	_, err = group.BeginOpen("late")
	require.ErrorIs(t, err, ErrGroupClosed)
	require.NoError(t, group.Close(ReasonCancel))
	require.Equal(t, int32(1), releases.Load())
}

func TestExecutionGroupZeroMemberAndAbortedOpenRelease(t *testing.T) {
	var releases atomic.Int32
	group, err := NewExecutionGroup("g", 1, 1, func() error {
		releases.Add(1)
		return nil
	})
	require.NoError(t, err)
	open, err := group.BeginOpen("will-abort")
	require.NoError(t, err)
	require.NoError(t, group.Close(ReasonPartialOpenError))
	require.Equal(t, int32(0), releases.Load())
	require.NoError(t, open.Abort())
	require.Equal(t, int32(1), releases.Load())

	group, err = NewExecutionGroup("empty", 1, 1, func() error {
		releases.Add(1)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, group.Close(ReasonEmptyInput))
	require.Equal(t, int32(2), releases.Load())

	group, err = NewExecutionGroup("late", 1, 1, func() error {
		releases.Add(1)
		return nil
	})
	require.NoError(t, err)
	open, err = group.BeginOpen("late-member")
	require.NoError(t, err)
	require.NoError(t, group.Close(ReasonCancel))
	require.ErrorIs(t, open.Commit(), ErrGroupClosed)
	require.ErrorIs(t, open.Commit(), ErrGroupClosed)
}

func TestTerminalLedgerRetainsTombstonesUntilExpiry(t *testing.T) {
	ledger, err := NewTerminalLedger(2, 200)
	require.NoError(t, err)
	now := time.Unix(100, 0)
	credit, err := ledger.Reserve("g1", 1, 100)
	require.NoError(t, err)
	require.NoError(t, credit.Add("i1", 100, now.Add(time.Hour)))
	require.NoError(t, ledger.Complete("i1"))
	credit.ReleaseUnused()

	second, err := ledger.Reserve("g2", 1, 100)
	require.NoError(t, err)
	require.NoError(t, second.Add("i2", 100, now.Add(time.Hour)))
	second.ReleaseUnused()
	_, err = ledger.Reserve("g3", 1, 1)
	require.ErrorIs(t, err, ErrLedgerFull)

	require.Equal(t, 2, func() int { n, _ := ledger.Counts(); return n }())
	require.Equal(t, 0, ledger.Expire(now.Add(time.Minute)))
	require.Equal(t, 2, func() int { n, _ := ledger.Counts(); return n }())
	require.Equal(t, 2, ledger.Expire(now.Add(2*time.Hour)))
	third, err := ledger.Reserve("g3", 1, 1)
	require.NoError(t, err)
	require.NoError(t, third.Add("i3", 1, now.Add(3*time.Hour)))

	require.ErrorIs(t, ledger.Complete("missing"), ErrUnknownIdentity)
	require.NoError(t, ledger.Complete("i3"))
	require.NoError(t, ledger.Complete("i3"))
}

func TestExecutionGroupReleaseErrorCanBeRetried(t *testing.T) {
	var attempts atomic.Int32
	group, err := NewExecutionGroup("g", 1, 1, func() error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary release failure")
		}
		return nil
	})
	require.NoError(t, err)
	require.Error(t, group.Close(ReasonCancel))
	require.Equal(t, GroupDraining, group.State())
	// A second close is the scheduler's retry-safe release trigger.
	require.NoError(t, group.Close(ReasonCancel))
	require.Equal(t, GroupReleased, group.State())
	require.Equal(t, int32(2), attempts.Load())
}
