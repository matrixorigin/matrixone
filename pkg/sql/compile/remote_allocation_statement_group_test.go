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
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

type remoteAllocationAccountedMessage struct {
	mp        *mpool.MPool
	buffer    []byte
	destroyed *atomic.Int32
}

func remoteAllocationStatementGroupRegistered(board *message.MessageBoard) bool {
	remoteAllocationStatementGroups.Lock()
	defer remoteAllocationStatementGroups.Unlock()
	_, registered := remoteAllocationStatementGroups.byBoard[board]
	return registered
}

func (m *remoteAllocationAccountedMessage) Serialize() []byte { return nil }

func (m *remoteAllocationAccountedMessage) Deserialize([]byte) message.Message {
	return m
}

func (m *remoteAllocationAccountedMessage) NeedBlock() bool { return true }

func (m *remoteAllocationAccountedMessage) GetMsgTag() int32 { return 1 }

func (m *remoteAllocationAccountedMessage) GetReceiverAddr() message.MessageAddress {
	return message.AddrBroadCastOnCurrentCN()
}

func (m *remoteAllocationAccountedMessage) DebugString() string {
	return "remote allocation-accounted message"
}

func (m *remoteAllocationAccountedMessage) Destroy() {
	if m.buffer != nil {
		m.mp.Free(m.buffer)
		m.buffer = nil
		m.destroyed.Add(1)
	}
}

func TestCollectRemoteFragmentCountsCarriesExecutionAddress(t *testing.T) {
	remoteC := &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Addr: "cn-c:6001"},
	}
	sameB := &Scope{
		Magic:     Remote,
		NodeInfo:  engine.Node{Addr: "cn-b:6001"},
		PreScopes: []*Scope{remoteC},
	}
	firstB := &Scope{
		Magic:     Remote,
		NodeInfo:  engine.Node{Addr: "cn-b:6001"},
		PreScopes: []*Scope{sameB},
	}
	secondB := &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Addr: "cn-b:6001"},
	}
	local := &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Addr: "cn-a:6001"},
	}

	require.Equal(t, map[string]uint32{
		"cn-b:6001": 2,
		"cn-c:6001": 1,
	}, collectRemoteFragmentCounts(
		[]*Scope{firstB, nil, secondB, local},
		"cn-a:6001",
	))
}

func TestRemoteExecutionIDSeparatesRetryMessageBoards(t *testing.T) {
	statementID := newRemoteExecutionID()
	firstAttempt := newRemoteExecutionID()
	secondAttempt := newRemoteExecutionID()
	require.NotEqual(t, firstAttempt, secondAttempt)
	require.Equal(t, statementID, remoteMessageBoardID(statementID, [16]byte{}))
	require.Equal(t, firstAttempt, remoteMessageBoardID(statementID, firstAttempt))
	require.Equal(t, secondAttempt, remoteMessageBoardID(statementID, secondAttempt))
	require.NotEqual(t,
		remoteAllocationStatementGroupKey(firstAttempt, "cn-a:6001"),
		remoteAllocationStatementGroupKey(secondAttempt, "cn-a:6001"),
	)
}

func TestRemoteAllocationTopologyCapabilityIsRequiredForOwners(t *testing.T) {
	owner := &allocationLifecycleOwnerOperator{
		MockOperator: colexec.NewMockOperator(),
	}
	scopes := []*Scope{{RootOp: owner}}
	require.Error(t, validateRemoteAllocationTopologyCapability(scopes, nil))
	require.NoError(t, validateRemoteAllocationTopologyCapability(
		scopes,
		map[string]uint32{"cn-a:6001": 1},
	))
}

func TestRemoteAllocationStatementGroupDefersSharedBoardTerminal(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 4)
	require.NoError(t, err)
	board := message.NewMessageBoard()
	producer := newTestAllocationLifecycleCompile(t, registry, func(
		mpool.AllocationAccountTerminalSnapshot,
	) {
		t.Fatal("remote statement group must own terminal export")
	})
	producer.MessageBoard = board
	attempt, err := producer.beginAllocationAccountAttempt()
	require.NoError(t, err)

	buffer, err := producer.proc.Mp().AllocAccounted(
		64,
		attempt.account,
		mpool.AllocationOwner(1),
		mpool.AllocationSite(1),
	)
	require.NoError(t, err)
	var destroyed atomic.Int32
	message.SendMessage(&remoteAllocationAccountedMessage{
		mp:        producer.proc.Mp(),
		buffer:    buffer,
		destroyed: &destroyed,
	}, board)

	first, err := acquireRemoteAllocationStatementParticipant(board, 2, nil)
	require.NoError(t, err)
	first.stage(attempt, producer.proc.Mp())
	terminal, err := first.finish(nil)
	require.NoError(t, err)
	require.Empty(t, terminal.allocation)
	require.False(t, terminal.complete)
	require.Zero(t, destroyed.Load())
	require.Equal(t, uint64(cap(buffer)), attempt.account.Snapshot().Used)
	require.NotContains(t, board.DebugString(), "closed")
	require.False(t, registry.AdmissionSuspended())

	second, err := acquireRemoteAllocationStatementParticipant(board, 2, nil)
	require.NoError(t, err)
	// The second fragment has no allocation owner. It still participates in
	// the statement boundary and, as the last fragment, drains the producer's
	// queued ownership before completing that producer's account.
	terminal, err = second.finish(nil)
	require.NoError(t, err)
	require.True(t, terminal.complete)
	require.Len(t, terminal.allocation, 1)
	require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.allocation[0].State)
	require.Zero(t, terminal.allocation[0].Used)
	require.Equal(t, uint64(cap(buffer)), terminal.allocation[0].Peak)
	require.Equal(t, uint64(cap(buffer)), terminal.memory.AllocatedBytes)
	require.Equal(t, uint64(cap(buffer)), terminal.memory.FreedBytes)
	require.Zero(t, terminal.memory.LiveBytesAtSeal)
	require.Zero(t, terminal.quality)
	require.Equal(t, int32(1), destroyed.Load())
	require.Contains(t, board.DebugString(), "closed")
	require.False(t, registry.AdmissionSuspended())
	require.Zero(t, registry.LiveAllocationMetadata())
	require.False(t, remoteAllocationStatementGroupRegistered(board))
}

func TestRemoteAllocationStatementGroupRejectsTopologyMismatch(t *testing.T) {
	board := message.NewMessageBoard()
	participant, err := acquireRemoteAllocationStatementParticipant(board, 2, nil)
	require.NoError(t, err)
	_, err = acquireRemoteAllocationStatementParticipant(board, 3, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)

	second, err := acquireRemoteAllocationStatementParticipant(board, 2, nil)
	require.NoError(t, err)
	_, err = participant.finish(nil)
	require.NoError(t, err)
	_, err = second.finish(nil)
	require.NoError(t, err)
	require.False(t, remoteAllocationStatementGroupRegistered(board))
}

func TestRemoteAllocationStatementGroupExpiresMissingFragment(t *testing.T) {
	previousTimeout := remoteAllocationStatementRegistrationTimeout
	remoteAllocationStatementRegistrationTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		remoteAllocationStatementRegistrationTimeout = previousTimeout
	})

	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	board := message.NewMessageBoard()
	producer := newTestAllocationLifecycleCompile(t, registry, func(
		mpool.AllocationAccountTerminalSnapshot,
	) {
		t.Fatal("expired remote statement group must own terminal export")
	})
	producer.MessageBoard = board
	attempt, err := producer.beginAllocationAccountAttempt()
	require.NoError(t, err)
	buffer, err := producer.proc.Mp().AllocAccounted(
		64,
		attempt.account,
		mpool.AllocationOwner(1),
		mpool.AllocationSite(1),
	)
	require.NoError(t, err)
	var destroyed atomic.Int32
	message.SendMessage(&remoteAllocationAccountedMessage{
		mp:        producer.proc.Mp(),
		buffer:    buffer,
		destroyed: &destroyed,
	}, board)

	participant, err := acquireRemoteAllocationStatementParticipant(board, 2, nil)
	require.NoError(t, err)
	participant.stage(attempt, producer.proc.Mp())
	terminal, err := participant.finish(nil)
	require.NoError(t, err)
	require.Empty(t, terminal.allocation)
	require.False(t, terminal.complete)
	require.Eventually(t, func() bool {
		return destroyed.Load() == 1 &&
			registry.LiveAllocationMetadata() == 0 &&
			!remoteAllocationStatementGroupRegistered(board)
	}, time.Second, time.Millisecond)
	require.Contains(t, board.DebugString(), "closed")
	require.False(t, registry.AdmissionSuspended())
}

func TestRemoteAllocationStatementRegistrationTimerStartsBeforeFinish(t *testing.T) {
	previousTimeout := remoteAllocationStatementRegistrationTimeout
	remoteAllocationStatementRegistrationTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		remoteAllocationStatementRegistrationTimeout = previousTimeout
	})

	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	board := message.NewMessageBoard()
	producer := newTestAllocationLifecycleCompile(t, registry, func(
		mpool.AllocationAccountTerminalSnapshot,
	) {
		t.Fatal("expired remote statement group must own terminal export")
	})
	producer.MessageBoard = board
	attempt, err := producer.beginAllocationAccountAttempt()
	require.NoError(t, err)
	buffer, err := producer.proc.Mp().AllocAccounted(
		64,
		attempt.account,
		mpool.AllocationOwner(1),
		mpool.AllocationSite(1),
	)
	require.NoError(t, err)
	var destroyed atomic.Int32
	message.SendMessage(&remoteAllocationAccountedMessage{
		mp:        producer.proc.Mp(),
		buffer:    buffer,
		destroyed: &destroyed,
	}, board)

	canceled := make(chan error, 1)
	participant, err := acquireRemoteAllocationStatementParticipant(
		board,
		2,
		func(cause error) { canceled <- cause },
	)
	require.NoError(t, err)

	select {
	case cause := <-canceled:
		require.Error(t, cause)
	case <-time.After(time.Second):
		t.Fatal("registration timeout did not cancel the active fragment")
	}
	require.Eventually(t, func() bool {
		return strings.Contains(board.DebugString(), "closed")
	}, time.Second, time.Millisecond)
	// Closing wakes the active fragment but cannot destroy ownership that the
	// fragment may still be consuming. The last finish performs the drain.
	require.Zero(t, destroyed.Load())
	require.Equal(t, uint64(cap(buffer)), attempt.account.Snapshot().Used)

	participant.stage(attempt, producer.proc.Mp())
	terminal, err := participant.finish(errors.New("active fragment observed cancellation"))
	require.Error(t, err)
	require.True(t, terminal.complete)
	require.Len(t, terminal.allocation, 1)
	require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.allocation[0].State)
	require.Zero(t, terminal.allocation[0].Used)
	require.Equal(t, int32(1), destroyed.Load())
	require.Zero(t, terminal.memory.LiveBytesAtSeal)
	require.Zero(t, registry.LiveAllocationMetadata())
	require.False(t, remoteAllocationStatementGroupRegistered(board))
}

func TestRemoteAllocationStatementGroupFailureAbortsMissingFragment(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	board := message.NewMessageBoard()
	producer := newTestAllocationLifecycleCompile(t, registry, func(
		mpool.AllocationAccountTerminalSnapshot,
	) {
		t.Fatal("remote statement group must own terminal export")
	})
	producer.MessageBoard = board
	attempt, err := producer.beginAllocationAccountAttempt()
	require.NoError(t, err)
	participant, err := acquireRemoteAllocationStatementParticipant(board, 2, nil)
	require.NoError(t, err)
	participant.stage(attempt, producer.proc.Mp())

	failure := errors.New("remote fragment failed")
	terminal, err := participant.finish(failure)
	require.ErrorIs(t, err, failure)
	require.True(t, terminal.complete)
	require.Len(t, terminal.allocation, 1)
	require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.allocation[0].State)
	require.Contains(t, board.DebugString(), "closed")
	require.Zero(t, registry.LiveAllocationMetadata())
	require.False(t, remoteAllocationStatementGroupRegistered(board))
}

func TestRemoteAllocationStatementGroupFailureCancelsActiveSibling(t *testing.T) {
	board := message.NewMessageBoard()
	canceled := make(chan error, 2)
	first, err := acquireRemoteAllocationStatementParticipant(
		board,
		3,
		func(cause error) { canceled <- cause },
	)
	require.NoError(t, err)
	second, err := acquireRemoteAllocationStatementParticipant(
		board,
		3,
		func(cause error) { canceled <- cause },
	)
	require.NoError(t, err)

	failure := errors.New("first remote fragment failed")
	terminal, err := first.finish(failure)
	require.NoError(t, err)
	require.Empty(t, terminal.allocation)
	require.False(t, terminal.complete)
	// The failing participant is already quiescent; only its active sibling
	// needs cancellation.
	require.ErrorIs(t, <-canceled, failure)
	select {
	case unexpected := <-canceled:
		t.Fatalf("finished participant was canceled: %v", unexpected)
	default:
	}
	require.Contains(t, board.DebugString(), "closed")

	terminal, err = second.finish(errors.New("active sibling canceled"))
	require.Error(t, err)
	require.Empty(t, terminal.allocation)
	require.False(t, remoteAllocationStatementGroupRegistered(board))
}

func TestRemoteAllocationStatementGroupExpirationWaitsForActiveFragment(t *testing.T) {
	previousTimeout := remoteAllocationStatementRegistrationTimeout
	remoteAllocationStatementRegistrationTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		remoteAllocationStatementRegistrationTimeout = previousTimeout
	})

	registry, err := mpool.NewAllocationAccountRegistry(2, 2)
	require.NoError(t, err)
	board := message.NewMessageBoard()
	newAttempt := func() (*Compile, *statementAllocationAttempt, []byte) {
		c := newTestAllocationLifecycleCompile(t, registry, func(
			mpool.AllocationAccountTerminalSnapshot,
		) {
			t.Fatal("remote statement group must own terminal export")
		})
		c.MessageBoard = board
		attempt, openErr := c.beginAllocationAccountAttempt()
		require.NoError(t, openErr)
		buffer, allocErr := c.proc.Mp().AllocAccounted(
			64,
			attempt.account,
			mpool.AllocationOwner(1),
			mpool.AllocationSite(1),
		)
		require.NoError(t, allocErr)
		return c, attempt, buffer
	}

	firstCompile, firstAttempt, firstBuffer := newAttempt()
	secondCompile, secondAttempt, secondBuffer := newAttempt()
	canceled := make(chan error, 2)
	first, err := acquireRemoteAllocationStatementParticipant(
		board, 3, func(cause error) { canceled <- cause },
	)
	require.NoError(t, err)
	second, err := acquireRemoteAllocationStatementParticipant(
		board, 3, func(cause error) { canceled <- cause },
	)
	require.NoError(t, err)
	firstCompile.proc.Mp().Free(firstBuffer)
	first.stage(firstAttempt, firstCompile.proc.Mp())
	terminal, err := first.finish(nil)
	require.NoError(t, err)
	require.Empty(t, terminal.allocation)

	require.Eventually(t, func() bool {
		return strings.Contains(board.DebugString(), "closed")
	}, time.Second, time.Millisecond)
	require.Error(t, <-canceled)
	select {
	case unexpected := <-canceled:
		t.Fatalf("finished participant was canceled: %v", unexpected)
	default:
	}
	// Expiration closes the shared transport but cannot terminally inspect a
	// registered fragment that is still executing.
	_, live := registry.Resolve(secondAttempt.account.Handle())
	require.True(t, live)
	require.Equal(t, uint64(cap(secondBuffer)), secondAttempt.account.Snapshot().Used)
	require.False(t, secondAttempt.account.Snapshot().Sealed)

	secondCompile.proc.Mp().Free(secondBuffer)
	second.stage(secondAttempt, secondCompile.proc.Mp())
	terminal, err = second.finish(errors.New("active fragment observed cancellation"))
	require.Error(t, err)
	require.Len(t, terminal.allocation, 2)
	for _, snapshot := range terminal.allocation {
		require.Equal(t, mpool.AllocationAccountTerminalValid, snapshot.State)
		require.Zero(t, snapshot.Used)
	}
	require.Zero(t, terminal.memory.LiveBytesAtSeal)
	require.Zero(t, terminal.quality)
	require.Zero(t, registry.LiveAllocationMetadata())
	require.False(t, remoteAllocationStatementGroupRegistered(board))
}
