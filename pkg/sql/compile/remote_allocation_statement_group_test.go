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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
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

func acquireRemoteAllocationStatementTestParticipant(
	t *testing.T,
	board *message.MessageBoard,
	expected uint32,
	cancel func(error),
) (*remoteAllocationStatementParticipant, error) {
	t.Helper()
	key := fmt.Sprintf("test@%p", board)
	t.Cleanup(func() { clearRemoteAllocationStatementTestTombstone(key) })
	p, _, err := acquireRemoteAllocationStatementParticipant(key, board, expected, cancel)
	return p, err
}

func clearRemoteAllocationStatementTestTombstone(key string) {
	remoteAllocationStatementGroups.Lock()
	if tombstone := remoteAllocationStatementGroups.tombstones[key]; tombstone != nil {
		tombstone.timer.Stop()
		delete(remoteAllocationStatementGroups.tombstones, key)
	}
	remoteAllocationStatementGroups.Unlock()
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

func TestCollectRemoteFragmentCountsExcludesLazyOwnedCandidates(t *testing.T) {
	ordinary := &Scope{Magic: Remote, NodeInfo: engine.Node{Addr: "cn-b:6001"}}
	post := &Scope{PreScopes: []*Scope{{Magic: Remote, NodeInfo: engine.Node{Addr: "cn-b:6001"}}}}
	pre := &Scope{PreScopes: []*Scope{{Magic: Remote, NodeInfo: engine.Node{Addr: "cn-c:6001"}}}}
	lazy := &Scope{LazyPreScopes: true, PreScopes: []*Scope{post, pre}}

	counts := collectRemoteFragmentCounts([]*Scope{ordinary, lazy}, "cn-a:6001")
	require.Equal(t, map[string]uint32{"cn-b:6001": 1}, counts)

	board := message.NewMessageBoard()
	executionID := newRemoteExecutionID()
	key := remoteAllocationStatementGroupKey(executionID, "cn-b:6001")
	participant, _, err := acquireRemoteAllocationStatementParticipant(key, board, counts["cn-b:6001"], nil)
	require.NoError(t, err)
	terminal, err := participant.finish(nil)
	require.NoError(t, err)
	require.True(t, terminal.complete)
	require.False(t, remoteAllocationStatementGroupRegistered(board))
	remoteAllocationStatementGroups.Lock()
	require.Nil(t, remoteAllocationStatementGroups.tombstones[key])
	remoteAllocationStatementGroups.Unlock()

	assignLazyRemoteGeneration(post, "cn-a:6001")
	postRemote := post.PreScopes[0]
	require.Equal(t, map[string]uint32{"cn-b:6001": 1}, postRemote.lazyRemoteFragmentCounts)
	assignLazyRemoteGeneration(pre, "cn-a:6001")
	preRemote := pre.PreScopes[0]
	require.Equal(t, map[string]uint32{"cn-c:6001": 1}, preRemote.lazyRemoteFragmentCounts)
	require.NotEqual(t, postRemote.lazyRemoteExecutionID, preRemote.lazyRemoteExecutionID)
}

func TestLazyRemoteCandidatesUseIndependentExactGenerations(t *testing.T) {
	postRemote := &Scope{Magic: Remote, NodeInfo: engine.Node{Addr: "cn-b:6001"}}
	post := &Scope{PreScopes: []*Scope{postRemote}}
	preRemoteB := &Scope{Magic: Remote, NodeInfo: engine.Node{Addr: "cn-b:6001"}}
	preRemoteC := &Scope{Magic: Remote, NodeInfo: engine.Node{Addr: "cn-c:6001"}}
	pre := &Scope{PreScopes: []*Scope{preRemoteB, preRemoteC}}

	assignLazyRemoteGeneration(post, "cn-a:6001")
	postID := postRemote.lazyRemoteExecutionID
	require.NotEqual(t, uuid.Nil, postID)
	require.Equal(t, map[string]uint32{"cn-b:6001": 1}, postRemote.lazyRemoteFragmentCounts)
	counts, executionID := remoteExecutionTopology(&Compile{}, postRemote)
	require.Equal(t, postRemote.lazyRemoteFragmentCounts, counts)
	require.Equal(t, postID, executionID)
	require.Equal(t, uuid.Nil, preRemoteB.lazyRemoteExecutionID, "deferred candidates must not own a generation")
	ordinaryID := newRemoteExecutionID()
	ordinaryCounts := map[string]uint32{"cn-z:6001": 2}
	counts, executionID = remoteExecutionTopology(&Compile{
		remoteFragmentCounts: ordinaryCounts,
		remoteExecutionID:    ordinaryID,
	}, &Scope{})
	require.Equal(t, ordinaryCounts, counts)
	require.Equal(t, ordinaryID, executionID)

	postBoard := message.NewMessageBoard()
	postParticipant, _, err := acquireRemoteAllocationStatementParticipant(
		remoteAllocationStatementGroupKey(postID, "cn-b:6001"), postBoard, 1, nil)
	require.NoError(t, err)
	terminal, err := postParticipant.finish(nil)
	require.NoError(t, err)
	require.True(t, terminal.complete)
	require.False(t, remoteAllocationStatementGroupRegistered(postBoard))

	assignLazyRemoteGeneration(pre, "cn-a:6001")
	preID := preRemoteB.lazyRemoteExecutionID
	require.NotEqual(t, postID, preID)
	require.Equal(t, preID, preRemoteC.lazyRemoteExecutionID)
	require.Equal(t, map[string]uint32{"cn-b:6001": 1, "cn-c:6001": 1}, preRemoteB.lazyRemoteFragmentCounts)

	for _, tc := range []struct {
		address string
		cancel  error
	}{
		{address: "cn-b:6001"},
		{address: "cn-c:6001", cancel: context.Canceled},
	} {
		board := message.NewMessageBoard()
		key := remoteAllocationStatementGroupKey(preID, tc.address)
		participant, _, acquireErr := acquireRemoteAllocationStatementParticipant(key, board, 1, nil)
		require.NoError(t, acquireErr)
		_, finishErr := participant.finish(tc.cancel)
		if tc.cancel == nil {
			require.NoError(t, finishErr)
		} else {
			require.ErrorIs(t, finishErr, tc.cancel)
		}
		require.False(t, remoteAllocationStatementGroupRegistered(board))
		remoteAllocationStatementGroups.Lock()
		require.Nil(t, remoteAllocationStatementGroups.tombstones[key])
		remoteAllocationStatementGroups.Unlock()
	}

	assignLazyRemoteGeneration(post, "cn-a:6001")
	require.NotEqual(t, postID, postRemote.lazyRemoteExecutionID, "reuse must allocate a new physical generation")
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

	connectorOp := connector.NewArgument()
	dispatchOp := dispatch.NewArgument()
	t.Cleanup(connectorOp.Release)
	t.Cleanup(dispatchOp.Release)
	require.NoError(t, validateRemoteAllocationTopologyCapability([]*Scope{
		{RootOp: connectorOp},
		{RootOp: dispatchOp},
	}, nil))
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

	first, err := acquireRemoteAllocationStatementTestParticipant(t, board, 2, nil)
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

	second, err := acquireRemoteAllocationStatementTestParticipant(t, board, 2, nil)
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

func TestRemoteAllocationStatementGroupPreservesStatementError(t *testing.T) {
	board := message.NewMessageBoard()
	participant, err := acquireRemoteAllocationStatementTestParticipant(
		t,
		board,
		1,
		nil,
	)
	require.NoError(t, err)

	primary := moerr.NewDuplicateEntryNoCtx("duplicate", "primary")
	terminal, err := participant.finish(primary)
	require.True(t, terminal.complete)
	require.Same(t, primary, err)
	require.Same(t, primary, moerr.ConvertGoError(context.Background(), err))
}

func TestRemoteAllocationStatementGroupRejectsTopologyMismatch(t *testing.T) {
	board := message.NewMessageBoard()
	participant, err := acquireRemoteAllocationStatementTestParticipant(t, board, 2, nil)
	require.NoError(t, err)
	_, err = acquireRemoteAllocationStatementTestParticipant(t, board, 3, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)

	second, err := acquireRemoteAllocationStatementTestParticipant(t, board, 2, nil)
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

	participant, err := acquireRemoteAllocationStatementTestParticipant(t, board, 2, nil)
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
	participant, err := acquireRemoteAllocationStatementTestParticipant(
		t,
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
	participant, err := acquireRemoteAllocationStatementTestParticipant(t, board, 2, nil)
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

func TestRemoteAllocationStatementGroupRejectsLateFragmentAfterAbort(t *testing.T) {
	key := remoteAllocationStatementGroupKey(newRemoteExecutionID(), "cn-a:6001")
	t.Cleanup(func() { clearRemoteAllocationStatementTestTombstone(key) })

	board := message.NewMessageBoard()
	participant, _, err := acquireRemoteAllocationStatementParticipant(
		key,
		board,
		2,
		nil,
	)
	require.NoError(t, err)
	failure := moerr.NewOutOfRangeNoCtx("int8", "1000")
	terminal, err := participant.finish(failure)
	require.Error(t, err)
	require.True(t, terminal.complete)
	require.False(t, remoteAllocationStatementGroupRegistered(board))

	lateBoard := message.NewMessageBoard()
	_, _, err = acquireRemoteAllocationStatementParticipant(key, lateBoard, 2, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
	lateBoard.CloseAndDrain()

	retryKey := remoteAllocationStatementGroupKey(newRemoteExecutionID(), "cn-a:6001")
	retryBoard := message.NewMessageBoard()
	retry, _, err := acquireRemoteAllocationStatementParticipant(
		retryKey,
		retryBoard,
		1,
		nil,
	)
	require.NoError(t, err)
	terminal, err = retry.finish(nil)
	require.NoError(t, err)
	require.True(t, terminal.complete)
}

func TestRemoteAllocationStatementGroupExpiryRejectsLateFragment(t *testing.T) {
	require.Equal(t, MaxRpcTime, remoteAllocationStatementTombstoneTimeout)
	key := remoteAllocationStatementGroupKey(newRemoteExecutionID(), "cn-a:6001")
	t.Cleanup(func() { clearRemoteAllocationStatementTestTombstone(key) })

	board := message.NewMessageBoard()
	participant, _, err := acquireRemoteAllocationStatementParticipant(
		key,
		board,
		2,
		nil,
	)
	require.NoError(t, err)
	expireRemoteAllocationStatementGroup(participant.group)
	terminal, err := participant.finish(nil)
	require.Error(t, err)
	require.True(t, terminal.complete)
	require.False(t, remoteAllocationStatementGroupRegistered(board))

	lateBoard := message.NewMessageBoard()
	_, _, err = acquireRemoteAllocationStatementParticipant(key, lateBoard, 2, nil)
	require.Contains(t, err.Error(), "registration timed out")
	lateBoard.CloseAndDrain()

	retryKey := remoteAllocationStatementGroupKey(newRemoteExecutionID(), "cn-a:6001")
	retryBoard := message.NewMessageBoard()
	retry, _, err := acquireRemoteAllocationStatementParticipant(
		retryKey,
		retryBoard,
		1,
		nil,
	)
	require.NoError(t, err)
	terminal, err = retry.finish(nil)
	require.NoError(t, err)
	require.True(t, terminal.complete)
}

func TestRemoteAllocationStatementGenerationCapacity(t *testing.T) {
	activeKey := remoteAllocationStatementGroupKey(newRemoteExecutionID(), "cn-a:6001")
	activeBoard := message.NewMessageBoard()
	first, _, err := acquireRemoteAllocationStatementParticipant(
		activeKey,
		activeBoard,
		3,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		remoteAllocationStatementGroups.Lock()
		if group := remoteAllocationStatementGroups.byKey[activeKey]; group != nil {
			if group.timer != nil {
				group.timer.Stop()
			}
			delete(remoteAllocationStatementGroups.byBoard, group.board)
			delete(remoteAllocationStatementGroups.byKey, activeKey)
		}
		if tombstone := remoteAllocationStatementGroups.tombstones[activeKey]; tombstone != nil {
			if tombstone.timer != nil {
				tombstone.timer.Stop()
			}
			delete(remoteAllocationStatementGroups.tombstones, activeKey)
		}
		remoteAllocationStatementGroups.Unlock()
		activeBoard.CloseAndDrain()
	})

	remoteAllocationStatementGroups.Lock()
	for i := 1; i < remoteAllocationStatementGenerationLimit; i++ {
		key := fmt.Sprintf("capacity-tombstone-%d", i)
		remoteAllocationStatementGroups.tombstones[key] =
			&remoteAllocationStatementTombstone{}
	}
	remoteAllocationStatementGroups.Unlock()
	t.Cleanup(func() {
		remoteAllocationStatementGroups.Lock()
		for i := 1; i < remoteAllocationStatementGenerationLimit; i++ {
			delete(
				remoteAllocationStatementGroups.tombstones,
				fmt.Sprintf("capacity-tombstone-%d", i),
			)
		}
		remoteAllocationStatementGroups.Unlock()
	})

	// The reservation belongs to the generation, not each fragment.
	second, _, err := acquireRemoteAllocationStatementParticipant(
		activeKey,
		activeBoard,
		3,
		nil,
	)
	require.NoError(t, err)

	newKey := remoteAllocationStatementGroupKey(newRemoteExecutionID(), "cn-a:6001")
	newBoard := message.NewMessageBoard()
	_, _, err = acquireRemoteAllocationStatementParticipant(newKey, newBoard, 1, nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	newBoard.CloseAndDrain()

	terminal, err := first.finish(errors.New("partial dispatch failed"))
	require.NoError(t, err)
	require.False(t, terminal.complete)
	terminal, err = second.finish(errors.New("sibling canceled"))
	require.Error(t, err)
	require.True(t, terminal.complete)

	remoteAllocationStatementGroups.Lock()
	tombstoneCount := len(remoteAllocationStatementGroups.tombstones)
	remoteAllocationStatementGroups.Unlock()
	require.Equal(t, remoteAllocationStatementGenerationLimit, tombstoneCount)
}

func TestRemoteAllocationStatementGroupFailureCancelsActiveSibling(t *testing.T) {
	board := message.NewMessageBoard()
	canceled := make(chan error, 2)
	first, err := acquireRemoteAllocationStatementTestParticipant(
		t,
		board,
		3,
		func(cause error) { canceled <- cause },
	)
	require.NoError(t, err)
	second, err := acquireRemoteAllocationStatementTestParticipant(
		t,
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
	first, err := acquireRemoteAllocationStatementTestParticipant(
		t,
		board, 3, func(cause error) { canceled <- cause },
	)
	require.NoError(t, err)
	second, err := acquireRemoteAllocationStatementTestParticipant(
		t,
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

func TestRemoteAllocationStatementLateFailurePreservesNativeErrorAndBoardOwner(t *testing.T) {
	board := message.NewMessageBoard()
	key := fmt.Sprintf("late-native@%p", board)
	t.Cleanup(func() { clearRemoteAllocationStatementTestTombstone(key) })
	first, owned, err := acquireRemoteAllocationStatementParticipant(key, board, 3, nil)
	require.NoError(t, err)
	require.True(t, owned)
	second, _, err := acquireRemoteAllocationStatementParticipant(key, board, 3, nil)
	require.NoError(t, err)
	native := moerr.NewOutOfRangeNoCtx("int8", "1000")
	first.recordFailure(native)
	_, _ = first.finish(native)
	assertLate := func(board *message.MessageBoard, wantOwned bool) {
		p, owns, got := acquireRemoteAllocationStatementParticipant(key, board, 3, nil)
		require.Nil(t, p)
		require.Equal(t, wantOwned, owns)
		var wire *moerr.Error
		require.ErrorAs(t, got, &wire)
		require.Equal(t, native.MySQLCode(), wire.MySQLCode())
		require.Equal(t, native.SqlState(), wire.SqlState())
		require.False(t, errors.Is(got, mpool.ErrAllocationAccountInvariant))
	}
	assertLate(board, true) // The active sibling retains the shared board.
	assertLate(message.NewMessageBoard(), false)
	_, owns, err := acquireRemoteAllocationStatementParticipant(key, board, 2, nil)
	require.True(t, owns)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	foreignBoard := message.NewMessageBoard()
	foreign, _, err := acquireRemoteAllocationStatementParticipant(key+"-foreign", foreignBoard, 1, nil)
	require.NoError(t, err)
	_, owns, err = acquireRemoteAllocationStatementParticipant(key, foreignBoard, 3, nil)
	require.True(t, owns)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	_, _ = foreign.finish(nil)
	_, _ = second.finish(context.Canceled)
	assertLate(message.NewMessageBoard(), false) // Tombstone retains the first SQL cause.
}

func TestRemoteAllocationStatementErrorSnapshotIsBounded(t *testing.T) {
	native := moerr.NewInternalErrorNoCtx(strings.Repeat("x", 8192))
	got := snapshotRemoteStatementError(errors.Join(native, context.Canceled))
	require.NotSame(t, native, got)
	require.LessOrEqual(t, len(got.Error()), 4096)
	require.Equal(t, native.ErrorCode(), got.ErrorCode())
	require.Equal(t, native.MySQLCode(), got.MySQLCode())
	require.Equal(t, native.SqlState(), got.SqlState())
}

type remoteTerminalBarrierMessage struct {
	*remoteAllocationAccountedMessage
	entered, release chan struct{}
}

func (m *remoteTerminalBarrierMessage) Destroy() {
	close(m.entered)
	<-m.release
}

func TestRemoteAllocationStatementLateFragmentDuringBoardDrain(t *testing.T) {
	board := message.NewMessageBoard()
	key := fmt.Sprintf("draining@%p", board)
	t.Cleanup(func() { clearRemoteAllocationStatementTestTombstone(key) })
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	message.SendMessage(&remoteTerminalBarrierMessage{
		remoteAllocationAccountedMessage: &remoteAllocationAccountedMessage{},
		entered:                          entered, release: release,
	}, board)
	p, _, err := acquireRemoteAllocationStatementParticipant(key, board, 2, nil)
	require.NoError(t, err)
	native := moerr.NewOutOfRangeNoCtx("int8", "1000")
	done := make(chan struct{})
	go func() { _, _ = p.finish(native); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("board drain not entered")
	}
	late, owned, err := acquireRemoteAllocationStatementParticipant(key, board, 2, nil)
	require.Nil(t, late)
	require.True(t, owned)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "draining board must retain native error")
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("board drain not completed")
	}
}

func TestRemoteAllocationStatementRejectedCleanupSealsBeforeAdmission(t *testing.T) {
	board := message.NewMessageBoard()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	message.SendMessage(&remoteTerminalBarrierMessage{
		remoteAllocationAccountedMessage: &remoteAllocationAccountedMessage{},
		entered:                          entered, release: release,
	}, board)
	firstDone := make(chan bool, 1)
	go func() {
		sealed := sealUnownedRemoteAllocationStatementBoard(board)
		if sealed {
			board.CloseAndDrain()
		}
		firstDone <- sealed
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("rejected board drain not entered")
	}
	require.True(t, board.IsClosed())
	// A second rejected fragment must not hold admission while waiting for
	// the first fragment's blocked destruction.
	secondSealed, secondDone := make(chan struct{}), make(chan bool, 1)
	go func() {
		sealed := sealUnownedRemoteAllocationStatementBoard(board)
		close(secondSealed)
		if sealed {
			board.CloseAndDrain()
		}
		secondDone <- sealed
	}()
	select {
	case <-secondSealed:
	case <-time.After(time.Second):
		t.Fatal("second cleanup waited for destruction while holding admission")
	}
	type admission struct {
		p   *remoteAllocationStatementParticipant
		err error
	}
	admitted := make(chan admission, 1)
	go func() {
		_, owned, err := acquireRemoteAllocationStatementParticipant(fmt.Sprintf("sealed@%p", board), board, 1, nil)
		if owned || !errors.Is(err, mpool.ErrAllocationAccountInvariant) {
			admitted <- admission{err: fmt.Errorf("sealed board acquired: owned=%v err=%v", owned, err)}
			return
		}
		fresh := message.NewMessageBoard()
		p, _, err := acquireRemoteAllocationStatementParticipant(fmt.Sprintf("fresh@%p", fresh), fresh, 1, nil)
		admitted <- admission{p: p, err: err}
	}()
	select {
	case got := <-admitted:
		require.NoError(t, got.err)
		require.NotNil(t, got.p)
		_, err := got.p.finish(nil)
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("rejected cleanup blocked unrelated admission")
	}
	once.Do(func() { close(release) })
	for _, done := range []chan bool{firstDone, secondDone} {
		select {
		case closed := <-done:
			require.True(t, closed)
		case <-time.After(time.Second):
			t.Fatal("rejected cleanup not joined")
		}
	}
}

func TestRemoteAllocationStatementOwnedBoardRejectsCleanup(t *testing.T) {
	board := message.NewMessageBoard()
	p, _, err := acquireRemoteAllocationStatementParticipant(fmt.Sprintf("owned@%p", board), board, 1, nil)
	require.NoError(t, err)
	require.False(t, sealUnownedRemoteAllocationStatementBoard(board))
	require.False(t, board.IsClosed(), "rejected fragment must not seal an admitted sibling")
	_, err = p.finish(nil)
	require.NoError(t, err)
	require.True(t, board.IsClosed())
}
