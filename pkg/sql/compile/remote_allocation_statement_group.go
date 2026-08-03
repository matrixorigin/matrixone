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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"go.uber.org/zap"
)

func newRemoteExecutionID() uuid.UUID {
	return uuid.New()
}

func remoteMessageBoardID(
	statementID uuid.UUID,
	remoteExecutionID uuid.UUID,
) uuid.UUID {
	if remoteExecutionID != uuid.Nil {
		return remoteExecutionID
	}
	return statementID
}

func remoteAllocationStatementGroupKey(
	remoteExecutionID uuid.UUID,
	address string,
) string {
	if remoteExecutionID == uuid.Nil || address == "" {
		return ""
	}
	return remoteExecutionID.String() + "@" + address
}

// A missing planned RPC means the coordinator failed before dispatch
// completed. Bound the orphan lifetime by the same five-minute interval used
// by blocking MessageBoard receives. The timer is canceled as soon as every
// expected fragment has registered, so it never limits a fully dispatched
// statement's execution time.
var remoteAllocationStatementRegistrationTimeout = 5 * time.Minute

// A late RPC can carry a new MessageBoard, so keep a bounded record of an
// incomplete execution after its active group has been released. A sender
// without a caller deadline can remain in flight for MaxRpcTime; expiring the
// key earlier would let that same physical generation reopen. The record
// contains no statement resources.
const remoteAllocationStatementTombstoneTimeout = MaxRpcTime

// collectRemoteFragmentCounts computes the number of pipeline RPCs that the
// complete physical scope graph will send to each CN. The execution address
// changes when traversal crosses a Remote scope: nested scopes targeting that
// same address execute inside the received pipeline and do not create another
// RPC, while a different target does.
func collectRemoteFragmentCounts(
	scopes []*Scope,
	rootAddress string,
) map[string]uint32 {
	counts := make(map[string]uint32)
	var visit func(*Scope, string)
	visit = func(scope *Scope, executionAddress string) {
		if scope == nil {
			return
		}
		if scope.Magic == Remote && !scope.ipAddrMatch(executionAddress) {
			target := scope.NodeInfo.Addr
			counts[target]++
			executionAddress = target
		}
		for _, pre := range scope.PreScopes {
			visit(pre, executionAddress)
		}
	}
	for _, scope := range scopes {
		visit(scope, rootAddress)
	}
	return counts
}

func validateRemoteAllocationTopologyCapability(
	scopes []*Scope,
	remoteFragmentCounts map[string]uint32,
) error {
	if len(remoteFragmentCounts) > 0 {
		return nil
	}
	owners, err := collectAllocationAccountOwners(scopes)
	if err != nil {
		return err
	}
	if !hasAllocationAccountActivator(owners) {
		return nil
	}
	return moerr.NewNotSupportedNoCtx(
		"remote allocation-accounted execution requires fragment topology metadata",
	)
}

var remoteAllocationStatementGroups = struct {
	sync.Mutex
	byBoard    map[*message.MessageBoard]*remoteAllocationStatementGroup
	byKey      map[string]*remoteAllocationStatementGroup
	tombstones map[string]*remoteAllocationStatementTombstone
}{
	byBoard:    make(map[*message.MessageBoard]*remoteAllocationStatementGroup),
	byKey:      make(map[string]*remoteAllocationStatementGroup),
	tombstones: make(map[string]*remoteAllocationStatementTombstone),
}

type remoteAllocationStatementTombstone struct {
	timer *time.Timer
}

// remoteAllocationStatementGroup is the terminal owner for all pipeline RPCs
// of one statement that execute on one CN. Those RPCs share a MessageBoard, so
// no individual fragment may close it or validate transferred allocations
// while a sibling can still consume them.
type remoteAllocationStatementGroup struct {
	key          string
	board        *message.MessageBoard
	expected     uint32
	registered   uint32
	finished     uint32
	attempts     []*statementAllocationAttempt
	pools        []*mpool.MPool
	participants []*remoteAllocationStatementParticipant
	timer        *time.Timer
	expired      bool
	finalized    bool
	err          error
}

type remoteAllocationStatementParticipant struct {
	group    *remoteAllocationStatementGroup
	cancel   func(error)
	finished bool

	stageOnce  sync.Once
	finishOnce sync.Once
	terminal   remoteAllocationStatementTerminal
	err        error
}

type remoteAllocationStatementTerminal struct {
	allocation []mpool.AllocationAccountTerminalSnapshot
	memory     resource.MemoryTotals
	quality    resource.QualityFlags
	complete   bool
}

func acquireRemoteAllocationStatementParticipant(
	key string,
	board *message.MessageBoard,
	expected uint32,
	cancel func(error),
) (*remoteAllocationStatementParticipant, error) {
	if key == "" || board == nil {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	if expected == 0 {
		return nil, mpool.ErrAllocationAccountInvariant
	}

	remoteAllocationStatementGroups.Lock()
	defer remoteAllocationStatementGroups.Unlock()
	if remoteAllocationStatementGroups.tombstones[key] != nil {
		return nil, errors.Join(
			mpool.ErrAllocationAccountInvariant,
			moerr.NewInternalErrorNoCtx("remote allocation statement group already aborted"),
		)
	}
	group := remoteAllocationStatementGroups.byBoard[board]
	if group == nil {
		if remoteAllocationStatementGroups.byKey[key] != nil {
			return nil, errors.Join(
				mpool.ErrAllocationAccountInvariant,
				moerr.NewInternalErrorNoCtx("remote allocation statement group key already registered"),
			)
		}
		group = &remoteAllocationStatementGroup{
			key:      key,
			board:    board,
			expected: expected,
		}
		remoteAllocationStatementGroups.byBoard[board] = group
		remoteAllocationStatementGroups.byKey[key] = group
	}
	if group.key != key || group.expected != expected || group.finalized ||
		group.expired || group.registered >= group.expected {
		return nil, errors.Join(
			mpool.ErrAllocationAccountInvariant,
			moerr.NewInternalErrorNoCtx("invalid remote allocation statement group registration"),
		)
	}
	participant := &remoteAllocationStatementParticipant{
		group:  group,
		cancel: cancel,
	}
	group.registered++
	group.participants = append(group.participants, participant)
	if group.registered == group.expected {
		if group.timer != nil {
			group.timer.Stop()
			group.timer = nil
		}
	} else if group.timer == nil {
		group.timer = time.AfterFunc(
			remoteAllocationStatementRegistrationTimeout,
			func() { expireRemoteAllocationStatementGroup(group) },
		)
	}
	return participant, nil
}

// stage clears fragment-local operators while they are still reachable and
// retains the fragment MPool for a statement-boundary snapshot. Account and
// allocator completion are deferred until every expected fragment has
// finished.
func (p *remoteAllocationStatementParticipant) stage(
	attempt *statementAllocationAttempt,
	pool *mpool.MPool,
) {
	if p == nil || p.group == nil {
		return
	}
	p.stageOnce.Do(func() {
		if attempt != nil {
			attempt.exporter = nil
		}

		remoteAllocationStatementGroups.Lock()
		if p.group.finalized {
			p.err = mpool.ErrAllocationAccountInvariant
			remoteAllocationStatementGroups.Unlock()
			return
		}
		if attempt != nil {
			p.group.attempts = append(p.group.attempts, attempt)
		}
		if pool != nil {
			p.group.pools = append(p.group.pools, pool)
		}
		remoteAllocationStatementGroups.Unlock()

		// Transfer terminal ownership before clearing operators. If a cleanup
		// hook panics, the handler defer can still finish this participant and
		// the group retains the account and allocator domain.
		if attempt != nil {
			_ = attempt.prepareTerminal(false)
		}
	})
}

// finish marks one remote fragment quiescent without imposing a cross-RPC
// response barrier. Nested remote paths can re-enter the same CN, so waiting
// for sibling completion here would create a B -> C -> B dependency cycle.
// The fragment that completes the group publishes the aggregate exactly once;
// earlier responses carry no duplicate terminal totals.
func (p *remoteAllocationStatementParticipant) finish(cause error) (
	remoteAllocationStatementTerminal,
	error,
) {
	if p == nil || p.group == nil {
		return remoteAllocationStatementTerminal{}, nil
	}
	p.stage(nil, nil)
	p.finishOnce.Do(func() {
		remoteAllocationStatementGroups.Lock()
		group := p.group
		if group.finalized ||
			(!group.expired && group.finished >= group.expected) ||
			(group.expired && group.finished >= group.registered) {
			p.err = joinAllocationLifecycleErrors(
				p.err,
				mpool.ErrAllocationAccountInvariant,
			)
			remoteAllocationStatementGroups.Unlock()
			return
		}
		abort := cause != nil && !group.expired
		if cause != nil {
			group.err = joinAllocationLifecycleErrors(group.err, cause)
			group.expired = true
			if group.timer != nil {
				group.timer.Stop()
				group.timer = nil
			}
		}
		group.finished++
		p.finished = true
		var cancels []func(error)
		if abort {
			cancels = activeRemoteAllocationStatementCancelsLocked(group)
		}
		if !group.expired && group.registered < group.expected && group.timer == nil {
			group.timer = time.AfterFunc(
				remoteAllocationStatementRegistrationTimeout,
				func() { expireRemoteAllocationStatementGroup(group) },
			)
		}
		complete := group.expired && group.finished == group.registered ||
			group.registered == group.expected && group.finished == group.expected
		if !complete {
			remoteAllocationStatementGroups.Unlock()
			if abort {
				abortErr := allocationLifecycleCall(func() error {
					group.board.Close()
					return nil
				})
				abortErr = joinAllocationLifecycleErrors(
					abortErr,
					cancelRemoteAllocationStatementParticipants(cancels, cause),
				)
				if abortErr != nil {
					remoteAllocationStatementGroups.Lock()
					group.err = joinAllocationLifecycleErrors(group.err, abortErr)
					remoteAllocationStatementGroups.Unlock()
					p.err = joinAllocationLifecycleErrors(p.err, abortErr)
				}
			}
		} else {
			attempts, pools := takeRemoteAllocationStatementGroupLocked(group)
			terminalErr := group.err
			remoteAllocationStatementGroups.Unlock()
			defer releaseRemoteAllocationStatementGroup(group)
			p.terminal, terminalErr = completeRemoteAllocationStatementGroup(
				group,
				attempts,
				pools,
				terminalErr,
			)
			p.err = joinAllocationLifecycleErrors(p.err, terminalErr)
		}
	})
	return p.terminal, p.err
}

func releaseRemoteAllocationStatementGroup(group *remoteAllocationStatementGroup) {
	remoteAllocationStatementGroups.Lock()
	if remoteAllocationStatementGroups.byBoard[group.board] == group {
		delete(remoteAllocationStatementGroups.byBoard, group.board)
	}
	if remoteAllocationStatementGroups.byKey[group.key] == group {
		delete(remoteAllocationStatementGroups.byKey, group.key)
	}
	remoteAllocationStatementGroups.Unlock()
}

func cancelRemoteAllocationStatementParticipants(
	cancels []func(error),
	cause error,
) error {
	var err error
	for _, cancel := range cancels {
		if cancel != nil {
			err = joinAllocationLifecycleErrors(err, allocationLifecycleCall(func() error {
				cancel(cause)
				return nil
			}))
		}
	}
	return err
}

func activeRemoteAllocationStatementCancelsLocked(
	group *remoteAllocationStatementGroup,
) []func(error) {
	cancels := make([]func(error), 0, len(group.participants))
	for _, participant := range group.participants {
		if participant != nil && !participant.finished && participant.cancel != nil {
			cancels = append(cancels, participant.cancel)
		}
	}
	return cancels
}

func takeRemoteAllocationStatementGroupLocked(
	group *remoteAllocationStatementGroup,
) ([]*statementAllocationAttempt, []*mpool.MPool) {
	group.finalized = true
	if group.expired && group.registered < group.expected {
		installRemoteAllocationStatementTombstoneLocked(group.key)
	}
	if group.timer != nil {
		group.timer.Stop()
		group.timer = nil
	}
	attempts := group.attempts
	group.attempts = nil
	pools := group.pools
	group.pools = nil
	return attempts, pools
}

func installRemoteAllocationStatementTombstoneLocked(key string) {
	if remoteAllocationStatementGroups.tombstones[key] != nil {
		return
	}
	tombstone := &remoteAllocationStatementTombstone{}
	remoteAllocationStatementGroups.tombstones[key] = tombstone
	tombstone.timer = time.AfterFunc(
		remoteAllocationStatementTombstoneTimeout,
		func() {
			remoteAllocationStatementGroups.Lock()
			if remoteAllocationStatementGroups.tombstones[key] == tombstone {
				delete(remoteAllocationStatementGroups.tombstones, key)
			}
			remoteAllocationStatementGroups.Unlock()
		},
	)
}

func completeRemoteAllocationStatementGroup(
	group *remoteAllocationStatementGroup,
	attempts []*statementAllocationAttempt,
	pools []*mpool.MPool,
	terminalErr error,
) (
	remoteAllocationStatementTerminal,
	error,
) {
	terminalErr = joinAllocationLifecycleErrors(
		terminalErr,
		allocationLifecycleCall(func() error {
			group.board.CloseAndDrain()
			return nil
		}),
	)
	terminal := remoteAllocationStatementTerminal{
		complete: true,
		allocation: make(
			[]mpool.AllocationAccountTerminalSnapshot,
			0,
			len(attempts),
		),
	}
	for _, attempt := range attempts {
		snapshot, err := attempt.completeTerminal()
		terminal.allocation = append(terminal.allocation, snapshot)
		terminalErr = joinAllocationLifecycleErrors(terminalErr, err)
	}
	for _, pool := range pools {
		terminalErr = joinAllocationLifecycleErrors(
			terminalErr,
			allocationLifecycleCall(func() error {
				domain, quality := pool.ResourceSnapshot()
				terminal.quality |= quality |
					resource.MergeMemoryDomain(&terminal.memory, domain)
				return nil
			}),
		)
	}
	return terminal, terminalErr
}

func expireRemoteAllocationStatementGroup(
	group *remoteAllocationStatementGroup,
) {
	remoteAllocationStatementGroups.Lock()
	if group.finalized || group.registered == group.expected {
		remoteAllocationStatementGroups.Unlock()
		return
	}
	group.expired = true
	timeoutErr := moerr.NewInternalErrorNoCtx(
		"remote allocation statement group registration timed out",
	)
	group.err = joinAllocationLifecycleErrors(group.err, timeoutErr)
	expected, registered, finished := group.expected, group.registered, group.finished
	cancels := activeRemoteAllocationStatementCancelsLocked(group)
	var attempts []*statementAllocationAttempt
	var pools []*mpool.MPool
	complete := group.finished == group.registered
	if complete {
		attempts, pools = takeRemoteAllocationStatementGroupLocked(group)
	}
	terminalErr := group.err
	remoteAllocationStatementGroups.Unlock()

	if complete {
		defer releaseRemoteAllocationStatementGroup(group)
		_, terminalErr = completeRemoteAllocationStatementGroup(
			group,
			attempts,
			pools,
			terminalErr,
		)
	} else {
		terminalErr = joinAllocationLifecycleErrors(
			terminalErr,
			allocationLifecycleCall(func() error {
				group.board.Close()
				return nil
			}),
		)
	}
	cancelErr := cancelRemoteAllocationStatementParticipants(cancels, timeoutErr)
	terminalErr = joinAllocationLifecycleErrors(terminalErr, cancelErr)
	if !complete && cancelErr != nil {
		remoteAllocationStatementGroups.Lock()
		if !group.finalized {
			group.err = joinAllocationLifecycleErrors(group.err, cancelErr)
		}
		remoteAllocationStatementGroups.Unlock()
	}
	fields := []zap.Field{
		zap.Uint32("expected-fragments", expected),
		zap.Uint32("registered-fragments", registered),
		zap.Uint32("finished-fragments", finished),
	}
	if terminalErr != nil {
		fields = append(fields, zap.Error(terminalErr))
	}
	logutil.Warn("expired incomplete remote allocation statement group", fields...)
}
