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

package disttae

import (
	"context"
	"encoding/hex"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"go.uber.org/zap"
	"slices"
	"sync"
	"time"
)

type txnRecordedUnpublished struct {
	mu    sync.Mutex
	names map[string]struct{}
}

func (r *txnRecordedUnpublished) merge(names []string) {
	if r == nil || len(names) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.names == nil {
		r.names = make(map[string]struct{}, len(names))
	}
	for _, n := range names {
		r.names[n] = struct{}{}
	}
}

func (r *txnRecordedUnpublished) has(name string) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.names[name]
	return ok
}

func (r *txnRecordedUnpublished) retract(names []string) {
	if r == nil || len(names) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range names {
		delete(r.names, n)
	}
}

func (r *txnRecordedUnpublished) retireSettled(names []string) {
	if r == nil || len(names) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range names {
		delete(r.names, n)
	}
}

func (r *txnRecordedUnpublished) takeAll() (names []string, took bool) {
	if r == nil {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.names) == 0 {
		return nil, false
	}
	names = make([]string, 0, len(r.names))
	for n := range r.names {
		names = append(names, n)
	}
	r.names = nil
	return names, true
}

func (e *Engine) registerOrphanHandoffProducer(reserved ...string) (func(), error) {
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	e.gcMu.Lock()
	defer e.gcMu.Unlock()
	if e.workspaceGCClosed.Load() {
		if len(reserved) == 0 {
			return nil, moerr.NewInvalidStateNoCtx("engine GC is closed")
		}
		for _, n := range reserved {
			if _, active := e.unpublishedPending.producerNames[n]; !active {
				return nil, moerr.NewInvalidStateNoCtx("engine GC is closed")
			}
		}
		// This is a transfer by already-admitted persistence producers, not a new
		// registration after close. Their permits remain live until queue ownership.
		return func() {}, nil
	}
	e.gcProducers++
	return func() { e.gcMu.Lock(); e.gcProducers--; e.gcMu.Unlock() }, nil
}

type pendingObjectCleanupItem struct {
	name                     string
	retainedIfRecordingLater bool
	retainedAt               int
}

func (txn *Transaction) pendingObjectCleanupNames() []string {
	names := make([]string, 0, len(txn.pendingObjectCleanup))
	for i := range txn.pendingObjectCleanup {
		names = append(names, txn.pendingObjectCleanup[i].name)
	}
	return names
}

func (txn *Transaction) PendingObjectCleanupNamesForTest() []string {
	txn.Lock()
	defer txn.Unlock()
	return txn.pendingObjectCleanupNames()
}

func (txn *Transaction) handoffPendingObjectCleanupLocked(quiesced bool) error {
	if len(txn.pendingObjectCleanup) == 0 {
		return nil
	}
	e := txn.engine
	if e == nil {
		logutil.Warn(
			"WORKSPACE-PENDING-CLEANUP-DROPPED",
			zap.Strings("files", txn.pendingObjectCleanupNames()),
			zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
		)
		return nil
	}
	epoch := txn.statementLifecycle.Load()
	moved := make([]string, 0, len(txn.pendingObjectCleanup))
	var kept []pendingObjectCleanupItem
	for i := range txn.pendingObjectCleanup {
		it := &txn.pendingObjectCleanup[i]
		if txn.hasRecordedUnpublishedObjectName(it.name) {
			continue // owned by its record's lifecycle since it landed
		}
		if borrowsCommittedCloneShare(e, txn, it.name) {
			continue
		}
		if !quiesced && it.retainedIfRecordingLater && int64(it.retainedAt) == epoch {
			kept = append(kept, *it)
			continue
		}
		moved = append(moved, it.name)
	}
	if len(moved) == 0 {
		txn.pendingObjectCleanup = kept
		return nil
	}
	releaseProducer, err := e.registerOrphanHandoffProducer(moved...)
	if err != nil {
		return err
	}
	e.unpublishedPending.mu.Lock()
	txn.pendingObjectCleanup = kept
	if orphanHandoffAfterClear != nil {
		orphanHandoffAfterClear()
	}
	moved = e.takeCleanupClaimsLocked(txn, moved)
	queued := e.handoffUnpublishedPendingLocked(moved)
	e.unpublishedPending.mu.Unlock()
	releaseProducer()
	if queued > 0 {
		e.scheduleUnpublishedPendingDrain()
	}
	return nil
}

func (e *Engine) handoffUnpublishedPendingLocked(names []string) int {
	if e.unpublishedPending.nameSet == nil {
		e.unpublishedPending.nameSet = make(map[string]struct{}, len(names))
	}
	queued := 0
	for _, n := range names {
		if _, ok := e.unpublishedPending.nameSet[n]; !ok {
			e.unpublishedPending.nameSet[n] = struct{}{}
			e.unpublishedPending.names = append(
				e.unpublishedPending.names, n)
			queued++
		}
	}
	if queued > 0 {
		e.unpublishedPending.arrivalSeq++
	}
	for _, n := range names {
		if _, ok := e.unpublishedPending.ownedNames[n]; ok {
			delete(e.unpublishedPending.ownedNames, n)
			e.unpublishedPending.live--
		}
	}
	e.finishReservationProducersLocked(names)
	return queued
}

type unpublishedPendingQueue struct {
	mu              sync.Mutex
	names           []string
	nameSet         map[string]struct{}
	drainScheduled  bool
	drainCtx        context.Context
	drainCancel     context.CancelFunc
	drainIdle       chan struct{}
	ownedNames      map[string]map[*Transaction]struct{}
	live            int64
	inFlight        int64
	arrivalSeq      uint64
	recordedOwners  map[string]*txnRecordedUnpublished
	publishedShares map[string]struct{}
	producerNames   map[string]struct{}
}

const unpublishedPendingDrainRounds = 8

const unpublishedPendingDeleteBatch = 1000

var gcUnpublishedPendingObjectTicker = time.Minute

var unpublishedPendingDrainBudget = 10 * time.Minute

var unpublishedPendingCloseDrainTimeout = 10 * time.Second

var unpublishedPendingAdmissionLimit = 500_000

func (e *Engine) InspectUnpublishedPendingForTest() (queued []string, live int, inFlight int) {
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	queued = slices.Clone(e.unpublishedPending.names)
	return queued, int(e.unpublishedPending.live), int(e.unpublishedPending.inFlight)
}

func (e *Engine) DrainUnpublishedPendingForTest(ctx context.Context) error {
	return e.deleteQueuedUnpublishedPendingOnce(ctx)
}

func (txn *Transaction) UnsettledCleanupOwnershipForTest() (pending []string, reserved []string) {
	pending = append(pending, txn.pendingObjectCleanupNames()...)
	txn.unpublishedDumpReservationsMu.Lock()
	reserved = make([]string, 0, len(txn.unpublishedDumpReservations))
	for name := range txn.unpublishedDumpReservations {
		reserved = append(reserved, name)
	}
	txn.unpublishedDumpReservationsMu.Unlock()
	slices.Sort(pending)
	slices.Sort(reserved)
	return pending, reserved
}

func (e *Engine) enqueueUnpublishedPending(names []string) {
	e.enqueueUnpublishedPendingFrom(nil, names)
}

func (e *Engine) enqueueUnpublishedPendingFrom(
	owner *Transaction, names []string,
) {
	var holder *txnRecordedUnpublished
	if owner != nil {
		holder = owner.recordedUnpublishedObjects
	}
	e.unpublishedPending.mu.Lock()
	names = e.takeCleanupClaimsLocked(owner, names)
	e.enqueueUnpublishedPendingHolderLocked(holder, names)
	e.unpublishedPending.mu.Unlock()
	e.scheduleUnpublishedPendingDrain()
}

func (e *Engine) enqueueUnpublishedPendingHolderLocked(
	holder *txnRecordedUnpublished, names []string,
) {
	if len(names) == 0 {
		return
	}
	if e.unpublishedPending.nameSet == nil {
		e.unpublishedPending.nameSet = make(map[string]struct{}, len(names))
	}
	added := 0
	for _, n := range names {
		if _, ok := e.unpublishedPending.nameSet[n]; !ok {
			e.unpublishedPending.nameSet[n] = struct{}{}
			e.unpublishedPending.names = append(e.unpublishedPending.names, n)
			added++
			if e.unpublishedPending.ownedNames != nil {
				if _, ok := e.unpublishedPending.ownedNames[n]; ok {
					delete(e.unpublishedPending.ownedNames, n)
					e.unpublishedPending.live--
				}
			}
		}
		if holder != nil {
			if e.unpublishedPending.recordedOwners == nil {
				e.unpublishedPending.recordedOwners = make(
					map[string]*txnRecordedUnpublished, len(names))
			}
			e.unpublishedPending.recordedOwners[n] = holder
		}
	}
	if added > 0 {
		e.unpublishedPending.arrivalSeq++
	}
	e.finishReservationProducersLocked(names)
}

func (e *Engine) reserveUnpublishedPendingObject(name string) error {
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	e.gcMu.Lock()
	defer e.gcMu.Unlock()
	if e.workspaceGCClosed.Load() {
		return moerr.NewInvalidStateNoCtx("engine GC is closed")
	}
	if _, ok := e.unpublishedPending.producerNames[name]; ok {
		return moerr.NewInvalidStateNoCtx("object already has a persistence producer")
	}
	if len(e.unpublishedPending.names)+int(e.unpublishedPending.live+e.unpublishedPending.inFlight) >= unpublishedPendingAdmissionLimit {
		return moerr.NewInvalidStateNoCtx("unpublished object admission bound reached")
	}
	if e.unpublishedPending.producerNames == nil {
		e.unpublishedPending.producerNames = make(map[string]struct{})
	}
	e.unpublishedPending.producerNames[name] = struct{}{}
	e.unpublishedPending.inFlight++
	e.gcProducers++
	return nil
}

// Queue lock precedes gcMu throughout admission, transfer and shutdown snapshots.
// Failed-publication conversion keeps this fence until the queue owns the name.
func (e *Engine) finishReservationProducersLocked(names []string) {
	e.gcMu.Lock()
	defer e.gcMu.Unlock()
	for _, n := range names {
		if _, ok := e.unpublishedPending.producerNames[n]; ok {
			delete(e.unpublishedPending.producerNames, n)
			e.gcProducers--
		}
	}
}

func (e *Engine) convertUnpublishedPendingReservations(
	txn *Transaction, names []string,
) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	e.unpublishedPending.inFlight -= int64(len(names))
	if e.unpublishedPending.ownedNames == nil {
		e.unpublishedPending.ownedNames = make(map[string]map[*Transaction]struct{}, len(names))
	}
	for _, n := range names {
		if _, ok := e.unpublishedPending.ownedNames[n]; !ok {
			e.unpublishedPending.ownedNames[n] = make(map[*Transaction]struct{}, 1)
			e.unpublishedPending.live++
		}
		e.unpublishedPending.ownedNames[n][txn] = struct{}{}
	}
	e.unpublishedPending.mu.Unlock()
}

func (e *Engine) admitUnpublishedPendingRecordedNames(
	txn *Transaction, names []string,
) error {
	if len(names) == 0 {
		return nil
	}
	e.unpublishedPending.mu.Lock()
	if e.workspaceGCClosed.Load() {
		e.unpublishedPending.mu.Unlock()
		return moerr.NewInvalidStateNoCtx("engine GC is closed")
	}
	for _, n := range names {
		_, queued := e.unpublishedPending.nameSet[n]
		if queued {
			e.unpublishedPending.mu.Unlock()
			return moerr.NewInvalidStateNoCtx("object cleanup already owns this name")
		}
	}
	if e.unpublishedPending.ownedNames == nil {
		e.unpublishedPending.ownedNames = make(map[string]map[*Transaction]struct{}, len(names))
	}
	fresh := 0
	for _, n := range names {
		if _, ok := e.unpublishedPending.ownedNames[n]; !ok {
			if _, ok := e.unpublishedPending.nameSet[n]; ok {
				continue // queued names are already counted and bound
			}
			fresh++
		}
	}
	queued := len(e.unpublishedPending.names)
	owned := int(e.unpublishedPending.live)
	inFlight := int(e.unpublishedPending.inFlight)
	if queued+owned+inFlight+fresh > unpublishedPendingAdmissionLimit {
		e.unpublishedPending.mu.Unlock()
		logutil.Warn(
			"WORKSPACE-UNPUBLISHED-PENDING-ADMISSION",
			zap.Strings("objects", names),
			zap.Int("retained", queued+owned),
			zap.Int("limit", unpublishedPendingAdmissionLimit),
			zap.Int("live", owned),
			zap.Int("inflight", inFlight),
		)
		return moerr.NewInternalErrorf(
			context.Background(),
			"workspace admission paused: %d retained unpublished storage objects reached the admission bound %d",
			queued+owned, unpublishedPendingAdmissionLimit,
		)
	}
	for _, n := range names {
		if _, ok := e.unpublishedPending.nameSet[n]; ok {
			continue // queued names are already counted and bound
		}
		if _, ok := e.unpublishedPending.ownedNames[n]; !ok {
			e.unpublishedPending.ownedNames[n] = make(map[*Transaction]struct{}, 1)
			e.unpublishedPending.live++
		}
		e.unpublishedPending.ownedNames[n][txn] = struct{}{}
	}
	e.unpublishedPending.mu.Unlock()
	return nil
}

func (e *Engine) releaseUnpublishedPendingPublished(txn *Transaction, names []string) {
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	for _, n := range names {
		claims, ok := e.unpublishedPending.ownedNames[n]
		if !ok {
			continue
		}
		if _, mine := claims[txn]; !mine {
			continue
		}
		delete(claims, txn)
		if len(claims) > 0 {
			if e.unpublishedPending.publishedShares == nil {
				e.unpublishedPending.publishedShares = make(map[string]struct{})
			}
			e.unpublishedPending.publishedShares[n] = struct{}{}
			continue
		}
		delete(e.unpublishedPending.publishedShares, n)
		delete(e.unpublishedPending.ownedNames, n)
		e.unpublishedPending.live--
	}
}

// Called with the queue lock: remove only this workspace's claim, and enqueue
// only after the last uncommitted claimant relinquishes ownership.
func (e *Engine) takeCleanupClaimsLocked(txn *Transaction, names []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		claims, exists := e.unpublishedPending.ownedNames[n]
		if exists {
			if _, mine := claims[txn]; !mine {
				continue
			}
			delete(claims, txn)
			if txn != nil {
				txn.recordedUnpublishedObjects.retract([]string{n})
			}
			if len(claims) > 0 {
				continue
			}
			if _, published := e.unpublishedPending.publishedShares[n]; published {
				delete(e.unpublishedPending.publishedShares, n)
				delete(e.unpublishedPending.ownedNames, n)
				e.unpublishedPending.live--
				continue
			}
		}
		out = append(out, n)
	}
	return out
}

func (e *Engine) detachTerminalRecordedUnpublishedObjectOwners(
	txn *Transaction, names []string,
) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	for _, n := range names {
		if owner, ok := e.unpublishedPending.recordedOwners[n]; ok &&
			owner == txn.recordedUnpublishedObjects {
			delete(e.unpublishedPending.recordedOwners, n)
		}
	}
	e.unpublishedPending.mu.Unlock()
}

func (e *Engine) retainUnpublishedPendingNames(
	txn *Transaction, names []string,
) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	if e.unpublishedPending.ownedNames == nil {
		e.unpublishedPending.ownedNames = make(map[string]map[*Transaction]struct{}, len(names))
	}
	for _, n := range names {
		if _, queued := e.unpublishedPending.nameSet[n]; queued {
			continue // queued names already hold the bound's space
		}
		if _, ok := e.unpublishedPending.ownedNames[n]; !ok {
			e.unpublishedPending.ownedNames[n] = make(map[*Transaction]struct{}, 1)
			e.unpublishedPending.live++
		}
		e.unpublishedPending.ownedNames[n][txn] = struct{}{}
	}
	e.unpublishedPending.mu.Unlock()
}

func (e *Engine) abandonUnpublishedPendingReservations(names []string) {
	if len(names) == 0 {
		return
	}
	e.unpublishedPending.mu.Lock()
	e.unpublishedPending.inFlight -= int64(len(names))
	e.finishReservationProducersLocked(names)
	e.unpublishedPending.mu.Unlock()
}

func (e *Engine) scheduleUnpublishedPendingDrain() {
	e.unpublishedPending.mu.Lock()
	if e.unpublishedPending.drainScheduled || len(e.unpublishedPending.names) == 0 {
		e.unpublishedPending.mu.Unlock()
		return
	}
	e.unpublishedPending.drainScheduled = true
	e.unpublishedPending.drainIdle = make(chan struct{})
	if e.unpublishedPending.drainCtx == nil {
		e.unpublishedPending.drainCtx, e.unpublishedPending.drainCancel =
			context.WithCancel(context.Background())
	}
	drainCtx := e.unpublishedPending.drainCtx
	e.unpublishedPending.mu.Unlock()
	e.gcMu.Lock()
	pool := e.gcPool
	closed := e.workspaceGCClosed.Load()
	e.gcMu.Unlock()
	if pool == nil || closed || e.fs == nil || drainCtx == nil {
		e.clearUnpublishedPendingSchedule()
		logutil.Debug("WORKSPACE-UNPUBLISHED-PENDING-NOPOOL")
		return
	}
	go func() {
		if err := pool.Submit(func() { e.drainUnpublishedPending() }); err != nil {
			e.clearUnpublishedPendingSchedule()
			logutil.Warn(
				"WORKSPACE-UNPUBLISHED-CLEANUP-SCHED",
				zap.Error(err),
			)
		}
	}()
}

func (e *Engine) clearUnpublishedPendingSchedule() {
	e.unpublishedPending.mu.Lock()
	e.unpublishedPending.drainScheduled = false
	if e.unpublishedPending.drainIdle != nil {
		close(e.unpublishedPending.drainIdle)
		e.unpublishedPending.drainIdle = nil
	}
	e.unpublishedPending.mu.Unlock()
}

func (e *Engine) cancelUnpublishedPendingDrains() {
	e.unpublishedPending.mu.Lock()
	cancel := e.unpublishedPending.drainCancel
	e.unpublishedPending.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (e *Engine) waitUnpublishedPendingDrains(timeout time.Duration) {
	e.unpublishedPending.mu.Lock()
	idle := e.unpublishedPending.drainIdle
	e.unpublishedPending.mu.Unlock()
	if idle == nil {
		return
	}
	select {
	case <-idle:
	case <-time.After(timeout):
		logutil.Warn("WORKSPACE-UNPUBLISHED-PENDING-DRAIN-JOIN-TIMEOUT",
			zap.Duration("budget", timeout))
	}
}

func (e *Engine) deleteQueuedUnpublishedPendingOnce(ctx context.Context) error {
	fs := e.fs
	if fs == nil {
		return moerr.NewInternalErrorNoCtx("file service is unavailable")
	}
	e.unpublishedPending.mu.Lock()
	pending := slices.Clone(e.unpublishedPending.names)
	e.unpublishedPending.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	var (
		failed error
		done   []string
	)
	for start := 0; start < len(pending); start += unpublishedPendingDeleteBatch {
		end := min(start+unpublishedPendingDeleteBatch, len(pending))
		batch := pending[start:end]
		if _, err := ioutil.DeleteUnpublishedObjects(ctx, fs, batch...); err != nil {
			logutil.Warn(
				"WORKSPACE-UNPUBLISHED-PENDING-KEEP",
				zap.Strings("files", batch),
				zap.Error(err),
			)
			if failed == nil {
				failed = err
			}
			continue
		}
		done = append(done, batch...)
	}
	if len(done) > 0 {
		e.unpublishedPending.mu.Lock()
		settledOwners := e.retireQueuedUnpublishedPendingLocked(done)
		e.unpublishedPending.mu.Unlock()
		for holder, holderNames := range settledOwners {
			holder.retireSettled(holderNames)
		}
	}
	return failed
}

func (e *Engine) retireQueuedUnpublishedPendingLocked(
	done []string,
) map[*txnRecordedUnpublished][]string {
	doneSet := make(map[string]struct{}, len(done))
	for _, n := range done {
		doneSet[n] = struct{}{}
	}
	var settledOwners map[*txnRecordedUnpublished][]string
	kept := e.unpublishedPending.names[:0]
	for _, n := range e.unpublishedPending.names {
		if _, ok := doneSet[n]; ok {
			if e.unpublishedPending.nameSet != nil {
				delete(e.unpublishedPending.nameSet, n)
			}
			if owner, ok := e.unpublishedPending.recordedOwners[n]; ok {
				delete(e.unpublishedPending.recordedOwners, n)
				if settledOwners == nil {
					settledOwners = make(map[*txnRecordedUnpublished][]string)
				}
				settledOwners[owner] = append(settledOwners[owner], n)
			}
			continue
		}
		kept = append(kept, n)
	}
	e.unpublishedPending.names = kept
	return settledOwners
}

func (e *Engine) drainUnpublishedPending() {
	resched := false
	defer func() {
		e.clearUnpublishedPendingSchedule()
		if resched {
			e.scheduleUnpublishedPendingDrain()
		}
	}()
	e.unpublishedPending.mu.Lock()
	drainCtx := e.unpublishedPending.drainCtx
	e.unpublishedPending.mu.Unlock()
	if e.fs == nil || drainCtx == nil {
		return
	}
	ctx, cancel := context.WithTimeout(drainCtx, unpublishedPendingDrainBudget)
	defer cancel()
	delay := 25 * time.Millisecond
	for round := 0; round < unpublishedPendingDrainRounds; round++ {
		if e.workspaceGCClosed.Load() || ctx.Err() != nil {
			return
		}
		if err := e.deleteQueuedUnpublishedPendingOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(delay)
			if delay < time.Second {
				delay *= 2
			}
			continue
		}
		e.unpublishedPending.mu.Lock()
		remaining := len(e.unpublishedPending.names) > 0
		e.unpublishedPending.mu.Unlock()
		resched = remaining
		return
	}
	logutil.Warn(
		"WORKSPACE-UNPUBLISHED-PENDING-HELD",
		zap.Int("names", func() int {
			e.unpublishedPending.mu.Lock()
			defer e.unpublishedPending.mu.Unlock()
			return len(e.unpublishedPending.names)
		}()),
	)
}

func (txn *Transaction) SinkerCapacityOption() ioutil.SinkerOption {
	return txn.dumpSinkerCapacityOption()
}

func (txn *Transaction) dumpSinkerCapacityOption() ioutil.SinkerOption {
	hook := func(name string) error {
		txn.unpublishedDumpReservationsMu.Lock()
		defer txn.unpublishedDumpReservationsMu.Unlock()
		if _, ok := txn.unpublishedDumpReservations[name]; ok {
			return nil
		}
		if err := txn.engine.reserveUnpublishedPendingObject(name); err != nil {
			return err
		}
		if txn.unpublishedDumpReservations == nil {
			txn.unpublishedDumpReservations = make(map[string]struct{})
		}
		txn.unpublishedDumpReservations[name] = struct{}{}
		return nil
	}

	return func(sinker *ioutil.Sinker) {
		ioutil.WithUncommittedCapacityHook(hook)(sinker)
		ioutil.WithWorkspaceOwnedCleanup()(sinker)
	}
}

func (txn *Transaction) noteUnpublishedDumpReservation(name string) {
	txn.unpublishedDumpReservationsMu.Lock()
	defer txn.unpublishedDumpReservationsMu.Unlock()
	if txn.unpublishedDumpReservations == nil {
		txn.unpublishedDumpReservations = make(map[string]struct{})
	}
	txn.unpublishedDumpReservations[name] = struct{}{}
}

func (txn *Transaction) recordedUnpublishedObjectNamesLocked(
	typ int, bat *batch.Batch,
) []string {
	if bat == nil || bat.Vecs == nil {
		return nil
	}
	var vec *vector.Vector
	switch typ {
	case INSERT:
		vec = bat.Vecs[1] // [%!%mo__meta_loc, object_stats]
	case DELETE:
		vec = bat.Vecs[0] // [object_stats, pk]
	default:
		return nil
	}
	if vec == nil || vec.Length() == 0 {
		return nil
	}
	col, area := vector.MustVarlenaRawData(vec)
	var (
		names = make([]string, 0, len(col))
		seen  map[string]struct{}
	)
	for i := range col {
		stats := objectio.ObjectStats(col[i].GetByteSlice(area))
		name := stats.ObjectName().String()
		if seen == nil {
			seen = make(map[string]struct{}, vec.Length())
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

func (txn *Transaction) registerUnpublishedObjectRecordLocked(
	typ int, bat *batch.Batch,
) error {
	names := txn.recordedUnpublishedObjectNamesLocked(typ, bat)
	if len(names) == 0 {
		return nil
	}
	e := txn.engine
	if e == nil {
		return nil
	}
	var reserved, fresh []string
	txn.unpublishedDumpReservationsMu.Lock()
	for _, n := range names {
		if _, ok := txn.unpublishedDumpReservations[n]; ok {
			reserved = append(reserved, n)
		} else {
			fresh = append(fresh, n)
		}
	}
	txn.unpublishedDumpReservationsMu.Unlock()
	if len(fresh) > 0 {
		if err := e.admitUnpublishedPendingRecordedNames(txn, fresh); err != nil {
			if !txn.isCCPRTxn {
				txn.retainDefiniteOrphansLocked(fresh,
					"record refused at the admission bound: the recording side fail-open-retains the refused reservation-less names so a remote fragment's persisted files never lose their cleanup owner (issue #25590 round-21 review)")
			}
			return err
		}
	}
	if len(reserved) > 0 {
		txn.unpublishedDumpReservationsMu.Lock()
		for _, n := range reserved {
			delete(txn.unpublishedDumpReservations, n)
		}
		txn.unpublishedDumpReservationsMu.Unlock()
		e.convertUnpublishedPendingReservations(txn, reserved)
	}
	txn.mergeRecordedUnpublishedObjectNames(names)
	return nil
}

func (txn *Transaction) mergeRecordedUnpublishedObjectNames(names []string) {
	if len(names) == 0 {
		return
	}
	if txn.recordedUnpublishedObjects == nil {
		txn.recordedUnpublishedObjects = &txnRecordedUnpublished{}
	}
	txn.recordedUnpublishedObjects.merge(names)
}

func (txn *Transaction) hasRecordedUnpublishedObjectName(name string) bool {
	return txn.recordedUnpublishedObjects.has(name)
}

func (txn *Transaction) releaseRecordedUnpublishedObjectsLocked() {
	names, took := txn.recordedUnpublishedObjects.takeAll()
	if !took {
		return
	}
	if txn.engine != nil {
		txn.engine.releaseUnpublishedPendingPublished(txn, names)
		txn.engine.detachTerminalRecordedUnpublishedObjectOwners(txn, names)
	}
}

func (txn *Transaction) convertDumpedReservationsLocked(files []string) {
	e := txn.engine
	if e == nil || len(files) == 0 {
		return
	}
	var reserved, extra []string
	txn.unpublishedDumpReservationsMu.Lock()
	for _, n := range files {
		if _, ok := txn.unpublishedDumpReservations[n]; ok {
			delete(txn.unpublishedDumpReservations, n)
			reserved = append(reserved, n)
		} else {
			extra = append(extra, n)
		}
	}
	txn.unpublishedDumpReservationsMu.Unlock()
	if len(reserved) > 0 {
		e.convertUnpublishedPendingReservations(txn, reserved)
	}
	if len(extra) > 0 {
		e.retainUnpublishedPendingNames(txn, extra)
	}
}

const drainUnpublishedDumpReservationsTerminalReason = "terminal reservation drain: the record never landed on a persisted or adopted name, so the terminal adopts it as its last reachable cleanup owner (issue #25590 round-22 review)"

func (txn *Transaction) drainUnpublishedDumpReservationsLocked(reason string) {
	txn.unpublishedDumpReservationsMu.Lock()
	if len(txn.unpublishedDumpReservations) == 0 {
		txn.unpublishedDumpReservationsMu.Unlock()
		return
	}
	names := make([]string, 0, len(txn.unpublishedDumpReservations))
	for n := range txn.unpublishedDumpReservations {
		names = append(names, n)
	}
	txn.unpublishedDumpReservationsMu.Unlock()
	if e := txn.engine; e != nil {
		if txn.isCCPRTxn {
			txn.unpublishedDumpReservationsMu.Lock()
			txn.unpublishedDumpReservations = nil
			txn.unpublishedDumpReservationsMu.Unlock()
			e.abandonUnpublishedPendingReservations(names)
			return
		}
		txn.retainDefiniteOrphansLocked(names, reason)
	}
	txn.unpublishedDumpReservationsMu.Lock()
	txn.unpublishedDumpReservations = nil
	txn.unpublishedDumpReservationsMu.Unlock()
}

func (txn *Transaction) RetainUnrecordedS3Objects(persisted []string, reason string) {
	txn.Lock()
	defer txn.Unlock()
	txn.retainUnrecordedS3ObjectsLocked(persisted, reason)
}

func (txn *Transaction) retainUnrecordedS3ObjectsLocked(persisted []string, reason string) {
	txn.retainUnrecordedS3ObjectsAtLocked(persisted, true, reason)
}

func (txn *Transaction) retainDefiniteOrphansLocked(persisted []string, reason string) {
	txn.retainUnrecordedS3ObjectsAtLocked(persisted, false, reason)
}

func (txn *Transaction) retainUnrecordedS3ObjectsAtLocked(
	persisted []string, retainedIfRecordingLater bool, reason string,
) {
	if len(persisted) == 0 {
		return
	}
	unrecorded := make([]string, 0, len(persisted))
	for _, n := range persisted {
		if txn.hasRecordedUnpublishedObjectName(n) {
			continue // owned by its record's lifecycle
		}
		if borrowsCommittedCloneShare(txn.engine, txn, n) {
			continue
		}
		unrecorded = append(unrecorded, n)
	}
	if len(unrecorded) == 0 {
		return
	}
	epoch := int(txn.statementLifecycle.Load())
	seen := make(map[string]struct{}, len(txn.pendingObjectCleanup)+len(unrecorded))
	for _, item := range txn.pendingObjectCleanup {
		seen[item.name] = struct{}{}
	}
	for _, n := range unrecorded {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		txn.pendingObjectCleanup = append(txn.pendingObjectCleanup,
			pendingObjectCleanupItem{
				name:                     n,
				retainedIfRecordingLater: retainedIfRecordingLater,
				retainedAt:               epoch,
			})
	}
	if txn.engine != nil {
		txn.convertDumpedReservationsLocked(unrecorded)
	}
	logutil.Warn(
		"WORKSPACE-PENDING-CLEANUP-RETAKE",
		zap.Strings("files", unrecorded),
		zap.String("reason", reason),
		zap.String("txn", hex.EncodeToString(txn.op.Txn().ID)),
	)
}

var gcFilesAfterAdmit func()
var gcFilesBeforeAdmit func()
var orphanHandoffAfterClear func()

func filterGCNamesLocked(
	e *Engine, txn *Transaction, scope cloneGCScope, names []string,
) []string {
	if !txn.isCloneTxn {
		return names
	}
	return readutil.RemoveIf(names, func(name string) bool {
		txnID := txn.op.Txn().ID
		if e.cloneTxnCache.IsSharedFile(txnID, name) {
			return true
		}
		return scope == cloneGCIntermediate &&
			e.cloneTxnCache.IsTxnLocalSharedFile(txnID, name)
	})
}

func borrowsCommittedCloneShare(e *Engine, txn *Transaction, name string) bool {
	if e == nil || !txn.isCloneTxn {
		return false
	}
	return e.cloneTxnCache.IsSharedFile(txn.op.Txn().ID, name)
}

func gcFiles(txn *Transaction, scope cloneGCScope, names ...string) error {
	return gcFilesToOwner(txn, scope, names...)
}

func (txn *Transaction) recordedWorkspaceNames(start, end int) []string {
	var objsName []string
	for i := start; i <= end; i++ {
		if txn.writes[i].bat == nil ||
			txn.writes[i].bat.RowCount() == 0 {
			continue
		}
		if txn.writes[i].fileName != "" && txn.writes[i].typ != SOFT_DELETE_OBJECT {
			var vec *vector.Vector
			if txn.writes[i].typ == DELETE {
				vec = txn.writes[i].bat.Vecs[0]
			} else {
				vec = txn.writes[i].bat.Vecs[1]
			}

			for j := range vec.Length() {
				ss := objectio.ObjectStats(vec.GetBytesAt(j))
				if txn.writes[i].typ == DELETE {
					txn.cn_flushed_s3_tombstone_object_stats_list.Delete(ss)
				}
				objsName = append(objsName, ss.ObjectName().String())
			}
		}
	}
	return objsName
}

func (txn *Transaction) gcObjsByIdxRangeToOwner(start, end int, scope cloneGCScope) (err error) {
	objsName := txn.recordedWorkspaceNames(start, end)

	defer func() {
		if err != nil {
			logutil.Warn("gc objects by index range failed",
				zap.String("txn", txn.op.Txn().DebugString()),
				zap.Strings("names", objsName),
				zap.Error(err),
			)
		}
	}()

	if len(objsName) == 0 {
		return nil
	}
	return gcFilesToOwner(txn, scope, objsName...)
}

func gcFilesToOwner(txn *Transaction, scope cloneGCScope, names ...string) error {
	if len(names) == 0 {
		return nil
	}
	e := txn.engine
	if gcFilesBeforeAdmit != nil {
		gcFilesBeforeAdmit()
	}
	release, err := e.registerOrphanHandoffProducer()
	if err != nil {
		return err
	}
	defer release()
	e.gcMu.Lock()
	names = filterGCNamesLocked(e, txn, scope, names)
	e.gcMu.Unlock()
	if gcFilesAfterAdmit != nil {
		gcFilesAfterAdmit()
	}
	// Even after shutdown begins, retain late obligations instead of dropping them.
	e.enqueueUnpublishedPendingFrom(txn, names)
	return nil
}

func (e *Engine) closeWorkspaceGC(deadline time.Time) error {
	e.gcMu.Lock()
	e.workspaceGCClosed.Store(true)
	e.gcMu.Unlock()
	e.cancelUnpublishedPendingDrains()
	e.waitUnpublishedPendingDrains(time.Until(deadline))
	// Storage may ignore cancellation. The detached attempt retains engine/queue
	// ownership and only retires names after confirmed deletion.
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		for {
			e.unpublishedPending.mu.Lock()
			e.gcMu.Lock()
			producers, queued := e.gcProducers, len(e.unpublishedPending.names)
			e.gcMu.Unlock()
			e.unpublishedPending.mu.Unlock()
			if producers == 0 && queued == 0 {
				done <- nil
				return
			}
			if err := e.deleteQueuedUnpublishedPendingOnce(ctx); err != nil {
				done <- err
				return
			}
			select {
			case <-ctx.Done():
				done <- ctx.Err()
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
