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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
)

// The record deliberately does not retain a Transaction. An unresolved
// delivery is an object obligation, not a still-running persistence producer.
type remoteWorkspaceHandoffRecord struct {
	producing bool
	exposed   bool
	decision  ioutil.RemoteWorkspaceHandoffDecision
}

type remoteWorkspaceRecordTarget struct {
	typ                              int
	accountID                        uint32
	databaseID, tableID              uint64
	autoIncrEpoch                    uint32
	autoIncrEpochKnown, skipTransfer bool
}

type remoteWorkspaceRecordReceipt struct {
	target   remoteWorkspaceRecordTarget
	decision ioutil.RemoteWorkspaceHandoffDecision
}

func (txn *Transaction) reserveRemoteCoordinatorReceipts(count int) error {
	if count == 0 {
		return nil
	}
	if txn.engine == nil {
		return moerr.NewInvalidStateNoCtx("remote workspace receipt has no engine")
	}
	q := &txn.engine.unpublishedPending
	q.mu.Lock()
	defer q.mu.Unlock()
	if txn.engine.workspaceGCClosed.Load() || count > unpublishedPendingAdmissionLimit-q.remoteCoordinatorReceipts {
		return moerr.NewInvalidStateNoCtx("remote workspace receipt capacity unavailable")
	}
	q.remoteCoordinatorReceipts += count
	return nil
}

func (txn *Transaction) releaseRemoteCoordinatorReceipts(count int) error {
	if count == 0 {
		return nil
	}
	q := &txn.engine.unpublishedPending
	q.mu.Lock()
	defer q.mu.Unlock()
	if count > q.remoteCoordinatorReceipts {
		return moerr.NewInvalidStateNoCtx("remote workspace receipt accounting underflow")
	}
	q.remoteCoordinatorReceipts -= count
	return nil
}

// Coordinator receipts are retained while the transaction exists. Once it
// ends, a missing receipt is unknown rather than evidence of non-adoption.
func (txn *Transaction) releaseRemoteCoordinatorRecordsLocked() error {
	if len(txn.remoteWorkspaceRecords) == 0 {
		return nil
	}
	q := &txn.engine.unpublishedPending
	q.mu.Lock()
	if len(txn.remoteWorkspaceRecords) > q.remoteCoordinatorReceipts {
		q.mu.Unlock()
		return moerr.NewInvalidStateNoCtx("remote workspace receipt accounting underflow")
	}
	for id := range txn.remoteWorkspaceRecords {
		delete(q.remoteCoordinatorDecisions, id)
	}
	q.remoteCoordinatorReceipts -= len(txn.remoteWorkspaceRecords)
	q.mu.Unlock()
	txn.remoteWorkspaceRecords = nil
	return nil
}

// retainRejectedRemoteRecordsLocked fences only names for which this CN has
// no existing owner or queued cleanup. All other failures remain unknown.
// The caller already reserved one coordinator receipt slot for each ID.
func (txn *Transaction) retainRejectedRemoteRecordsLocked(ids []ioutil.RemoteWorkspaceHandoffID, target remoteWorkspaceRecordTarget) int {
	if txn.isCCPRTxn || txn.isCloneTxn {
		return 0 // shared source ownership cannot be disproved here
	}
	q := &txn.engine.unpublishedPending
	q.mu.Lock()
	defer q.mu.Unlock()
	if txn.engine.workspaceGCClosed.Load() {
		return 0
	}
	kept := 0
	for _, id := range ids {
		name := id.ObjectName
		if _, ok := q.ownedNames[name]; ok {
			continue
		}
		if _, ok := q.nameSet[name]; ok {
			continue
		}
		if _, ok := q.publishedShares[name]; ok {
			continue
		}
		if _, ok := q.remoteByName[name]; ok {
			continue
		}
		if _, ok := q.producerNames[name]; ok {
			continue
		}
		if _, exists := q.remoteCoordinatorDecisions[id]; exists {
			continue
		}
		if txn.remoteWorkspaceRecords == nil {
			txn.remoteWorkspaceRecords = make(map[ioutil.RemoteWorkspaceHandoffID]remoteWorkspaceRecordReceipt)
		}
		if q.remoteCoordinatorDecisions == nil {
			q.remoteCoordinatorDecisions = make(map[ioutil.RemoteWorkspaceHandoffID]ioutil.RemoteWorkspaceHandoffDecision)
		}
		txn.remoteWorkspaceRecords[id] = remoteWorkspaceRecordReceipt{target: target, decision: ioutil.RemoteWorkspaceHandoffRejected}
		q.remoteCoordinatorDecisions[id] = ioutil.RemoteWorkspaceHandoffRejected
		q.noteRemoteRejectedNameLocked(name)
		kept++
	}
	return kept
}

// selectRemoteWorkspaceRecordsLocked runs under the transaction mutex before
// any entry is published. Retained identities make a repeated delivery a
// no-op, including after the original statement has rolled back.
func (txn *Transaction) selectRemoteWorkspaceRecordsLocked(typ int, bat *batch.Batch, target remoteWorkspaceRecordTarget) ([]int64, []ioutil.RemoteWorkspaceHandoffID, bool, error) {
	if bat == nil {
		return nil, nil, false, nil
	}
	scope, present, err := ioutil.ParseRemoteWorkspaceHandoffScope(bat.ExtraBuf)
	if err != nil || !present {
		return nil, nil, present, err
	}
	if txn.op == nil || scope.TxnID != string(txn.op.Txn().ID) {
		return nil, nil, true, moerr.NewInvalidStateNoCtx("workspace record has a mismatched remote owner")
	}
	column := 1
	if typ == DELETE {
		column = 0
	} else if typ != INSERT {
		return nil, nil, true, moerr.NewInvalidInputNoCtx("unsupported remote workspace record type")
	}
	if column >= len(bat.Vecs) || bat.Vecs[column] == nil || (typ == DELETE && bat.Vecs[column].Length() != bat.RowCount()) || (typ == INSERT && (len(bat.Vecs) != 2 || bat.Vecs[0] == nil || bat.Vecs[0].Length() != bat.RowCount())) {
		return nil, nil, true, moerr.NewInvalidInputNoCtx("invalid remote workspace object batch")
	}
	count := bat.Vecs[column].Length()
	keep := make([]int64, 0, count)
	ids := make([]ioutil.RemoteWorkspaceHandoffID, 0, count)
	seen := make(map[ioutil.RemoteWorkspaceHandoffID]struct{}, count)
	var objects map[objectio.ObjectNameShort]struct{}
	if typ == INSERT {
		objects = make(map[objectio.ObjectNameShort]struct{}, count)
	}
	for i := 0; i < count; i++ {
		raw := bat.Vecs[column].GetBytesAt(i)
		if len(raw) != objectio.ObjectStatsLen {
			return nil, nil, true, moerr.NewInvalidInputNoCtx("invalid remote workspace object statistics")
		}
		stats := objectio.ObjectStats(raw)
		id := scope.Object(stats.ObjectName().String())
		if _, ok := seen[id]; ok {
			return nil, nil, true, moerr.NewInvalidInputNoCtx("duplicate object in remote workspace batch")
		}
		seen[id] = struct{}{}
		if objects != nil {
			objects[*stats.ObjectName().Short()] = struct{}{}
		}
		if previous, ok := txn.remoteWorkspaceRecords[id]; ok {
			if previous.target != target {
				return nil, nil, true, moerr.NewInvalidStateNoCtx("remote workspace object was recorded for another target")
			}
			if previous.decision == ioutil.RemoteWorkspaceHandoffRejected {
				return nil, nil, true, moerr.NewInvalidStateNoCtx("remote workspace object was previously rejected")
			}
			continue
		}
		keep = append(keep, int64(i))
		ids = append(ids, id)
	}
	if typ == INSERT {
		for i := 0; i < bat.RowCount(); i++ {
			raw := bat.Vecs[0].GetBytesAt(i)
			if len(raw) != objectio.BlockInfoSize {
				return nil, nil, true, moerr.NewInvalidInputNoCtx("invalid remote workspace block metadata")
			}
			block := objectio.DecodeBlockInfo(raw)
			if _, ok := objects[objectio.ObjectNameShort(*block.BlockID.Object())]; !ok {
				return nil, nil, true, moerr.NewInvalidInputNoCtx("remote workspace block has no matching object")
			}
		}
	}
	if len(txn.remoteWorkspaceRecords)+len(ids) > unpublishedPendingAdmissionLimit {
		return nil, nil, true, moerr.NewInternalErrorNoCtx("remote workspace receipt limit reached")
	}
	return keep, ids, true, nil
}

// INSERT metadata has one row per block in column zero and one row per object
// in column one. Shrink them independently when a delivery mixes already
// adopted objects with new ones; Batch.Shrink would conflate these lengths.
func shrinkRemoteWorkspaceMetadata(typ int, bat *batch.Batch, objectRows []int64) error {
	if typ == DELETE {
		bat.Shrink(objectRows, false)
		return nil
	}
	kept := make(map[objectio.ObjectNameShort]struct{}, len(objectRows))
	for _, i := range objectRows {
		stats := objectio.ObjectStats(bat.Vecs[1].GetBytesAt(int(i)))
		kept[*stats.ObjectName().Short()] = struct{}{}
	}
	blockRows := make([]int64, 0, bat.RowCount())
	for i := 0; i < bat.RowCount(); i++ {
		raw := bat.Vecs[0].GetBytesAt(i)
		if len(raw) != objectio.BlockInfoSize {
			return moerr.NewInvalidInputNoCtx("invalid remote workspace block metadata")
		}
		block := objectio.DecodeBlockInfo(raw)
		if _, ok := kept[objectio.ObjectNameShort(*block.BlockID.Object())]; ok {
			blockRows = append(blockRows, int64(i))
		}
	}
	bat.Vecs[0].Shrink(blockRows, false)
	bat.Vecs[1].Shrink(objectRows, false)
	bat.SetRowCount(len(blockRows))
	return nil
}

// A bounded receipt cache permits acknowledgement retries after settlement.
// An evicted receipt is unknown, never evidence that an object was rejected.
const remoteWorkspaceHandoffReceiptLimit = 4096

// This bounded filter has no false negatives for names seen in this Engine
// lifetime. False positives retain an object as unknown rather than deleting
// an object which might have been adopted by an older attempt.
func remoteAdoptedNameBits(name string) [3]uint64 {
	const mask = (1 << 23) - 1
	h := uint64(14695981039346656037)
	for i := 0; i < len(name); i++ {
		h = (h ^ uint64(name[i])) * 1099511628211
	}
	step := (h>>33 | h<<31) | 1
	return [3]uint64{h & mask, (h + step) & mask, (h + 2*step) & mask}
}

func (q *unpublishedPendingQueue) noteRemoteAdoptedNameLocked(name string) {
	if q.remoteAdoptedNameFilter == nil {
		q.remoteAdoptedNameFilter = new([1 << 17]uint64)
	}
	for _, bit := range remoteAdoptedNameBits(name) {
		q.remoteAdoptedNameFilter[bit/64] |= uint64(1) << (bit % 64)
	}
}

func (q *unpublishedPendingQueue) mightHaveRemoteAdoptedNameLocked(name string) bool {
	if q.remoteAdoptedNameFilter == nil {
		return false
	}
	for _, bit := range remoteAdoptedNameBits(name) {
		if q.remoteAdoptedNameFilter[bit/64]&(uint64(1)<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

func (q *unpublishedPendingQueue) noteRemoteRejectedNameLocked(name string) {
	if q.remoteRejectedNameFilter == nil {
		q.remoteRejectedNameFilter = new([1 << 17]uint64)
	}
	for _, bit := range remoteAdoptedNameBits(name) {
		q.remoteRejectedNameFilter[bit/64] |= uint64(1) << (bit % 64)
	}
}

func (q *unpublishedPendingQueue) mightHaveRemoteRejectedNameLocked(name string) bool {
	if q.remoteRejectedNameFilter == nil {
		return false
	}
	for _, bit := range remoteAdoptedNameBits(name) {
		if q.remoteRejectedNameFilter[bit/64]&(uint64(1)<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// ConfigureRemoteWorkspaceHandoff installs the transport-negotiated identity
// before any Sinker can reserve an object. It cannot be changed mid-fragment.
func (txn *Transaction) ConfigureRemoteWorkspaceHandoff(scope ioutil.RemoteWorkspaceHandoffScope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if txn == nil || txn.op == nil || txn.engine == nil || string(txn.op.Txn().ID) != scope.TxnID {
		return moerr.NewInvalidStateNoCtx("invalid remote workspace handoff scope")
	}
	txn.remoteHandoffMu.Lock()
	defer txn.remoteHandoffMu.Unlock()
	if txn.remoteHandoffScope != nil {
		if *txn.remoteHandoffScope == scope {
			return nil
		}
		return moerr.NewInvalidStateNoCtx("remote workspace handoff scope already set")
	}
	txn.unpublishedDumpReservationsMu.Lock()
	defer txn.unpublishedDumpReservationsMu.Unlock()
	if len(txn.unpublishedDumpReservations) != 0 {
		return moerr.NewInvalidStateNoCtx("remote workspace handoff was configured after object admission")
	}
	txn.remoteHandoffScope = &scope
	return nil
}

// RemoteWorkspaceHandoffScope returns a copy; the configured scope remains
// immutable for the lifetime of the fragment.
func (txn *Transaction) RemoteWorkspaceHandoffScope() (ioutil.RemoteWorkspaceHandoffScope, bool) {
	if txn == nil {
		return ioutil.RemoteWorkspaceHandoffScope{}, false
	}
	txn.remoteHandoffMu.RLock()
	defer txn.remoteHandoffMu.RUnlock()
	if txn.remoteHandoffScope == nil {
		return ioutil.RemoteWorkspaceHandoffScope{}, false
	}
	return *txn.remoteHandoffScope, true
}

// MarkRemoteWorkspaceOutputReady runs before a result batch can escape the
// writer. Unmarked objects were not included in an emitted result batch.
func (txn *Transaction) MarkRemoteWorkspaceOutputReady(names []string) error {
	if txn == nil || txn.engine == nil {
		return moerr.NewInvalidStateNoCtx("remote workspace handoff has no engine")
	}
	txn.remoteHandoffMu.RLock()
	defer txn.remoteHandoffMu.RUnlock()
	if txn.remoteHandoffScope == nil {
		return moerr.NewInvalidStateNoCtx("remote workspace handoff is not configured")
	}
	txn.unpublishedDumpReservationsMu.Lock()
	defer txn.unpublishedDumpReservationsMu.Unlock()
	q := &txn.engine.unpublishedPending
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, name := range names {
		id := txn.remoteHandoffScope.Object(name)
		if _, ok := txn.unpublishedDumpReservations[name]; !ok {
			return moerr.NewInvalidStateNoCtx("remote workspace output has no reservation")
		}
		record, ok := q.remoteHandoffs[id]
		if !ok || !record.producing {
			return moerr.NewInvalidStateNoCtx("remote workspace output has no active producer")
		}
		record.exposed = true
		q.remoteHandoffs[id] = record
	}
	return nil
}

func validateRemoteWorkspaceHandoff(id ioutil.RemoteWorkspaceHandoffID) error {
	if id.Version != ioutil.RemoteWorkspaceHandoffVersion1 ||
		len(id.TxnID) == 0 || len(id.TxnID) > 64 ||
		id.ExecutionID == [16]byte{} || id.FragmentID == [16]byte{} ||
		len(id.ObjectName) == 0 || len(id.ObjectName) > 512 {
		return moerr.NewInvalidInputNoCtx("invalid remote workspace handoff identity")
	}
	return nil
}

// bindRemoteWorkspaceHandoff attaches an execution identity to an already
// admitted producer before persistence starts. Lock order matches Sinker
// admission: reservation mutex, queue mutex, then GC mutex.
func (txn *Transaction) bindRemoteWorkspaceHandoff(id ioutil.RemoteWorkspaceHandoffID) error {
	if err := validateRemoteWorkspaceHandoff(id); err != nil {
		return err
	}
	if txn == nil || txn.op == nil || txn.engine == nil || string(txn.op.Txn().ID) != id.TxnID {
		return moerr.NewInvalidStateNoCtx("remote handoff transaction does not match producer")
	}
	txn.unpublishedDumpReservationsMu.Lock()
	defer txn.unpublishedDumpReservationsMu.Unlock()
	return txn.bindRemoteWorkspaceHandoffReservationLocked(id)
}

func (txn *Transaction) bindRemoteWorkspaceHandoffReservationLocked(id ioutil.RemoteWorkspaceHandoffID) error {
	q := &txn.engine.unpublishedPending
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := txn.unpublishedDumpReservations[id.ObjectName]; !ok {
		return moerr.NewInvalidStateNoCtx("remote handoff has no producer reservation")
	}
	if _, ok := q.remoteReceipts[id]; ok {
		return moerr.NewInvalidStateNoCtx("remote handoff identity was already settled")
	}
	if existing, ok := q.remoteHandoffs[id]; ok {
		if existing.producing {
			return nil
		}
		return moerr.NewInvalidStateNoCtx("remote handoff producer has already stopped")
	}
	if _, ok := q.producerNames[id.ObjectName]; !ok {
		return moerr.NewInvalidStateNoCtx("remote handoff producer is not active")
	}
	if _, ok := q.remoteByName[id.ObjectName]; ok {
		return moerr.NewInvalidStateNoCtx("object has another remote handoff")
	}
	if _, ok := q.nameSet[id.ObjectName]; ok {
		return moerr.NewInvalidStateNoCtx("object is already queued for cleanup")
	}
	if _, ok := q.ownedNames[id.ObjectName]; ok {
		return moerr.NewInvalidStateNoCtx("object already has a local owner")
	}
	if _, ok := q.publishedShares[id.ObjectName]; ok {
		return moerr.NewInvalidStateNoCtx("object is already shared by a published owner")
	}
	if txn.isCloneTxn && txn.engine.cloneTxnCache != nil &&
		(txn.engine.cloneTxnCache.IsSharedFile(txn.op.Txn().ID, id.ObjectName) ||
			txn.engine.cloneTxnCache.IsTxnLocalSharedFile(txn.op.Txn().ID, id.ObjectName)) {
		return moerr.NewInvalidStateNoCtx("remote producer cannot own a shared source object")
	}
	if q.remoteHandoffs == nil {
		q.remoteHandoffs = make(map[ioutil.RemoteWorkspaceHandoffID]remoteWorkspaceHandoffRecord)
		q.remoteByName = make(map[string]ioutil.RemoteWorkspaceHandoffID)
	}
	q.remoteHandoffs[id] = remoteWorkspaceHandoffRecord{producing: true}
	q.remoteByName[id.ObjectName] = id
	return nil
}

// stopRemoteWorkspaceProducer is called only after all writes and jobs for the
// object have stopped. A definite unsent failure is rejected; a sent or
// ambiguous result remains pending until an authenticated decision arrives.
func (txn *Transaction) stopRemoteWorkspaceProducer(id ioutil.RemoteWorkspaceHandoffID, definitelyUnsent bool) error {
	if err := validateRemoteWorkspaceHandoff(id); err != nil {
		return err
	}
	if txn == nil || txn.engine == nil || txn.op == nil || string(txn.op.Txn().ID) != id.TxnID {
		return moerr.NewInvalidStateNoCtx("remote handoff transaction does not match producer")
	}
	txn.unpublishedDumpReservationsMu.Lock()
	q := &txn.engine.unpublishedPending
	q.mu.Lock()
	record, ok := q.remoteHandoffs[id]
	if !ok {
		_, reserved := txn.unpublishedDumpReservations[id.ObjectName]
		q.mu.Unlock()
		txn.unpublishedDumpReservationsMu.Unlock()
		if !reserved {
			return nil // repeated stop, or an old attempt; neither owns a producer
		}
		return moerr.NewInvalidStateNoCtx("remote handoff identity is not registered")
	}
	if !record.producing {
		q.mu.Unlock()
		txn.unpublishedDumpReservationsMu.Unlock()
		return nil
	}
	if definitelyUnsent && !record.exposed && record.decision == ioutil.RemoteWorkspaceHandoffAdopted {
		q.mu.Unlock()
		txn.unpublishedDumpReservationsMu.Unlock()
		return moerr.NewInvalidStateNoCtx("an adopted object cannot be definitely unsent")
	}
	if _, ok := txn.unpublishedDumpReservations[id.ObjectName]; !ok || q.inFlight <= 0 {
		q.mu.Unlock()
		txn.unpublishedDumpReservationsMu.Unlock()
		return moerr.NewInvalidStateNoCtx("remote producer reservation is missing")
	}
	if _, ok := q.producerNames[id.ObjectName]; !ok {
		q.mu.Unlock()
		txn.unpublishedDumpReservationsMu.Unlock()
		return moerr.NewInvalidStateNoCtx("remote persistence producer is missing")
	}
	delete(txn.unpublishedDumpReservations, id.ObjectName)
	q.inFlight--
	txn.engine.finishReservationProducersLocked([]string{id.ObjectName})
	record.producing = false
	q.remotePending++
	if definitelyUnsent && !record.exposed && !q.mightHaveRemoteAdoptedNameLocked(id.ObjectName) {
		record.decision = ioutil.RemoteWorkspaceHandoffRejected
	}
	q.remoteHandoffs[id] = record
	queued := txn.engine.settleRemoteWorkspaceHandoffLocked(id, record)
	q.mu.Unlock()
	txn.unpublishedDumpReservationsMu.Unlock()
	if queued {
		txn.engine.scheduleUnpublishedPendingDrain()
	}
	return nil
}

// decideRemoteWorkspaceHandoff applies only an exact identity. The bool is
// false for an absent or stale attempt; such a response does not prove adoption.
func (e *Engine) decideRemoteWorkspaceHandoff(id ioutil.RemoteWorkspaceHandoffID, decision ioutil.RemoteWorkspaceHandoffDecision) (bool, error) {
	if err := validateRemoteWorkspaceHandoff(id); err != nil {
		return false, err
	}
	if decision != ioutil.RemoteWorkspaceHandoffAdopted && decision != ioutil.RemoteWorkspaceHandoffRejected {
		return false, moerr.NewInvalidInputNoCtx("invalid remote handoff decision")
	}
	q := &e.unpublishedPending
	q.mu.Lock()
	record, ok := q.remoteHandoffs[id]
	if !ok {
		previous, found := q.remoteReceipts[id]
		q.mu.Unlock()
		if !found {
			return false, nil
		}
		if previous != decision {
			return false, moerr.NewInvalidStateNoCtx("conflicting remote handoff decisions")
		}
		return true, nil
	}
	if record.decision != ioutil.RemoteWorkspaceHandoffUnknown && record.decision != decision {
		q.mu.Unlock()
		return false, moerr.NewInvalidStateNoCtx("conflicting remote handoff decisions")
	}
	if decision == ioutil.RemoteWorkspaceHandoffRejected && q.mightHaveRemoteAdoptedNameLocked(id.ObjectName) {
		q.mu.Unlock()
		return false, nil
	}
	if decision == ioutil.RemoteWorkspaceHandoffAdopted {
		q.noteRemoteAdoptedNameLocked(id.ObjectName)
	}
	record.decision = decision
	q.remoteHandoffs[id] = record
	queued := e.settleRemoteWorkspaceHandoffLocked(id, record)
	q.mu.Unlock()
	if queued {
		e.scheduleUnpublishedPendingDrain()
	}
	return true, nil
}

// ResolveRemoteWorkspaceHandoff processes a peer's per-object decision. A
// false match means the identity is unknown, including an evicted receipt.
func (e *Engine) ResolveRemoteWorkspaceHandoff(id ioutil.RemoteWorkspaceHandoffID, decision ioutil.RemoteWorkspaceHandoffDecision) (bool, error) {
	if e == nil {
		return false, moerr.NewInvalidStateNoCtx("remote workspace handoff engine is unavailable")
	}
	return e.decideRemoteWorkspaceHandoff(id, decision)
}

// QueryRemoteWorkspaceHandoff never treats an absent or evicted receipt as a
// rejection. The exact identity, including attempt, is required for lookup.
func (e *Engine) QueryRemoteWorkspaceHandoff(id ioutil.RemoteWorkspaceHandoffID) (ioutil.RemoteWorkspaceHandoffDecision, error) {
	if e == nil {
		return ioutil.RemoteWorkspaceHandoffUnknown, moerr.NewInvalidStateNoCtx("remote workspace handoff engine is unavailable")
	}
	if err := validateRemoteWorkspaceHandoff(id); err != nil {
		return ioutil.RemoteWorkspaceHandoffUnknown, err
	}
	q := &e.unpublishedPending
	q.mu.Lock()
	defer q.mu.Unlock()
	if record, ok := q.remoteHandoffs[id]; ok {
		return record.decision, nil
	}
	return q.remoteReceipts[id], nil
}

func (e *Engine) QueryCoordinatorWorkspaceHandoff(id ioutil.RemoteWorkspaceHandoffID) (ioutil.RemoteWorkspaceHandoffDecision, error) {
	if e == nil {
		return ioutil.RemoteWorkspaceHandoffUnknown, moerr.NewInvalidStateNoCtx("remote workspace coordinator is unavailable")
	}
	if err := validateRemoteWorkspaceHandoff(id); err != nil {
		return ioutil.RemoteWorkspaceHandoffUnknown, err
	}
	q := &e.unpublishedPending
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.remoteCoordinatorDecisions[id], nil
}

// InspectRemoteWorkspaceHandoffsForTest reports aggregate state without
// exposing transaction or object identifiers.
func (e *Engine) InspectRemoteWorkspaceHandoffsForTest() (active, pending, adopted, rejected int) {
	q := &e.unpublishedPending
	q.mu.Lock()
	defer q.mu.Unlock()
	active, pending = len(q.remoteHandoffs), int(q.remotePending)
	for _, decision := range q.remoteReceipts {
		switch decision {
		case ioutil.RemoteWorkspaceHandoffAdopted:
			adopted++
		case ioutil.RemoteWorkspaceHandoffRejected:
			rejected++
		}
	}
	return
}

// InspectRemoteWorkspaceCoordinatorForTest exposes aggregate live decisions
// without exposing transaction or object identifiers.
func (e *Engine) InspectRemoteWorkspaceCoordinatorForTest() (adopted, rejected int) {
	q := &e.unpublishedPending
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, decision := range q.remoteCoordinatorDecisions {
		switch decision {
		case ioutil.RemoteWorkspaceHandoffAdopted:
			adopted++
		case ioutil.RemoteWorkspaceHandoffRejected:
			rejected++
		}
	}
	return
}

func (e *Engine) RemoteWorkspaceCoordinatorIDsForTest(decision ioutil.RemoteWorkspaceHandoffDecision) []ioutil.RemoteWorkspaceHandoffID {
	q := &e.unpublishedPending
	q.mu.Lock()
	defer q.mu.Unlock()
	var ids []ioutil.RemoteWorkspaceHandoffID
	for id, state := range q.remoteCoordinatorDecisions {
		if state == decision {
			ids = append(ids, id)
		}
	}
	return ids
}

func (e *Engine) RemoteWorkspaceRejectedObjectCleanedForTest(id ioutil.RemoteWorkspaceHandoffID) bool {
	q := &e.unpublishedPending
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.remoteReceipts[id] != ioutil.RemoteWorkspaceHandoffRejected {
		return false
	}
	_, queued := q.nameSet[id.ObjectName]
	_, producing := q.producerNames[id.ObjectName]
	_, unresolved := q.remoteHandoffs[id]
	return !queued && !producing && !unresolved
}

// Caller holds unpublishedPending.mu. An accepted object is now exclusively
// the coordinator's responsibility; a rejected object goes to local GC.
func (e *Engine) settleRemoteWorkspaceHandoffLocked(id ioutil.RemoteWorkspaceHandoffID, record remoteWorkspaceHandoffRecord) bool {
	if record.producing || record.decision == ioutil.RemoteWorkspaceHandoffUnknown {
		return false
	}
	q := &e.unpublishedPending
	delete(q.remoteHandoffs, id)
	delete(q.remoteByName, id.ObjectName)
	q.remotePending--
	if q.remoteReceipts == nil {
		q.remoteReceipts = make(map[ioutil.RemoteWorkspaceHandoffID]ioutil.RemoteWorkspaceHandoffDecision)
	}
	if len(q.remoteReceiptOrder) < remoteWorkspaceHandoffReceiptLimit {
		q.remoteReceiptOrder = append(q.remoteReceiptOrder, id)
	} else {
		delete(q.remoteReceipts, q.remoteReceiptOrder[q.remoteReceiptNext])
		q.remoteReceiptOrder[q.remoteReceiptNext] = id
		q.remoteReceiptNext = (q.remoteReceiptNext + 1) % remoteWorkspaceHandoffReceiptLimit
	}
	q.remoteReceipts[id] = record.decision
	if record.decision == ioutil.RemoteWorkspaceHandoffRejected {
		e.handoffUnpublishedPendingLocked([]string{id.ObjectName})
		return true
	}
	return false
}
