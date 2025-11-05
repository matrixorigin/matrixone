// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// ReaderState represents the state of a table reader
type ReaderState int32

const (
	// ReaderStateIdle - Reader is idle, no active transaction
	ReaderStateIdle ReaderState = iota
	// ReaderStateReading - Reader is actively reading changes
	ReaderStateReading
	// ReaderStateProcessing - Reader is processing and sending data to sinker
	ReaderStateProcessing
	// ReaderStateCommitting - Reader is committing transaction
	ReaderStateCommitting
	// ReaderStateError - Reader encountered an error
	ReaderStateError
)

func (s ReaderState) String() string {
	switch s {
	case ReaderStateIdle:
		return "Idle"
	case ReaderStateReading:
		return "Reading"
	case ReaderStateProcessing:
		return "Processing"
	case ReaderStateCommitting:
		return "Committing"
	case ReaderStateError:
		return "Error"
	default:
		return "Unknown"
	}
}

// TransactionTracker tracks the lifecycle of a transaction
// This is a key improvement: explicit transaction state tracking
// instead of relying on implicit hasBegin flag
type TransactionTracker struct {
	mu sync.Mutex

	// Transaction boundaries
	fromTs types.TS
	toTs   types.TS

	// Transaction state
	hasBegin      bool // Whether BEGIN has been sent to sinker
	hasCommitted  bool // Whether COMMIT has been sent to sinker
	hasRolledBack bool // Whether ROLLBACK has been sent to sinker

	// Watermark tracking
	// The watermark we expect to update to after successful commit
	expectedWatermark types.TS
	// Whether watermark has been updated
	watermarkUpdated bool
}

// NewTransactionTracker creates a new transaction tracker
func NewTransactionTracker(fromTs, toTs types.TS) *TransactionTracker {
	return &TransactionTracker{
		fromTs:            fromTs,
		toTs:              toTs,
		expectedWatermark: toTs,
	}
}

// MarkBegin marks that BEGIN has been sent to sinker
func (t *TransactionTracker) MarkBegin() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.hasBegin = true
}

// MarkCommit marks that COMMIT has been sent to sinker
func (t *TransactionTracker) MarkCommit() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.hasCommitted = true
}

// MarkRollback marks that ROLLBACK has been sent to sinker
func (t *TransactionTracker) MarkRollback() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.hasRolledBack = true
}

// MarkWatermarkUpdated marks that watermark has been updated
func (t *TransactionTracker) MarkWatermarkUpdated() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.watermarkUpdated = true
}

// NeedsRollback returns true if transaction needs to be rolled back
// This is the key improvement: explicit check instead of relying on watermark
func (t *TransactionTracker) NeedsRollback() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	// If BEGIN was sent but not committed and not rolled back, need rollback
	return t.hasBegin && !t.hasCommitted && !t.hasRolledBack
}

// IsCompleted returns true if transaction is fully completed (committed or rolled back)
func (t *TransactionTracker) IsCompleted() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.hasCommitted || t.hasRolledBack
}

// GetFromTs returns the fromTs of this transaction
func (t *TransactionTracker) GetFromTs() types.TS {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.fromTs
}

// GetToTs returns the toTs of this transaction
func (t *TransactionTracker) GetToTs() types.TS {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.toTs
}

// GetExpectedWatermark returns the expected watermark after commit
func (t *TransactionTracker) GetExpectedWatermark() types.TS {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.expectedWatermark
}

// IsWatermarkUpdated returns whether watermark has been updated
func (t *TransactionTracker) IsWatermarkUpdated() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.watermarkUpdated
}

// Reset resets the tracker for a new transaction
func (t *TransactionTracker) Reset(fromTs, toTs types.TS) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fromTs = fromTs
	t.toTs = toTs
	t.expectedWatermark = toTs
	t.hasBegin = false
	t.hasCommitted = false
	t.hasRolledBack = false
	t.watermarkUpdated = false
}

// StateManager manages the reader state atomically
type StateManager struct {
	state atomic.Int32
}

// NewStateManager creates a new state manager
func NewStateManager() *StateManager {
	return &StateManager{}
}

// SetState sets the reader state
func (sm *StateManager) SetState(state ReaderState) {
	sm.state.Store(int32(state))
}

// GetState returns the current reader state
func (sm *StateManager) GetState() ReaderState {
	return ReaderState(sm.state.Load())
}

// CompareAndSwapState atomically compares and swaps the state
func (sm *StateManager) CompareAndSwapState(old, new ReaderState) bool {
	return sm.state.CompareAndSwap(int32(old), int32(new))
}

// ReadContext holds the context for a read operation
type ReadContext struct {
	ctx    context.Context
	txnOp  client.TxnOperator
	packer *types.Packer

	// Current transaction tracker
	tracker *TransactionTracker

	// Change collection
	changes engine.ChangesHandle
	rel     engine.Relation

	// Error handling
	err error
}

// NewReadContext creates a new read context
func NewReadContext(ctx context.Context, txnOp client.TxnOperator, packer *types.Packer) *ReadContext {
	return &ReadContext{
		ctx:    ctx,
		txnOp:  txnOp,
		packer: packer,
	}
}

// SetTracker sets the transaction tracker
func (rc *ReadContext) SetTracker(tracker *TransactionTracker) {
	rc.tracker = tracker
}

// GetTracker returns the transaction tracker
func (rc *ReadContext) GetTracker() *TransactionTracker {
	return rc.tracker
}
