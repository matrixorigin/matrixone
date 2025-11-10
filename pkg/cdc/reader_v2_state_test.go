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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

func TestReaderState_String(t *testing.T) {
	tests := []struct {
		name     string
		state    ReaderState
		expected string
	}{
		{"Idle", ReaderStateIdle, "Idle"},
		{"Reading", ReaderStateReading, "Reading"},
		{"Processing", ReaderStateProcessing, "Processing"},
		{"Committing", ReaderStateCommitting, "Committing"},
		{"Error", ReaderStateError, "Error"},
		{"Unknown", ReaderState(999), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestTransactionTracker_Basic(t *testing.T) {
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	tracker := NewTransactionTracker(fromTs, toTs)

	// Initial state
	assert.False(t, tracker.hasBegin)
	assert.False(t, tracker.hasCommitted)
	assert.False(t, tracker.hasRolledBack)
	assert.False(t, tracker.watermarkUpdated)
	assert.Equal(t, fromTs, tracker.GetFromTs())
	assert.Equal(t, toTs, tracker.GetToTs())
	assert.Equal(t, toTs, tracker.GetExpectedWatermark())
	assert.False(t, tracker.NeedsRollback())
	assert.False(t, tracker.IsCompleted())
}

func TestTransactionTracker_MarkBegin(t *testing.T) {
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	tracker := NewTransactionTracker(fromTs, toTs)

	tracker.MarkBegin()

	assert.True(t, tracker.hasBegin)
	assert.False(t, tracker.hasCommitted)
	assert.False(t, tracker.hasRolledBack)
	assert.True(t, tracker.NeedsRollback()) // BEGIN sent but not committed
	assert.False(t, tracker.IsCompleted())
}

func TestTransactionTracker_MarkCommit(t *testing.T) {
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	tracker := NewTransactionTracker(fromTs, toTs)

	tracker.MarkBegin()
	tracker.MarkCommit()

	assert.True(t, tracker.hasBegin)
	assert.True(t, tracker.hasCommitted)
	assert.False(t, tracker.hasRolledBack)
	assert.False(t, tracker.NeedsRollback()) // Committed, no rollback needed
	assert.True(t, tracker.IsCompleted())
}

func TestTransactionTracker_MarkRollback(t *testing.T) {
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	tracker := NewTransactionTracker(fromTs, toTs)

	tracker.MarkBegin()
	tracker.MarkRollback()

	assert.True(t, tracker.hasBegin)
	assert.False(t, tracker.hasCommitted)
	assert.True(t, tracker.hasRolledBack)
	assert.False(t, tracker.NeedsRollback()) // Rolled back, no rollback needed
	assert.True(t, tracker.IsCompleted())
}

func TestTransactionTracker_WatermarkTracking(t *testing.T) {
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	tracker := NewTransactionTracker(fromTs, toTs)

	assert.False(t, tracker.IsWatermarkUpdated())
	assert.Equal(t, toTs, tracker.GetExpectedWatermark())

	tracker.MarkWatermarkUpdated()

	assert.True(t, tracker.IsWatermarkUpdated())
}

func TestTransactionTracker_Reset(t *testing.T) {
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	tracker := NewTransactionTracker(fromTs, toTs)

	// Set some state
	tracker.MarkBegin()
	tracker.MarkCommit()
	tracker.MarkWatermarkUpdated()

	// Reset with new timestamps
	newFromTs := toTs
	newToTs := (&toTs).Next()
	tracker.Reset(newFromTs, newToTs)

	// Verify reset state
	assert.False(t, tracker.hasBegin)
	assert.False(t, tracker.hasCommitted)
	assert.False(t, tracker.hasRolledBack)
	assert.False(t, tracker.watermarkUpdated)
	assert.Equal(t, newFromTs, tracker.GetFromTs())
	assert.Equal(t, newToTs, tracker.GetToTs())
	assert.Equal(t, newToTs, tracker.GetExpectedWatermark())
}

func TestStateManager_Basic(t *testing.T) {
	sm := NewStateManager()

	// Initial state should be zero (Idle)
	assert.Equal(t, ReaderStateIdle, sm.GetState())

	// Set state
	sm.SetState(ReaderStateReading)
	assert.Equal(t, ReaderStateReading, sm.GetState())

	sm.SetState(ReaderStateProcessing)
	assert.Equal(t, ReaderStateProcessing, sm.GetState())
}

func TestStateManager_CompareAndSwap(t *testing.T) {
	sm := NewStateManager()

	// Initial state is Idle
	assert.Equal(t, ReaderStateIdle, sm.GetState())

	// Successful CAS
	swapped := sm.CompareAndSwapState(ReaderStateIdle, ReaderStateReading)
	assert.True(t, swapped)
	assert.Equal(t, ReaderStateReading, sm.GetState())

	// Failed CAS (wrong old value)
	swapped = sm.CompareAndSwapState(ReaderStateIdle, ReaderStateProcessing)
	assert.False(t, swapped)
	assert.Equal(t, ReaderStateReading, sm.GetState()) // Still Reading

	// Successful CAS
	swapped = sm.CompareAndSwapState(ReaderStateReading, ReaderStateCommitting)
	assert.True(t, swapped)
	assert.Equal(t, ReaderStateCommitting, sm.GetState())
}

func TestReadContext_Basic(t *testing.T) {
	ctx := context.Background()
	// Note: We can't easily create a real txnOp and packer for unit tests,
	// so we'll use nil for now. In real usage, these would be set.
	readCtx := NewReadContext(ctx, nil, nil)

	assert.Equal(t, ctx, readCtx.ctx)
	assert.Nil(t, readCtx.txnOp)
	assert.Nil(t, readCtx.packer)
	assert.Nil(t, readCtx.tracker)

	// Set and get tracker
	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	tracker := NewTransactionTracker(fromTs, toTs)

	readCtx.SetTracker(tracker)
	assert.Equal(t, tracker, readCtx.GetTracker())
}
