// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestLockReadObservationMergeAndSeal(t *testing.T) {
	ctx := context.Background()
	root := NewRoot(ConnExternal)
	at := time.Now()
	leaf := &LockReadObservation{}
	leaf.Record(ctx, LockReadPKReadiness, at, at.Add(10*time.Millisecond), context.Canceled)
	leaf.Record(ctx, LockReadPKCheck, at, at.Add(12*time.Millisecond), context.Canceled)

	latest := &LockReadObservation{}
	latest.Record(ctx, LockReadAcquisition, at, at.Add(30*time.Millisecond), nil)
	key := []byte("0123456789abcdef")
	lockCtx := ContextWithLockReadAcquisition(ctx, latest)
	ObserveLocalLockConflict(lockCtx, [][]byte{append(append([]byte(nil), key...), 0xff)})
	latest.ObserveAcquisitionConflict(false, nil)
	require.True(t, latest.totals.conflict)
	require.False(t, latest.totals.hasHolder)
	ObserveLocalLockConflict(lockCtx, [][]byte{key})
	clear(key)
	latest.ObserveAcquisitionConflict(false, nil)
	latest.Record(ctx, LockReadLogtail, at, at.Add(20*time.Millisecond), context.DeadlineExceeded)

	// The later-finishing phase must win regardless of merge order.
	var workers sync.WaitGroup
	for _, observed := range []*LockReadObservation{latest, leaf} {
		workers.Go(func() { root.MergeLockRead(observed) })
	}
	workers.Wait()
	require.Nil(t, root.PreResponseSummary().LockRead)
	require.True(t, root.MergeExecution(ExecutionSummary{AttemptCount: 2}))
	summary, snapshot := root.SealWithLockRead(1)
	require.True(t, snapshot.Qualifies(0, time.Second))
	d := snapshot.Diagnostic(uuid.Nil, summary.AttemptCount, false)
	require.Equal(t, uint8(15), d.Seen)
	require.Equal(t, uint64(30*time.Millisecond), d.AcquisitionNS)
	require.Equal(t, uint64(20*time.Millisecond), d.LogtailNS)
	require.Equal(t, uint64(12*time.Millisecond), d.PKCheckNS)
	require.Equal(t, uint64(10*time.Millisecond), d.PKReadinessNS)
	require.Equal(t, "30313233343536373839616263646566", d.ObservedConflictTxnID)
	require.Equal(t, "logtail", d.LastIncomplete)
	require.Equal(t, "success", d.Outcome)
	require.Equal(t, uint64(2), d.AttemptCount)

	root.MergeLockRead(latest)
	sealed, after := root.SealWithLockRead(9)
	require.Equal(t, summary, sealed)
	require.Equal(t, snapshot, after)
	summary.LockRead = &d
	cloned := summary.Clone()
	summary.LockRead.Outcome = "failed"
	require.Equal(t, "success", cloned.LockRead.Outcome)
	raw, err := json.Marshal(StatementResourceSummary{})
	require.NoError(t, err)
	require.NotContains(t, string(raw), "lock_read")
}

func TestLockReadWitnessAcrossAttempts(t *testing.T) {
	ctx := context.Background()
	root := NewRoot(ConnExternal)
	at := time.Unix(1, 0)
	first, second := []byte("first-holder-txn"), []byte("next-holder--txn")

	for i, holder := range [][]byte{first, second} {
		var err error
		if i == 1 {
			err = context.Canceled
		}
		observation := &LockReadObservation{}
		lockCtx := ContextWithLockReadAcquisition(ctx, observation)
		ObserveLocalLockConflict(lockCtx, [][]byte{holder})
		observation.Record(lockCtx, LockReadAcquisition, at, at.Add(time.Millisecond), err)
		observation.ObserveAcquisitionConflict(false, err)
		root.MergeLockRead(observation)
		require.True(t, root.MergeExecution(ExecutionSummary{AttemptCount: 1}))
		at = at.Add(time.Millisecond)
	}

	summary, snapshot := root.SealWithLockRead(uint64(2 * time.Millisecond))
	diagnostic := snapshot.Diagnostic(uuid.Nil, summary.AttemptCount, true)
	require.Equal(t, uint64(2), diagnostic.AttemptCount)
	require.Equal(t, uint64(2*time.Millisecond), diagnostic.AcquisitionNS)
	require.Equal(t, "acquisition", diagnostic.LastIncomplete)
	require.Equal(t, "failed", diagnostic.Outcome)
	require.Equal(t, hex.EncodeToString(first), diagnostic.ObservedConflictTxnID)
}

func TestLockReadObservationSaturatesAndRetainsFailedLeaf(t *testing.T) {
	ctx := context.Background()
	root := NewRoot(ConnExternal)
	at := time.Now()
	o := &LockReadObservation{}
	o.Record(ctx, LockReadPKReadiness, at, at.Add(time.Millisecond), context.Canceled)
	o.Record(ctx, LockReadPKCheck, at, at.Add(2*time.Millisecond), context.Canceled)
	for range 3 {
		o.Record(ctx, LockReadAcquisition, at, at.Add(math.MaxInt64), nil)
	}
	root.MergeLockRead(o)
	_, snapshot := root.SealWithLockRead(1)
	d := snapshot.Diagnostic(uuid.Nil, 0, true)
	require.Equal(t, uint64(math.MaxUint64), d.AcquisitionNS)
	require.Equal(t, "pk_readiness", d.LastIncomplete)
}
