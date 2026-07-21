// Copyright 2026 Matrix Origin
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

package txnbase

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestTryUpdateMaxCommittedTSNeverMovesBackward(t *testing.T) {
	mgr := &TxnManager{}
	mgr.initMaxCommittedTS()

	newer := types.BuildTS(2, 0)
	older := types.BuildTS(1, 0)
	mgr.TryUpdateMaxCommittedTS(newer)
	mgr.TryUpdateMaxCommittedTS(older)

	require.Equal(t, newer, *mgr.MaxCommittedTS.Load())
}

func TestTryUpdateMaxCommittedTSConcurrent(t *testing.T) {
	mgr := &TxnManager{}
	mgr.initMaxCommittedTS()

	const updates = 100
	var wg sync.WaitGroup
	for i := 1; i <= updates; i++ {
		ts := types.BuildTS(int64(i), 0)
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.TryUpdateMaxCommittedTS(ts)
		}()
	}
	wg.Wait()

	require.Equal(t, types.BuildTS(updates, 0), *mgr.MaxCommittedTS.Load())
}

func TestAllocateAndPublishCommitTSSerializesPublication(t *testing.T) {
	mgr := NewTxnManager(nil, nil, types.NewMockHLCClock(1))
	defer mgr.workers.Release()

	firstStarted := make(chan types.TS, 1)
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := mgr.AllocateAndPublishCommitTS(func(ts types.TS) error {
			firstStarted <- ts
			<-releaseFirst
			return nil
		})
		firstDone <- err
	}()

	firstTS := <-firstStarted
	require.True(t, mgr.MaxCommittedTS.Load().LT(&firstTS))

	secondStarted := make(chan types.TS, 1)
	secondDone := make(chan error, 1)
	go func() {
		_, err := mgr.AllocateAndPublishCommitTS(func(ts types.TS) error {
			secondStarted <- ts
			return nil
		})
		secondDone <- err
	}()

	select {
	case <-secondStarted:
		t.Fatal("later timestamp allocated before earlier state was published")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseFirst)
	require.NoError(t, <-firstDone)
	secondTS := <-secondStarted
	require.True(t, secondTS.GT(&firstTS))
	require.NoError(t, <-secondDone)
	require.Equal(t, secondTS, *mgr.MaxCommittedTS.Load())
}

func TestAllocateAndPublishCommitTSErrorDoesNotPublish(t *testing.T) {
	mgr := NewTxnManager(nil, nil, types.NewMockHLCClock(1))
	defer mgr.workers.Release()

	publishErr := errors.New("publish failed")
	ts, err := mgr.AllocateAndPublishCommitTS(func(types.TS) error {
		return publishErr
	})
	require.ErrorIs(t, err, publishErr)
	require.True(t, mgr.MaxCommittedTS.Load().LT(&ts))
}
