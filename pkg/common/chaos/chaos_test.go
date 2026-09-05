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

package chaos

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type lifecycleTestTester struct {
	startCount atomic.Int32
	stopCount  atomic.Int32
}

func (t *lifecycleTestTester) name() string { return "test" }

func (t *lifecycleTestTester) start() error {
	t.startCount.Add(1)
	return nil
}

func (t *lifecycleTestTester) stop() error {
	t.stopCount.Add(1)
	return nil
}

func newTestChaosTester(t tester) *ChaosTester {
	return &ChaosTester{
		testers:   []tester{t},
		stopC:     make(chan struct{}),
		startDone: make(chan struct{}),
	}
}

func TestChaosTesterStopBeforeBootstrapPreventsStart(t *testing.T) {
	tester := &lifecycleTestTester{}
	chaosTester := newTestChaosTester(tester)
	require.NoError(t, chaosTester.Stop())
	require.NoError(t, chaosTester.startWith(func() bool { return true }))
	select {
	case <-chaosTester.startDone:
	case <-time.After(time.Second):
		require.FailNow(t, "chaos tester did not finish after stop")
	}
	require.Zero(t, tester.startCount.Load())
	require.NoError(t, chaosTester.Stop())
	require.Zero(t, tester.stopCount.Load())
}

func TestChaosTesterStartAndStopAreExactlyOnce(t *testing.T) {
	tester := &lifecycleTestTester{}
	chaosTester := newTestChaosTester(tester)
	require.NoError(t, chaosTester.startWith(func() bool { return true }))
	// The production Start waits for bootstrap asynchronously. The injected
	// wait function makes the test deterministic without opening a database.
	select {
	case <-chaosTester.startDone:
	case <-time.After(time.Second):
		require.FailNow(t, "chaos tester did not start")
	}
	// Start is intentionally idempotent; the second call must not create a
	// second tester or goroutine.
	require.NoError(t, chaosTester.startWith(func() bool { return true }))
	require.NoError(t, chaosTester.Stop())
	require.NoError(t, chaosTester.Stop())
	require.Equal(t, int32(1), tester.startCount.Load())
	require.Equal(t, int32(1), tester.stopCount.Load())
}
