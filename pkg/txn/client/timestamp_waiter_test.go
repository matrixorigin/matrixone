// Copyright 2023 Matrix Origin
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

package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetTimestamp(t *testing.T) {
	runTimestampWaiterTests(
		t,
		func(tw *timestampWaiter) {
			ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
			defer cancel()

			for i := int64(1); i < 100; i++ {
				ts := newTestTimestamp(i)
				tw.latestTS.Store(&ts)
				v, err := tw.GetTimestamp(ctx, newTestTimestamp(i))
				require.NoError(t, err)
				assert.Equal(t, ts.Next(), v)
			}
		},
	)
}

func TestGetTimestampWithWaitTimeout(t *testing.T) {
	runTimestampWaiterTests(
		t,
		func(tw *timestampWaiter) {
			timeout := time.Millisecond * 100
			ctx, cancel := context.WithTimeout(context.TODO(), timeout)
			defer cancel()

			_, err := tw.GetTimestamp(ctx, newTestTimestamp(10))
			require.Error(t, err)
		},
	)
}

func TestGetTimestampWithNotified(t *testing.T) {
	runTimestampWaiterTests(
		t,
		func(tw *timestampWaiter) {
			timeout := time.Second * 10
			ctx, cancel := context.WithTimeout(context.TODO(), timeout)
			defer cancel()

			c := make(chan struct{})
			go func() {
				defer close(c)
				tw.NotifyLatestCommitTS(newTestTimestamp(10))
			}()
			<-c
			ts, err := tw.GetTimestamp(ctx, newTestTimestamp(10))
			require.NoError(t, err)
			v := newTestTimestamp(10)
			assert.Equal(t, v.Next(), ts)
		},
	)
}

func TestNotifyWaiters(t *testing.T) {
	tw := &timestampWaiter{}
	var values []*waiter
	values = append(values, tw.addToWait(newTestTimestamp(1)))
	values = append(values, tw.addToWait(newTestTimestamp(6)))
	values = append(values, tw.addToWait(newTestTimestamp(3)))
	values = append(values, tw.addToWait(newTestTimestamp(2)))
	values = append(values, tw.addToWait(newTestTimestamp(5)))

	var wg sync.WaitGroup
	for _, w := range values {
		wg.Add(1)
		go func(w *waiter) {
			defer wg.Done()
			w.wait(context.Background())
		}(w)
	}
	tw.notifyWaiters(newTestTimestamp(4))
	assert.Equal(t, 2, len(tw.mu.waiters))
	assert.Equal(t, newTestTimestamp(6), tw.mu.waiters[0].waitAfter)
	assert.Equal(t, newTestTimestamp(5), tw.mu.waiters[1].waitAfter)

	tw.notifyWaiters(newTestTimestamp(7))
	wg.Wait()
	assert.Equal(t, 0, len(tw.mu.waiters))
}

func BenchmarkGetTimestampWithWaitNotify(b *testing.B) {
	runTimestampWaiterTests(
		b,
		func(tw *timestampWaiter) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var wg sync.WaitGroup
			wg.Add(1)
			c := make(chan struct{})
			timer := time.NewTicker(time.Nanosecond)
			defer timer.Stop()
			go func() {
				defer wg.Done()
				var v int64
				for {
					select {
					case <-timer.C:
						tw.NotifyLatestCommitTS(newTestTimestamp(v))
						v++
					case <-c:
						return
					}
				}
			}()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ts := newTestTimestamp(int64(i))
				v, err := tw.GetTimestamp(ctx, ts)
				if err != nil {
					panic(err)
				}
				if v.LessEq(ts) {
					panic(v)
				}
			}
			close(c)
			wg.Wait()
		},
	)
}

func BenchmarkGetTimestampWithNoWait(b *testing.B) {
	runTimestampWaiterTests(
		b,
		func(tw *timestampWaiter) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			tw.NotifyLatestCommitTS(newTestTimestamp(1))
			ts := newTestTimestamp(0)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v, err := tw.GetTimestamp(ctx, ts)
				if err != nil {
					panic(err)
				}
				if v.LessEq(ts) {
					panic(v)
				}
			}
		},
	)
}

func newTestTimestamp(v int64) timestamp.Timestamp {
	return timestamp.Timestamp{
		PhysicalTime: v,
	}
}

func runTimestampWaiterTests(
	t testing.TB,
	fn func(*timestampWaiter)) {
	defer leaktest.AfterTest(t)()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	tw := NewTimestampWaiter()
	defer tw.Close()
	fn(tw.(*timestampWaiter))
}
