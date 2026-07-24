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

package incrservice

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
)

func TestAlloc(t *testing.T) {
	runAllocatorTests(
		t,
		func(a valueAllocator) {
			ctx, cancel := context.WithCancel(defines.AttachAccountId(context.Background(), catalog.System_Account))
			defer cancel()

			cases := []struct {
				key        string
				count      int
				step       int
				expectFrom uint64
				expectTo   uint64
			}{
				{
					key:        "k1",
					count:      1,
					step:       1,
					expectFrom: 1,
					expectTo:   2,
				},
				{
					key:        "k1",
					count:      1,
					step:       1,
					expectFrom: 2,
					expectTo:   3,
				},
				{
					key:        "k1",
					count:      2,
					step:       1,
					expectFrom: 3,
					expectTo:   5,
				},
				{
					key:        "k2",
					count:      2,
					step:       3,
					expectFrom: 3,
					expectTo:   9,
				},
				{
					key:        "k2",
					count:      2,
					step:       3,
					expectFrom: 9,
					expectTo:   15,
				},
			}

			for _, c := range cases {
				require.NoError(t,
					a.(*allocator).store.Create(
						ctx,
						0,
						[]AutoColumn{
							{
								ColName: c.key,
								Offset:  0,
								Step:    uint64(c.step),
							},
						},
						nil))
				from, to, _, err := a.allocate(ctx, 0, c.key, c.count, nil)
				require.NoError(t, err)
				require.Equal(t, c.expectFrom, from)
				require.Equal(t, c.expectTo, to)
			}
		})
}

func TestAsyncAlloc(t *testing.T) {
	runAllocatorTests(
		t,
		func(a valueAllocator) {
			ctx, cancel := context.WithCancel(defines.AttachAccountId(context.Background(), catalog.System_Account))
			defer cancel()

			cases := []struct {
				key        string
				count      int
				step       int
				expectFrom uint64
				expectTo   uint64
			}{
				{
					key:        "k1",
					count:      1,
					step:       1,
					expectFrom: 1,
					expectTo:   2,
				},
				{
					key:        "k1",
					count:      1,
					step:       1,
					expectFrom: 2,
					expectTo:   3,
				},
				{
					key:        "k1",
					count:      2,
					step:       1,
					expectFrom: 3,
					expectTo:   5,
				},
				{
					key:        "k2",
					count:      2,
					step:       3,
					expectFrom: 3,
					expectTo:   9,
				},
				{
					key:        "k2",
					count:      2,
					step:       3,
					expectFrom: 9,
					expectTo:   15,
				},
			}

			for _, c := range cases {
				require.NoError(t,
					a.(*allocator).store.Create(
						ctx,
						0,
						[]AutoColumn{
							{
								ColName: c.key,
								Offset:  0,
								Step:    uint64(c.step),
							},
						},
						nil))
				var wg sync.WaitGroup
				wg.Add(1)
				a.asyncAllocate(
					ctx,
					0,
					c.key,
					c.count,
					nil,
					func(from, to uint64, allocateAt timestamp.Timestamp, err error) {
						defer wg.Done()
						require.NoError(t, err)
						require.Equal(t, c.expectFrom, from)
						require.Equal(t, c.expectTo, to)
					})
				wg.Wait()
			}
		})
}

func TestAllocatorEnqueueHoldsCloseLockUntilSendCompletes(t *testing.T) {
	a := &allocator{c: make(chan action, 1), done: make(chan struct{})}
	a.c <- action{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- a.enqueue(ctx, action{}) }()

	require.Eventually(t, func() bool {
		if a.mu.TryLock() {
			a.mu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}

func TestAllocatorCloseDrainsAcceptedActions(t *testing.T) {
	runAllocatorTests(t, func(va valueAllocator) {
		a := va.(*allocator)
		ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
		require.NoError(t, a.store.Create(ctx, 0, []AutoColumn{{ColName: "auto", Step: 1}}, nil))
		var completed atomic.Int64
		for range 100 {
			require.NoError(t, a.asyncAllocate(ctx, 0, "auto", 1, nil,
				func(uint64, uint64, timestamp.Timestamp, error) { completed.Add(1) }))
		}
		a.close()
		require.Equal(t, int64(100), completed.Load())
	})
}

func runAllocatorTests(
	t *testing.T,
	fn func(valueAllocator),
) {
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			defer leaktest.AfterTest(t)()
			a := newValueAllocator(sid, nil)
			defer a.close()
			fn(a)
		},
	)
}
