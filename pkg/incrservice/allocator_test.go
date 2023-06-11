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
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/stretchr/testify/require"
)

func TestAlloc(t *testing.T) {
	runAllocatorTests(
		t,
		func(a valueAllocator) {
			ctx, cancel := context.WithCancel(context.Background())
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
				from, to, err := a.allocate(ctx, 0, c.key, c.count, nil)
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
			ctx, cancel := context.WithCancel(context.Background())
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
					func(from, to uint64, err error) {
						defer wg.Done()
						require.NoError(t, err)
						require.Equal(t, c.expectFrom, from)
						require.Equal(t, c.expectTo, to)
					})
				wg.Wait()
			}
		})
}

func runAllocatorTests(
	t *testing.T,
	fn func(valueAllocator)) {
	defer leaktest.AfterTest(t)()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	a := newValueAllocator(nil)
	defer a.close()
	fn(a)
}
