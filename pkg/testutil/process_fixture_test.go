// Copyright 2021 Matrix Origin
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

package testutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/require"
	goruntime "runtime"
	"testing"
	"time"
)

type fixtureProbeTB struct {
	testing.TB
	dirs    int
	panicAt int
	goexit  bool
	cause   any
}

func (tb *fixtureProbeTB) TempDir() string {
	tb.dirs++
	if tb.dirs == tb.panicAt {
		if tb.goexit {
			goruntime.Goexit()
		}
		panic(tb.cause)
	}
	return tb.TB.TempDir()
}

type fixtureBorrowedFS struct {
	fileservice.FileService
	closes int
}

func (fs *fixtureBorrowedFS) Close(context.Context) { fs.closes++ }

type fixtureBorrowedQueryClient struct {
	client.QueryClient
	closes int
}

func (qc *fixtureBorrowedQueryClient) Close() error { qc.closes++; return nil }

func fixturePoolCount(t *testing.T, tag string) int {
	t.Helper()
	var pools []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(mpool.ReportMemUsage(tag)), &pools))
	return len(pools)
}

func TestProcessFixtureOwnedLifetimes(t *testing.T) {
	ensureAutoIncrService("")
	const tag = "must_new_zero_no_fixed"
	before := fixturePoolCount(t, tag)
	for round := 0; round < 3; round++ {
		var mp *mpool.MPool
		require.True(t, t.Run(fmt.Sprintf("round_%d", round), func(t *testing.T) {
			proc := NewProcess(t)
			mp = proc.Mp()
			require.Equal(t, before+1, fixturePoolCount(t, tag))
			require.Equal(t, int64(1<<20), proc.GetLim().Size)
			require.Equal(t, time.Local, proc.Base.SessionInfo.TimeZone)
			for _, offHeap := range []bool{true, false} {
				block, err := mp.Alloc(1, offHeap)
				require.NoError(t, err)
				t.Cleanup(func() {
					defer mp.Free(block)
					require.Equal(t, before+1, fixturePoolCount(t, tag), "caller cleanup must precede pool deletion")
				})
			}
		}))
		require.Equal(t, before, fixturePoolCount(t, tag))
		require.Zero(t, mp.CurrNB())
		require.Zero(t, mp.OnHeapCurrNB())
	}
}

func TestProcessFixtureBorrowedDependencies(t *testing.T) {
	ensureAutoIncrService("")
	const tag = "fixture-borrowed-pool"
	before := fixturePoolCount(t, tag)
	t.Cleanup(func() { require.Equal(t, before, fixturePoolCount(t, tag)) })
	mp := mpool.MustNew(tag)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	defaultBefore := fixturePoolCount(t, "must_new_zero_no_fixed")
	fs1, fs2 := &fixtureBorrowedFS{}, &fixtureBorrowedFS{}
	qc1, qc2 := &fixtureBorrowedQueryClient{}, &fixtureBorrowedQueryClient{}
	var dirs int
	require.True(t, t.Run("options", func(t *testing.T) {
		tb := &fixtureProbeTB{TB: t}
		proc := NewProcess(tb, WithMPool(nil), WithMPool(mp), WithFileService(fs1), WithFileService(fs2), WithQueryClient(qc1), WithQueryClient(qc2))
		require.Same(t, mp, proc.Mp())
		require.Same(t, fs2, proc.GetFileService())
		require.Same(t, qc2, proc.Base.QueryClient)
		dirs = tb.dirs
		require.Equal(t, defaultBefore, fixturePoolCount(t, "must_new_zero_no_fixed"))
	}))
	require.Zero(t, dirs)
	require.Zero(t, fs1.closes)
	require.Zero(t, fs2.closes)
	require.Zero(t, qc1.closes)
	require.Zero(t, qc2.closes)
	require.Equal(t, 1, fixturePoolCount(t, tag))
	block, err := mp.Alloc(1, true)
	require.NoError(t, err)
	mp.Free(block)
	for _, name := range []string{"supplied-pool-first", "supplied-pool-second"} {
		require.True(t, t.Run(name, func(t *testing.T) {
			require.Equal(t, before+1, fixturePoolCount(t, tag))
			proc := NewProcessWithMPool(t, "", mp)
			require.Same(t, mp, proc.Mp())
		}))
		require.Equal(t, before+1, fixturePoolCount(t, tag), "child cleanup must preserve the parent-owned pool")
	}
	require.True(t, t.Run("explicit-nil", func(t *testing.T) {
		tb := &fixtureProbeTB{TB: t}
		proc := NewProcess(tb, WithMPool(nil), WithFileService(nil))
		require.Nil(t, proc.Mp())
		require.Nil(t, proc.GetFileService())
		require.Zero(t, tb.dirs)
		require.Equal(t, defaultBefore, fixturePoolCount(t, "must_new_zero_no_fixed"))
	}))
}

func TestProcessFixtureOwnedMPool(t *testing.T) {
	ensureAutoIncrService("")
	const tag = "must_new_zero"
	before := fixturePoolCount(t, tag)
	require.True(t, t.Run("owned", func(t *testing.T) {
		proc := NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
		mp := proc.Mp()
		require.Equal(t, before+1, fixturePoolCount(t, tag))

		blocks := make([][]byte, 0, 2)
		for _, offHeap := range []bool{false, true} {
			block, err := mp.Alloc(1, offHeap)
			require.NoError(t, err)
			blocks = append(blocks, block)
		}
		t.Cleanup(func() {
			for _, block := range blocks {
				mp.Free(block)
			}
			require.Equal(t, before+1, fixturePoolCount(t, tag), "caller cleanup must precede owned pool deletion")
		})
	}))
	require.Equal(t, before, fixturePoolCount(t, tag))
}

func TestProcessFixtureManualLifetime(t *testing.T) {
	ensureAutoIncrService("")
	const tag = "must_new_zero_no_fixed"
	before := fixturePoolCount(t, tag)
	var mp *mpool.MPool
	func() {
		proc := NewProcess(nil)
		mp = proc.Mp()
		defer mpool.DeleteMPool(mp)
		defer proc.GetFileService().Close(context.Background())
		defer proc.Free()
		require.Equal(t, before+1, fixturePoolCount(t, tag))
		proc.Free()
		require.Equal(t, before+1, fixturePoolCount(t, tag))
	}()
	require.Equal(t, before, fixturePoolCount(t, tag))
	require.Zero(t, mp.CurrNB())
	require.Zero(t, mp.OnHeapCurrNB())
}

func TestProcessFixtureConstructionFailure(t *testing.T) {
	ensureAutoIncrService("")
	const tag = "must_new_zero_no_fixed"
	for _, at := range []int{2, 3} {
		t.Run(fmt.Sprintf("panic-child_%d", at), func(t *testing.T) {
			before := fixturePoolCount(t, tag)
			cause := errors.New("fixture construction failed")
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_ = NewProcess(&fixtureProbeTB{TB: t, panicAt: at, cause: cause})
			}()
			require.Same(t, cause, recovered)
			require.Equal(t, before, fixturePoolCount(t, tag), "rollback must run before test cleanup")
		})
	}
	t.Run("goexit", func(t *testing.T) {
		before := fixturePoolCount(t, tag)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = NewProcess(&fixtureProbeTB{TB: t, panicAt: 2, goexit: true})
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("construction Goexit did not finish")
		}
		require.Equal(t, before, fixturePoolCount(t, tag))
	})
}
