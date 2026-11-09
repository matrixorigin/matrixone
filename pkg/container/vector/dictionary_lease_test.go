// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"math"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

type dictionaryLeaseFixture struct {
	mp           *mpool.MPool
	source, dict *Vector
	base         *DictionaryBase
	rows         *RowSelection
}

func newDictionaryLeaseFixture(t *testing.T) *dictionaryLeaseFixture {
	t.Helper()
	f := &dictionaryLeaseFixture{mp: mpool.MustNewZero()}
	t.Cleanup(func() {
		if f.dict != nil {
			f.dict.Free(f.mp)
		}
		f.dropCallers()
		require.Zero(t, f.mp.CurrNB())
	})
	f.source = NewVec(types.T_int64.ToType())
	require.NoError(t, AppendFixedList(f.source, []int64{11, 22, 33}, nil, f.mp))
	var err error
	f.base, err = FreezeVector(f.source, f.mp, nil)
	require.NoError(t, err)
	f.rows, err = NewRowSelection([]int64{2, 0, 2}, 3, f.mp, nil)
	require.NoError(t, err)
	f.dict, err = NewDictionaryVector(f.base, f.rows, f.mp, nil)
	require.NoError(t, err)
	return f
}

func (f *dictionaryLeaseFixture) dropCallers() {
	if f.rows != nil {
		f.rows.Release()
		f.rows = nil
	}
	if f.base != nil {
		f.base.Release()
		f.base = nil
	}
	if f.source != nil {
		f.source.Free(f.mp)
		f.source = nil
	}
}

func TestDictionaryBackingRetainedHolder(t *testing.T) {
	f := newDictionaryLeaseFixture(t)
	backing := f.dict.dictionaryState()
	require.Same(t, backing, f.dict.dataLease)
	retained := f.dict.DictionaryRetainedBytes()
	require.Positive(t, retained)
	require.Equal(t, retained, f.dict.BorrowedAccountedBytes(), "composite backing is counted once")
	require.Nil(t, backing.Bytes(), "mapping has no contiguous byte payload")
	require.True(t, backing.Retain())
	held := true
	t.Cleanup(func() {
		if held {
			backing.Release()
		}
	})
	f.dropCallers()
	f.dict.Free(f.mp)
	require.Positive(t, f.mp.CurrNB(), "retained descriptor keeps physical allocations live")
	require.Equal(t, int64(33), GetFixedAtNoTypeCheck[int64](backing.base.vec, int(backing.rows.storageFixed()[0])))
	require.Equal(t, retained, backing.AccountedBytes())
	backing.Release()
	held = false
	require.Zero(t, f.mp.CurrNB())
	require.False(t, backing.Retain())
	require.Equal(t, retained, backing.AccountedBytes(), "immutable accounting survives release")
	require.Panics(t, backing.Release, "over-release is an ownership violation")
}

func TestDictionaryBackingTerminalPaths(t *testing.T) {
	for _, path := range []string{"free", "reset", "flatten"} {
		t.Run(path, func(t *testing.T) {
			f := newDictionaryLeaseFixture(t)
			backing := f.dict.dictionaryState()
			switch path {
			case "free":
				f.dict.Free(f.mp)
				f.dict.Free(f.mp)
			case "reset":
				f.dict.ResetWithSameType()
				f.dict.ResetWithSameType()
			case "flatten":
				require.NoError(t, f.dict.EnsureWritable(f.mp))
				require.Equal(t, []int64{33, 11, 33}, MustFixedColWithTypeCheck[int64](f.dict))
			}
			require.False(t, f.dict.IsDictionary())
			require.Nil(t, f.dict.dataLease)
			require.False(t, backing.Retain(), "descriptor cannot resurrect while child handles still exist")
			require.True(t, f.base.Retain())
			f.base.Release()
			require.True(t, f.rows.Retain())
			f.rows.Release()
		})
	}
}

func TestDictionaryBackingReferenceBoundaries(t *testing.T) {
	var missing *dictionaryVector
	require.False(t, missing.Retain())
	require.Zero(t, missing.AccountedBytes())
	require.Nil(t, missing.Bytes())
	require.Panics(t, missing.Release)
	f := newDictionaryLeaseFixture(t)
	backing := f.dict.dictionaryState()
	backing.refs.Store(math.MaxInt64)
	defer backing.refs.Store(1)
	require.False(t, backing.Retain(), "reference count must not overflow")
}

func TestDictionaryBackingConcurrentHolders(t *testing.T) {
	f := newDictionaryLeaseFixture(t)
	backing := f.dict.dictionaryState()
	f.dropCallers()
	start := make(chan struct{})
	var release sync.Once
	releaseWorkers := func() { release.Do(func() { close(start) }) }
	var done sync.WaitGroup
	t.Cleanup(func() { releaseWorkers(); done.Wait() })
	for worker := 0; worker < 8; worker++ {
		require.True(t, backing.Retain())
		done.Add(1)
		go func() {
			defer done.Done()
			defer backing.Release()
			<-start
			for iteration := 0; iteration < 64; iteration++ {
				if !backing.Retain() {
					t.Error("live descriptor refused a retain")
					return
				}
				value := GetFixedAtNoTypeCheck[int64](backing.base.vec, int(backing.rows.storageFixed()[0]))
				if value != 33 {
					t.Errorf("immutable payload changed: %d", value)
				}
				_ = backing.AccountedBytes()
				backing.Release()
			}
		}()
	}
	releaseWorkers()
	f.dict.Free(f.mp)
	done.Wait()
	require.False(t, backing.Retain())
	require.Zero(t, f.mp.CurrNB())
}
