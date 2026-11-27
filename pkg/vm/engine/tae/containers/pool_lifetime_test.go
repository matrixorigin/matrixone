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

package containers

import (
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestVectorPoolClosePreservesBorrowers(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	pool := NewVectorPool(t.Name(), 2, WithMPool(mp))
	other := NewVectorPool(t.Name()+"-other", 2, WithMPool(mp))
	fixedType, bytesType := types.T_int64.ToType(), types.T_varchar.ToType()
	idle := pool.GetVector(&fixedType)
	idle.MustAppendValue(int64(42), false)
	ReleaseVector(idle)
	held := pool.GetVector(&bytesType)
	held.MustAppendValue([]byte("still valid"), false)
	independent := other.GetVector(&fixedType)
	independent.MustAppendValue(int64(17), false)
	before := mp.CurrNB()

	pool.Close()
	pool.Close()
	require.Less(t, mp.CurrNB(), before)
	require.Equal(t, []byte("still valid"), held.GetValue(0))
	require.Equal(t, int64(17), independent.GetValue(0))
	ReleaseVector(held)
	ReleaseVector(independent)
	other.Close()
	require.Zero(t, mp.CurrNB())

	// A caller racing with or arriving after Close gets an ordinary, unpooled
	// vector whose lifetime remains independent of the closed pool.
	after := pool.GetVector(&fixedType)
	after.MustAppendValue(int64(9), false)
	require.Equal(t, int64(9), after.GetValue(0))
	ReleaseVector(after)
	require.Zero(t, mp.CurrNB())
}

func TestVectorPoolCloseConcurrentBorrowAndReturn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	pool := NewVectorPool(t.Name(), 16, WithMPool(mp))
	typ := types.T_int64.ToType()
	const workers, iterations = 16, 200
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	release := make(chan struct{})
	errCh := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer done.Done()
			held := pool.GetVector(&typ)
			held.MustAppendValue(int64(worker), false)
			ready.Done()
			<-release
			if got := held.GetValue(0); got != int64(worker) {
				errCh <- fmt.Errorf("held vector: got %v, want %d", got, worker)
				ReleaseVector(held)
				return
			}
			ReleaseVector(held)
			for i := 0; i < iterations; i++ {
				v := pool.GetVector(&typ)
				v.MustAppendValue(int64(i), false)
				if got := v.GetValue(0); got != int64(i) {
					errCh <- fmt.Errorf("vector: got %v, want %d", got, i)
					ReleaseVector(v)
					return
				}
				ReleaseVector(v)
			}
		}(worker)
	}
	ready.Wait()
	closed := make(chan struct{})
	go func() {
		pool.Close()
		close(closed)
	}()
	close(release)
	done.Wait()
	<-closed
	pool.Close()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	require.Zero(t, mp.CurrNB())
}
