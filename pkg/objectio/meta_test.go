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

package objectio

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/assert"
)

func TestBuildMetaData(t *testing.T) {
	objectMeta := BuildMetaData(20, 30)
	for i := uint16(0); i < 20; i++ {
		blkMeta := objectMeta.GetBlockMeta(uint32(i))
		assert.Equal(t, i, blkMeta.BlockHeader().Sequence())
		assert.Equal(t, uint16(30), blkMeta.BlockHeader().ColumnCount())
	}
}

func TestDedupLoadCleansUpAfterPanic(t *testing.T) {
	oldMetaCache := metaCache
	metaCache = newMetaCache(fscache.ConstCapacity(1024))
	defer func() {
		metaCache = oldMetaCache
	}()

	var key mataCacheKey
	key[0] = 1
	started := make(chan struct{})
	release := make(chan struct{})
	panicDone := make(chan any, 1)

	go func() {
		defer func() {
			panicDone <- recover()
		}()
		_, _ = dedupLoad(context.Background(), key, func() ([]byte, error) {
			close(started)
			<-release
			panic("boom")
		})
	}()

	<-started
	waiterDone := make(chan error, 1)
	go func() {
		_, err := dedupLoad(context.Background(), key, func() ([]byte, error) {
			return nil, errors.New("unexpected waiter load")
		})
		waiterDone <- err
	}()

	time.Sleep(10 * time.Millisecond)
	close(release)
	assert.Equal(t, "boom", <-panicDone)
	err := <-waiterDone
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dedup load did not complete")

	metaLoadMu.Lock()
	_, ok := metaLoadCalls[key]
	metaLoadMu.Unlock()
	assert.False(t, ok)

	v, err := dedupLoad(context.Background(), key, func() ([]byte, error) {
		return []byte("ok"), nil
	})
	assert.NoError(t, err)
	assert.Equal(t, []byte("ok"), v)
}

func TestDedupLoadWaiterGetsSuccessfulOwnerValue(t *testing.T) {
	oldMetaCache := metaCache
	metaCache = newMetaCache(fscache.ConstCapacity(1024))
	defer func() {
		metaCache = oldMetaCache
	}()

	var key mataCacheKey
	key[0] = 8
	started := make(chan struct{})
	release := make(chan struct{})
	waiterDone := make(chan struct {
		val []byte
		err error
	}, 1)

	go func() {
		_, _ = dedupLoad(context.Background(), key, func() ([]byte, error) {
			close(started)
			<-release
			return []byte("ok"), nil
		})
	}()
	<-started

	go func() {
		v, err := dedupLoad(context.Background(), key, func() ([]byte, error) {
			return nil, errors.New("unexpected waiter load")
		})
		waiterDone <- struct {
			val []byte
			err error
		}{v, err}
	}()

	time.Sleep(10 * time.Millisecond)
	close(release)
	result := <-waiterDone
	assert.NoError(t, result.err)
	assert.Equal(t, []byte("ok"), result.val)
}

func TestDedupLoadWaiterTimeoutWhileOwnerStillLoading(t *testing.T) {
	oldMetaCache := metaCache
	metaCache = newMetaCache(fscache.ConstCapacity(1024))
	defer func() {
		metaCache = oldMetaCache
	}()

	var key mataCacheKey
	key[0] = 7
	started := make(chan struct{})
	release := make(chan struct{})
	ownerDone := make(chan error, 1)

	go func() {
		_, err := dedupLoad(context.Background(), key, func() ([]byte, error) {
			close(started)
			<-release
			return []byte("ok"), nil
		})
		ownerDone <- err
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := dedupLoad(ctx, key, func() ([]byte, error) {
		return nil, errors.New("unexpected waiter load")
	})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotContains(t, err.Error(), "dedup load did not complete")

	close(release)
	assert.NoError(t, <-ownerDone)
}

type dedupLoadWaiterContext struct {
	context.Context
	admitted chan struct{}
	once     sync.Once
}

const dedupLoadWaiterAdmissionTimeout = time.Second

// dedupLoad calls Done only after it finds an existing load call. Observing
// that call gives these tests a phase barrier without relying on scheduling.
func (c *dedupLoadWaiterContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.admitted) })
	return c.Context.Done()
}

func newDedupLoadWaiterContext() (*dedupLoadWaiterContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	return &dedupLoadWaiterContext{
		Context:  ctx,
		admitted: make(chan struct{}),
	}, cancel
}

func waitForDedupLoadWaiterAdmission(
	t *testing.T,
	waiterCtx *dedupLoadWaiterContext,
	waiterDone <-chan struct{},
	ownerDone <-chan struct{},
	releaseOwner func(),
	cancelWaiter context.CancelFunc,
) {
	t.Helper()

	timer := time.NewTimer(dedupLoadWaiterAdmissionTimeout)
	defer timer.Stop()

	select {
	case <-waiterCtx.admitted:
		return
	case <-waiterDone:
		cancelWaiter()
		releaseOwner()
		drainDedupLoadAfterAdmissionFailure(t, ownerDone, waiterDone)
		t.Fatalf("dedup load waiter completed before admission")
	case <-timer.C:
		cancelWaiter()
		releaseOwner()
		drainDedupLoadAfterAdmissionFailure(t, ownerDone, waiterDone)
		t.Fatalf("timed out waiting for dedup load waiter admission")
	}
}

func drainDedupLoadAfterAdmissionFailure(
	t *testing.T,
	ownerDone <-chan struct{},
	waiterDone <-chan struct{},
) {
	t.Helper()
	if !waitForDedupLoadCompletion(ownerDone) {
		t.Errorf("dedup load owner did not exit after admission failure")
	}
	if !waitForDedupLoadCompletion(waiterDone) {
		t.Errorf("dedup load waiter did not exit after admission failure")
	}
}

func waitForDedupLoadCompletion(done <-chan struct{}) bool {
	timer := time.NewTimer(dedupLoadWaiterAdmissionTimeout)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func TestEvictCacheToCapacityPercent(t *testing.T) {
	oldMetaCache := metaCache
	metaCache = newMetaCache(fscache.ConstCapacity(10))
	defer func() {
		metaCache = oldMetaCache
	}()

	ctx := context.Background()
	var key mataCacheKey
	key[0] = 3
	metaCache.Set(ctx, key, []byte("1234567890"), 10)

	used := EvictCacheToCapacityPercent(ctx, 50)

	assert.LessOrEqual(t, used, int64(5))
	assert.Equal(t, used, metaCache.Used())
}

func TestMetaCachePressureAdmissionSkipsWritesAboveTarget(t *testing.T) {
	oldMetaCache := metaCache
	metaCache = newMetaCache(fscache.ConstCapacity(10))
	clearMetaCachePressureTargetForTest()
	defer func() {
		clearMetaCachePressureTargetForTest()
		metaCache = oldMetaCache
	}()

	ctx := context.Background()
	var existingKey mataCacheKey
	existingKey[0] = 4
	metaCache.Set(ctx, existingKey, []byte("12345"), 5)

	SetMetaCachePressureTargetPercent(50, time.Now().Add(time.Minute))

	var key mataCacheKey
	key[0] = 5
	v, err := dedupLoad(ctx, key, func() ([]byte, error) {
		return []byte("6"), nil
	})
	assert.NoError(t, err)
	assert.Equal(t, []byte("6"), v)
	assert.Equal(t, int64(5), metaCache.Used())

	_, ok := metaCache.Get(ctx, key)
	assert.False(t, ok)
}

func TestMetaCachePressureAdmissionExpires(t *testing.T) {
	oldMetaCache := metaCache
	metaCache = newMetaCache(fscache.ConstCapacity(10))
	clearMetaCachePressureTargetForTest()
	defer func() {
		clearMetaCachePressureTargetForTest()
		metaCache = oldMetaCache
	}()

	ctx := context.Background()
	SetMetaCachePressureTargetPercent(50, time.Now().Add(-time.Second))

	var key mataCacheKey
	key[0] = 6
	v, err := dedupLoad(ctx, key, func() ([]byte, error) {
		return []byte("1"), nil
	})
	assert.NoError(t, err)
	assert.Equal(t, []byte("1"), v)
	assert.Equal(t, int64(1), metaCache.Used())
}
