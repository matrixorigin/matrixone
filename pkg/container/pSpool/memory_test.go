// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pSpool

import (
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"math"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpoolMetadataBoundIncludesEveryReceiverQueue(t *testing.T) {
	for _, receivers := range []uint32{1, 2, 3, 16, 17, 16384, 16385} {
		bound, ok := MemoryMetadataBound(receivers)
		require.True(t, ok)
		queue := nextPowerOfTwo(max(uint32(2), receivers))
		require.GreaterOrEqual(t, bound, int64(receivers)*int64(queue)*4)
	}
	before, ok := MemoryMetadataBound(16384)
	require.True(t, ok)
	after, ok := MemoryMetadataBound(16385)
	require.True(t, ok)
	require.Greater(t, after, before, "the queue capacity doubles just above a power-of-two fanout")
	_, ok = MemoryMetadataBound(math.MaxUint32)
	require.False(t, ok, "uint32 queue-capacity wrap is not an executable layout")
}

// Match the constructor's separate allocation classes, including the scratch
// arrays needed even by the single-receiver Abort path.
func TestSpoolMetadataCoversConstructedArrays(t *testing.T) {
	for _, receivers := range []uint32{1, 2, 3, 16, 17, 33} {
		mp := mpool.MustNewZeroNoFixed()
		sp := InitMyPipelineSpool(mp, receivers)
		bound, ok := MemoryMetadataBound(receivers)
		require.True(t, ok)
		var actual uint64
		add := func(bytes uintptr, scanned bool) {
			if scanned {
				bytes += 8
			}
			rounded, valid := malloc.ClassAllocationSize(uint64(bytes))
			require.True(t, valid)
			actual += rounded
		}
		add(reflect.TypeFor[PipelineSpool]().Size(), true)
		add(reflect.TypeFor[cachedBatch]().Size(), true)
		add(reflect.TypeFor[spoolBuffer]().Size(), true)
		add(uintptr(cap(sp.shardPool))*reflect.TypeFor[pipelineSpoolMessage]().Size(), true)
		add(uintptr(cap(sp.doRefCheck)), false)
		add(uintptr(cap(sp.shardRefs))*reflect.TypeFor[atomic.Int32]().Size(), false)
		add(uintptr(cap(sp.abortCurrentRefs))*reflect.TypeFor[int32]().Size(), false)
		add(uintptr(cap(sp.rs))*reflect.TypeFor[receiver]().Size(), true)
		add(uintptr(cap(sp.cache.buffer.readyToUse))*reflect.TypeFor[readyToUseItem]().Size(), true)
		add(uintptr(cap(sp.cache.buffer.bytesCache))*reflect.TypeFor[oneBatchMemoryCache]().Size(), true)
		for _, r := range sp.rs {
			add(uintptr(cap(r.elements))*reflect.TypeFor[uint32]().Size(), false)
		}
		for range cap(sp.shardPool) {
			add(reflect.TypeFor[batch.Batch]().Size(), true)
		}
		add(256+uintptr(cap(sp.freeShardPool))*4, false)
		add(256, false)
		require.GreaterOrEqual(t, uint64(bound), actual)
		sp.CloseAfterReadersStopped()
		mpool.DeleteMPool(mp)
	}
}
