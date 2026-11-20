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

package objectio

import (
	"bytes"
	"runtime"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// A merge object writer may need a large serialization buffer while active,
// but that capacity must not be retained after its arena returns to the pool.
func TestMergeWriterArenaSerialRetention(t *testing.T) {
	drainArenaPools()
	t.Cleanup(drainArenaPools)

	const retainLimit = 16 << 20
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	vec := vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { vec.Free(mp) })
	require.NoError(t, vector.AppendBytes(vec, bytes.Repeat([]byte("x"), retainLimit+1), false, mp))
	bat := batch.NewWithSize(1)
	bat.SetVector(0, vec)
	bat.SetRowCount(1)

	arena := NewArena(0)
	arena.sizeLimit = arenaMaxSize
	t.Cleanup(arena.FreeBuffers)
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	id := NewObjectid()
	writer, err := NewObjectWriter(BuildObjectNameWithObjectID(&id), fs, 0, []uint16{0}, arena)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	require.Greater(t, arena.serialBuf.Cap(), retainLimit, "the real writer must grow a large buffer")

	PutArena(arena)
	got := GetArena(ArenaLarge)
	require.Same(t, arena, got)
	require.LessOrEqual(t, got.serialBuf.Cap(), retainLimit, "pooled arena must not retain a large writer buffer")
	require.Zero(t, got.serialBuf.Len())
}

func TestWriteArenaSerialBufferLifecycle(t *testing.T) {
	arena := NewArena(0)
	t.Cleanup(arena.FreeBuffers)
	arena.serialBuf.Grow(32 << 20)
	largeCap := arena.serialBuf.Cap()
	require.Greater(t, largeCap, arenaSerialRetainLimit)

	// Keep an active writer's scratch space across similarly sized blocks.
	arena.serialPeak = 24 << 20
	arena.Reset()
	require.Equal(t, largeCap, arena.serialBuf.Cap())

	// Once the next cycle uses only a small column, release the old peak.
	arena.serialPeak = 1 << 10
	arena.Reset()
	require.Zero(t, arena.serialBuf.Cap())
	require.Zero(t, arena.serialPeak)

	arena.serialBuf.Grow(32 << 20)
	arena.FreeBuffers()
	require.Zero(t, arena.serialBuf.Cap())
}

func TestArenaPooledSerialGaugeConcurrent(t *testing.T) {
	drainArenaPools()
	t.Cleanup(drainArenaPools)
	require.Zero(t, promtestutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge))

	const workers, rounds = 8, 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < rounds; j++ {
				arena := GetArena(ArenaSmall)
				arena.serialBuf.Grow(1024)
				PutArena(arena)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	close(start)
	minimum := float64(0)
	for {
		select {
		case <-done:
			require.GreaterOrEqual(t, minimum, float64(0), "a concurrent pop must not make the gauge negative")
			drainArenaPools()
			require.Zero(t, promtestutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge))
			return
		default:
			if pooled := promtestutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge); pooled < minimum {
				minimum = pooled
			}
			runtime.Gosched()
		}
	}
}
