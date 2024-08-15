// Copyright 2024 Matrix Origin
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

package mergesort

import (
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/mocks"
	"github.com/stretchr/testify/require"
)

type testPool struct {
	pool *containers.VectorPool
}

func (p *testPool) GetVector(typ *types.Type) (ret *vector.Vector, release func()) {
	v := p.pool.GetVector(typ)
	return v.GetDownstreamVector(), v.Close
}

func (p *testPool) GetMPool() *mpool.MPool {
	return p.pool.GetMPool()
}

func TestReshapeBatches(t *testing.T) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	batchSize := 5
	fromLayout := []uint32{2000, 4000, 6000}
	toLayout := []uint32{8192, 3808}

	batches := make([]*containers.Batch, len(fromLayout))
	for i := range fromLayout {
		batches[i] = containers.NewBatch()
		for j := 0; j < batchSize; j++ {
			vec := containers.MakeVector(types.T_int32.ToType(), common.DefaultAllocator)
			for k := uint32(0); k < fromLayout[i]; k++ {
				vec.Append(int32(k), false)
			}
			batches[i].Vecs = append(batches[i].Vecs, vec)
			batches[i].Attrs = append(batches[i].Attrs, "")
		}
	}

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	retBatch, releaseF, _, err := ReshapeBatches(batches, toLayout, pool)
	require.NoError(t, err)
	runtime.ReadMemStats(&m2)
	t.Log("total:", m2.TotalAlloc-m1.TotalAlloc)
	t.Log("mallocs:", m2.Mallocs-m1.Mallocs)

	for i, v := range retBatch {
		require.Equal(t, toLayout[i], uint32(v.RowCount()))
	}
	releaseF()
}
