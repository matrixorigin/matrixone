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

package process

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestNewBatchFromSrcReleasesPartiallyInitializedColumns(t *testing.T) {
	mp, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)
	proc := &Process{Base: &BaseProcess{mp: mp}}
	source := batch.NewWithSize(2)
	source.Vecs[0] = vector.NewVec(types.T_int8.ToType())
	source.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	defer source.Clean(mp)
	baseline := mp.AllocatedBytes()
	for _, rows := range []int{8, 150000, 8} {
		copy, err := proc.NewBatchFromSrc(source, rows)
		if rows == 150000 {
			// The int8 column fits; the following int64 column exceeds the
			// pool's capacity after the first native allocation succeeded.
			require.Error(t, err)
			require.Nil(t, copy)
		} else {
			require.NoError(t, err)
			require.Len(t, copy.Vecs, 2)
			copy.Clean(mp)
		}
		require.Equal(t, baseline, mp.AllocatedBytes(), "both success and failed initialization retire all owned stores")
	}
}
