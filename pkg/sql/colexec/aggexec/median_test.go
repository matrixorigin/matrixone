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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMedianMarshal(t *testing.T) {
	m := hackAggMemoryManager()

	vs := NewVectors[int64](types.T_int64.ToType())
	defer vs.Free(m.mp)
	AppendMultiFixed(vs, 1, false, 262145, m.mp)
	assert.Equal(t, vs.Length(), 262145)
	assert.Equal(t, 2, len(vs.vecs))
	size1 := vs.Size()
	assert.Greater(t, size1, int64(0))

	vs2 := NewVectors[int64](types.T_int64.ToType())
	defer vs2.Free(m.mp)
	AppendMultiFixed(vs2, 1, false, 262145, m.mp)
	assert.Equal(t, vs2.Length(), 262145)
	assert.Equal(t, 2, len(vs2.vecs))

	vs.Union(vs2, m.mp)
	assert.Equal(t, vs.Length(), 262145*2)
	assert.Equal(t, 3, len(vs.vecs))
	size2 := vs.Size()
	assert.Greater(t, size2, size1)

	b, err := vs.MarshalBinary()
	assert.NoError(t, err)
	vs3 := NewEmptyVectors[int64]()
	defer vs3.Free(m.mp)
	err = vs3.Unmarshal(b, types.T_int64.ToType(), m.mp)
	assert.NoError(t, err)
	assert.Equal(t, vs.Length(), vs3.Length())
	assert.Equal(t, vs.vecs[0].Length(), vs3.vecs[0].Length())
	assert.Greater(t, vs3.Size(), int64(0))
}

func TestMedianAggSize(t *testing.T) {
	m := hackAggMemoryManager()
	defer func() {
		require.Equal(t, int64(0), m.Mp().CurrNB())
	}()

	agg, err := newMedianExecutor(m, singleAggInfo{
		aggID:     AggIdOfMedian,
		distinct:  false,
		argType:   types.T_int64.ToType(),
		retType:   MedianReturnType([]types.Type{types.T_int64.ToType()}),
		emptyNull: true,
	})
	require.NoError(t, err)

	initialSize := agg.Size()
	require.Greater(t, initialSize, int64(0))

	// grow
	groupCount := 10
	err = agg.GroupGrow(groupCount)
	require.NoError(t, err)
	grownSize := agg.Size()
	require.Greater(t, grownSize, initialSize)

	// fill
	v := vector.NewVec(types.T_int64.ToType())
	defer v.Free(m.Mp())
	err = vector.AppendFixedList(v, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil, m.Mp())
	require.NoError(t, err)

	for i := 0; i < groupCount; i++ {
		require.NoError(t, agg.BulkFill(i, []*vector.Vector{v}))
	}
	filledSize := agg.Size()
	require.Greater(t, filledSize, grownSize)

	// free
	agg.Free()
}
