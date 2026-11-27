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

package preinsertunique

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestODKUFloatKeysPreservePhysicalIdentity(t *testing.T) {
	a, proc, b, _ := newODKUKeyFixture(t, 1)
	typ := types.T_float32.ToType()
	typ.Scale = 2
	b.Vecs[1].Free(proc.Mp())
	b.Vecs[1] = vector.NewVec(typ)
	for _, value := range []float32{0.12541, 0.12549, 0, math.Float32frombits(1 << 31)} {
		require.NoError(t, vector.AppendFixed(b.Vecs[1], value, false, proc.Mp()))
	}
	a.ctr.acceptedKeys[0].key[0] = b.Vecs[1]
	require.NoError(t, a.initODKUKey(proc, 0))
	generic, err := hashmap.NewStrHashMapWithAllocations(false, proc.Mp(), a.hashAllocation, a.iteratorAllocation)
	require.NoError(t, err)
	itr := generic.NewIterator()
	defer func() { hashmap.IteratorClearOwner(itr); generic.Free() }()
	for row := range 4 {
		require.NoError(t, a.claimODKUKey(proc, 0, row, int64(row)))
	}
	for row := range 4 {
		owner, found, err := a.lookupODKUKeyIndex(proc, &a.ctr.acceptedKeys[0], row)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, int64(row), owner)
	}
	require.Equal(t, uint64(4), a.ctr.acceptedKeys[0].index.GroupCount())
	// The SQL hashmap deliberately keeps its separate equality contract.
	strGroups, _, err := itr.Insert(0, 4, []*vector.Vector{b.Vecs[1]})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 1, 2, 2}, strGroups)

}

func TestODKUPhysicalTypeCompatibility(t *testing.T) {
	integer := types.T_int64.ToType()
	unspecifiedScale := integer
	unspecifiedScale.Scale = -1
	require.True(t, odkuKeyTypeEqual(integer, unspecifiedScale), "integer scale does not change stored values")
	wrongSize := integer
	wrongSize.Size = 4
	require.False(t, odkuKeyTypeEqual(integer, wrongSize))
	float := types.T_float64.ToType()
	scaled := float
	scaled.Scale = 2
	require.False(t, odkuKeyTypeEqual(float, scaled), "floating scale affects the physical key encoder")
}
