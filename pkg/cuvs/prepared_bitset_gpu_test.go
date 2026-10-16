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

//go:build gpu

package cuvs

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// bitsetTestMembership returns an exact C membership of keys and its free.
func bitsetTestMembership(t *testing.T, keys []int64) (docfilter.CMembership, func()) {
	mp := mpool.MustNewZero()
	v := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(v, keys, nil, mp))
	f, err := docfilter.NewForC(v, nil)
	v.Free(mp)
	require.NoError(t, err)
	mem, err := docfilter.CExactMembershipOf(f)
	require.NoError(t, err)
	return mem, f.Free
}

// bitsetTestDataset returns n rows of dimension 2 at (i, i) and ids 1000 + i.
func bitsetTestDataset(n uint64) ([]float32, []int64) {
	dataset := make([]float32, n*2)
	ids := make([]int64, n)
	for i := uint64(0); i < n; i++ {
		dataset[i*2] = float32(i)
		dataset[i*2+1] = float32(i)
		ids[i] = int64(1000 + i)
	}
	return dataset, ids
}

func TestGpuCagraPreparedBitset(t *testing.T) {
	const n, dimension, limit = uint64(1000), uint32(2), uint32(3)
	dataset, ids := bitsetTestDataset(n)
	bp := DefaultCagraBuildParams()
	index, err := NewGpuCagra[float32, float32](dataset, n, dimension, L2Expanded, bp, []int{0}, 1, SingleGpu, ids)
	require.NoError(t, err)
	defer index.Destroy()
	require.NoError(t, index.Start())
	require.NoError(t, index.Build())

	mem, free := bitsetTestMembership(t, []int64{1005, 1050, 1500})
	query := []float32{0, 0}
	sp := DefaultCagraSearchParams()
	want, err := index.SearchQuantizeWithFilter(query, 1, dimension, limit, sp, "", mem)
	require.NoError(t, err)
	require.Equal(t, int64(1005), want.Neighbors[0])
	for _, id := range want.Neighbors {
		require.Contains(t, []int64{1005, 1050, 1500, -1}, id)
	}

	bitset, err := index.PrepareBitset("", mem)
	require.NoError(t, err)
	free() // the bitset does not read the membership
	for round := 0; round < 2; round++ {
		got, err := index.SearchQuantizeWithBitset(query, 1, dimension, limit, sp, bitset)
		require.NoError(t, err)
		require.Equal(t, want.Neighbors, got.Neighbors)
	}
	job, err := index.SearchQuantizeWithBitsetAsync(query, 1, dimension, limit, sp, bitset)
	require.NoError(t, err)
	bitset.Free()
	bitset.Free()
	neighbors, _, err := index.SearchWait(job, 1, limit)
	require.NoError(t, err)
	require.Equal(t, want.Neighbors, neighbors)

	_, err = index.SearchQuantizeWithBitset(query, 1, dimension, limit, sp, nil)
	require.ErrorContains(t, err, "bitset is null")
}

func TestGpuIvfPqPreparedBitset(t *testing.T) {
	const n, dimension, limit = uint64(1000), uint32(2), uint32(3)
	dataset, ids := bitsetTestDataset(n)
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 4
	index, err := NewGpuIvfPq[float32, float32](dataset, n, dimension, L2Expanded, bp, []int{0}, 1, SingleGpu, ids)
	require.NoError(t, err)
	defer index.Destroy()
	require.NoError(t, index.Start())
	require.NoError(t, index.Build())

	mem, free := bitsetTestMembership(t, []int64{1005, 1050, 1500})
	query := []float32{0, 0}
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 4
	want, err := index.SearchQuantizeWithFilter(query, 1, dimension, limit, sp, "", mem)
	require.NoError(t, err)

	bitset, err := index.PrepareBitset("", mem)
	require.NoError(t, err)
	defer bitset.Free()
	free()
	for round := 0; round < 2; round++ {
		got, err := index.SearchQuantizeWithBitset(query, 1, dimension, limit, sp, bitset)
		require.NoError(t, err)
		require.Equal(t, want.Neighbors, got.Neighbors)
		for _, id := range got.Neighbors {
			require.Contains(t, []int64{1005, 1050, 1500, -1}, id)
		}
	}
	job, err := index.SearchQuantizeWithBitsetAsync(query, 1, dimension, limit, sp, bitset)
	require.NoError(t, err)
	neighbors, _, err := index.SearchWait(job, 1, limit)
	require.NoError(t, err)
	require.Equal(t, want.Neighbors, neighbors)

	// a bitset of another index is refused
	other, err := NewGpuIvfPq[float32, float32](dataset, n, dimension, L2Expanded, bp, []int{0}, 1, SingleGpu, ids)
	require.NoError(t, err)
	defer other.Destroy()
	require.NoError(t, other.Start())
	require.NoError(t, other.Build())
	foreign, err := other.PrepareBitset("", docfilter.CMembership{})
	require.NoError(t, err)
	defer foreign.Free()
	_, err = index.SearchQuantizeWithBitset(query, 1, dimension, limit, sp, foreign)
	require.ErrorContains(t, err, "prepared by another index")
}

func TestGpuBruteForcePreparedBitset(t *testing.T) {
	const n, dimension, limit = uint64(100), uint32(2), uint32(3)
	dataset, ids := bitsetTestDataset(n)
	index, err := NewGpuBruteForceEmpty[float32, float32](n, dimension, L2Expanded, 1, 0)
	require.NoError(t, err)
	defer index.Destroy()
	require.NoError(t, index.Start())
	require.NoError(t, index.AddChunkQuantize(dataset, n, ids))
	require.NoError(t, index.Build())

	mem, free := bitsetTestMembership(t, []int64{1005, 1050})
	query := []float32{0, 0}
	bitset, err := index.PrepareBitset("", mem)
	require.NoError(t, err)
	defer bitset.Free()
	free()
	for round := 0; round < 2; round++ {
		neighbors, _, err := index.SearchQuantizeWithBitset(query, 1, dimension, limit, bitset)
		require.NoError(t, err)
		require.Equal(t, []int64{1005, 1050, -1}, neighbors)
	}
	job, err := index.SearchQuantizeWithBitsetAsync(query, 1, dimension, limit, bitset)
	require.NoError(t, err)
	neighbors, _, err := index.SearchWait(job, 1, limit)
	require.NoError(t, err)
	require.Equal(t, []int64{1005, 1050, -1}, neighbors)
}

// bitsetTestOverflow returns a brute-force overflow of rows n..n+m-1 at (i, i)
// with ids 1000 + i.
func bitsetTestOverflow(t *testing.T, n, m uint64) *GpuBruteForce[float32, float32] {
	dataset := make([]float32, m*2)
	ids := make([]int64, m)
	for i := uint64(0); i < m; i++ {
		dataset[i*2] = float32(n + i)
		dataset[i*2+1] = float32(n + i)
		ids[i] = int64(1000 + n + i)
	}
	bf, err := NewGpuBruteForceEmpty[float32, float32](m, 2, L2Expanded, 1, 0)
	require.NoError(t, err)
	require.NoError(t, bf.Start())
	require.NoError(t, bf.AddChunkQuantize(dataset, m, ids))
	require.NoError(t, bf.Build())
	return bf
}

func TestMultiGpuCagraPreparedBitset(t *testing.T) {
	const n, m, dimension, limit = uint64(1000), uint64(50), uint32(2), uint32(4)
	dataset, ids := bitsetTestDataset(n)
	index, err := NewGpuCagra[float32, float32](dataset, n, dimension, L2Expanded, DefaultCagraBuildParams(), []int{0}, 1, SingleGpu, ids)
	require.NoError(t, err)
	require.NoError(t, index.Start())
	require.NoError(t, index.Build())
	overflow := bitsetTestOverflow(t, n, m)
	defer index.Destroy()
	defer overflow.Destroy()
	multi := NewMultiGpuCagra[float32, float32]([]*GpuCagra[float32, float32]{index}, overflow, dimension, L2Expanded)

	// members in the index (1005, 1050) and in the overflow (2010)
	mem, free := bitsetTestMembership(t, []int64{1005, 1050, 2010})
	query := []float32{0, 0}
	sp := DefaultCagraSearchParams()
	want, _, err := multi.SearchQuantizeWithFilter(query, 1, dimension, limit, sp, "", mem)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{1005, 1050, 2010, -1}, want)

	bitset, err := multi.PrepareBitset("", mem)
	require.NoError(t, err)
	defer bitset.Free()
	free()
	for round := 0; round < 2; round++ {
		got, _, err := multi.SearchQuantizeWithBitset(query, 1, dimension, limit, sp, bitset)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	_, _, err = multi.SearchQuantizeWithBitset(query, 1, dimension, limit, sp, nil)
	require.ErrorContains(t, err, "bitset is nil")
}

func TestMultiGpuIvfPqPreparedBitset(t *testing.T) {
	const n, m, dimension, limit = uint64(1000), uint64(50), uint32(2), uint32(4)
	dataset, ids := bitsetTestDataset(n)
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 4
	index, err := NewGpuIvfPq[float32, float32](dataset, n, dimension, L2Expanded, bp, []int{0}, 1, SingleGpu, ids)
	require.NoError(t, err)
	require.NoError(t, index.Start())
	require.NoError(t, index.Build())
	overflow := bitsetTestOverflow(t, n, m)
	defer index.Destroy()
	defer overflow.Destroy()
	multi := NewMultiGpuIvfPq[float32, float32]([]*GpuIvfPq[float32, float32]{index}, overflow, dimension, L2Expanded)
	require.Equal(t, uint64(4), multi.NLists())
	require.Zero(t, NewMultiGpuIvfPq[float32, float32](nil, overflow, dimension, L2Expanded).NLists())

	mem, free := bitsetTestMembership(t, []int64{1005, 1050, 2010})
	query := []float32{0, 0}
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 4
	want, _, err := multi.SearchQuantizeWithFilter(query, 1, dimension, limit, sp, "", mem)
	require.NoError(t, err)
	require.Contains(t, want, int64(2010))

	bitset, err := multi.PrepareBitset("", mem)
	require.NoError(t, err)
	defer bitset.Free()
	free()
	for round := 0; round < 2; round++ {
		got, _, err := multi.SearchQuantizeWithBitset(query, 1, dimension, limit, sp, bitset)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}
