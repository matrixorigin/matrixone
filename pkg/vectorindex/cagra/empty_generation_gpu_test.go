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

//go:build gpu

package cagra

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

const (
	emptyGenDim  = 2
	emptyGenRows = 1000
)

func emptyGenCfg() vectorindex.IndexConfig {
	cfg := vectorindex.IndexConfig{}
	cfg.CuvsCagra.Dimensions = emptyGenDim
	cfg.CuvsCagra.Metric = uint16(metric.Metric_L2Distance)
	cfg.CuvsCagra.DistributionMode = uint16(vectorindex.DistributionMode_SINGLE_GPU)
	return cfg
}

// emptyGenLoad assembles a loaded CagraSearch the way Load does — buildOverflow then
// buildMultiIndex over the given models — so EmptyGeneration is read off the state a real
// load produces rather than off hand-set fields.
func emptyGenLoad(t *testing.T, models []*CagraModel[float32, float32]) *CagraSearch[float32, float32] {
	t.Helper()
	s := &CagraSearch[float32, float32]{
		Idxcfg:        emptyGenCfg(),
		Devices:       []int{0},
		ThreadsSearch: 1,
		Indexes:       models,
	}
	require.NoError(t, s.buildOverflow())
	mi, err := s.buildMultiIndex()
	require.NoError(t, err)
	s.MultiIndex = mi
	t.Cleanup(s.Destroy)
	return s
}

// emptyGenRowsVecs returns n distinct dim-wide vectors, row i filled with i.
func emptyGenRowsVecs(n int) []float32 {
	vecs := make([]float32, n*emptyGenDim)
	for i := 0; i < n; i++ {
		for j := 0; j < emptyGenDim; j++ {
			vecs[i*emptyGenDim+j] = float32(i)
		}
	}
	return vecs
}

// A generation with nothing in it — no metadata rows at all (the async-build window before the
// first sub-index is committed, or a freshly created index), or metadata rows whose models
// carry neither a deserialized sub-index nor CDC overflow — leaves buildMultiIndex nil, which
// is exactly the state Search answers empty on. EmptyGeneration must report it so the cache
// serves that empty answer without retaining the generation: the async build fills the SAME
// version in place, so a retained empty generation keeps answering empty on this CN until the
// IsStale sweep evicts it ~10min later (#29011).
func TestCagraEmptyGenerationIsNotRetained(t *testing.T) {
	// No metadata rows.
	none := emptyGenLoad(t, nil)
	require.Nil(t, none.MultiIndex)
	require.Nil(t, none.Overflow)
	require.True(t, none.EmptyGeneration())

	// Metadata rows, but no sub-index deserialized and no CDC tail rows.
	bare := emptyGenLoad(t, []*CagraModel[float32, float32]{{Id: "m0"}, {Id: "m1"}})
	require.Nil(t, bare.MultiIndex)
	require.Nil(t, bare.Overflow)
	require.True(t, bare.EmptyGeneration())

	// The result the cache serves from that generation is empty, so evicting it after the
	// search frees nothing the returned keys alias.
	keys, dists, err := none.Search(nil, make([]float32, emptyGenDim), vectorindex.RuntimeConfig{Limit: 4})
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Empty(t, dists)
}

// A CDC-only generation is the small-data shape: too few rows to build a base index, so every
// row lives in the brute-force overflow built from the tag=1 tail. It holds real vectors and
// answers real queries, so EmptyGeneration must report false — reporting true would evict it
// after every single query and reload the tail each time, forever.
func TestCagraEmptyGenerationKeepsCdcOnlyGeneration(t *testing.T) {
	s := emptyGenLoad(t, []*CagraModel[float32, float32]{
		{Id: "m0", OverflowPkids: []int64{11, 22}, OverflowVecs: emptyGenRowsVecs(2)},
	})

	require.NotNil(t, s.Overflow, "the tail rows built a brute-force overflow")
	require.EqualValues(t, 2, s.Overflow.Len())
	require.NotNil(t, s.MultiIndex, "overflow alone is still a searchable index")
	require.False(t, s.EmptyGeneration())
}

// A generation with a deserialized base sub-index is the ordinary loaded index: cached as
// before, whether or not it also carries CDC overflow.
func TestCagraEmptyGenerationKeepsBuiltIndex(t *testing.T) {
	bp := cuvs.DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128
	idx, err := cuvs.NewGpuCagra[float32, float32](emptyGenRowsVecs(emptyGenRows), emptyGenRows,
		emptyGenDim, cuvs.L2Expanded, bp, []int{0}, 1, cuvs.SingleGpu, nil)
	require.NoError(t, err)
	require.NoError(t, idx.Start())
	require.NoError(t, idx.Build())

	// The model owns idx from here — CagraSearch.Destroy releases it via the model.
	s := emptyGenLoad(t, []*CagraModel[float32, float32]{{Id: "m0", Index: idx, Len: emptyGenRows}})
	require.NotNil(t, s.MultiIndex)
	require.False(t, s.EmptyGeneration())
}
