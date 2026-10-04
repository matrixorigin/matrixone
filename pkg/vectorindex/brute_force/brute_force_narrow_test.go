//go:build !gpu

// Copyright 2022 Matrix Origin
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

package brute_force

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// TestNewCpuBruteForceIndexNarrow covers the bf16 / f16 / int8 / uint8 dispatch
// arms of NewCpuBruteForceIndex. The f32/f64 arms are covered by the existing
// tests; the default arm is unreachable because ArrayElement is exactly these
// six element types. Queries are typed [][]T to match GoBruteForceIndex.Search.
func TestNewCpuBruteForceIndexNarrow(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)
	rt := vectorindex.RuntimeConfig{Limit: 2, NThreads: 1}
	const dim = uint(3)

	bf := func(vs ...float32) []types.BF16 {
		out := make([]types.BF16, len(vs))
		for i, v := range vs {
			out[i] = types.BF16FromFloat32(v)
		}
		return out
	}
	f16 := func(vs ...float32) []types.Float16 {
		out := make([]types.Float16, len(vs))
		for i, v := range vs {
			out[i] = types.Float16FromFloat32(v)
		}
		return out
	}

	t.Run("bf16", func(t *testing.T) {
		ds := [][]types.BF16{bf(1, 2, 3), bf(3, 4, 5)}
		idx, err := NewCpuBruteForceIndex[types.BF16](ds, dim, metric.Metric_L2sqDistance, 2)
		require.NoError(t, err)
		keys, dists, err := idx.Search(sqlproc, ds, rt)
		require.NoError(t, err)
		require.NotNil(t, keys)
		require.Len(t, dists, 4)
	})

	t.Run("f16", func(t *testing.T) {
		ds := [][]types.Float16{f16(1, 2, 3), f16(3, 4, 5)}
		idx, err := NewCpuBruteForceIndex[types.Float16](ds, dim, metric.Metric_L2sqDistance, 2)
		require.NoError(t, err)
		keys, dists, err := idx.Search(sqlproc, ds, rt)
		require.NoError(t, err)
		require.NotNil(t, keys)
		require.Len(t, dists, 4)
	})

	t.Run("int8", func(t *testing.T) {
		ds := [][]int8{{1, 2, 3}, {3, 4, 5}}
		idx, err := NewCpuBruteForceIndex[int8](ds, dim, metric.Metric_L2sqDistance, 1)
		require.NoError(t, err)
		keys, dists, err := idx.Search(sqlproc, ds, rt)
		require.NoError(t, err)
		require.NotNil(t, keys)
		require.Len(t, dists, 4)
	})

	t.Run("uint8", func(t *testing.T) {
		ds := [][]uint8{{1, 2, 3}, {3, 4, 5}}
		idx, err := NewCpuBruteForceIndex[uint8](ds, dim, metric.Metric_L2sqDistance, 1)
		require.NoError(t, err)
		keys, dists, err := idx.Search(sqlproc, ds, rt)
		require.NoError(t, err)
		require.NotNil(t, keys)
		require.Len(t, dists, 4)
	})
}
