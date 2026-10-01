//go:build gpu

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

package cuvs_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func blockScaledCells(t *testing.T, r *rand.Rand, f types.BlockScaledFormat, n, dim int) ([]byte, []metric.VecBlockOperand) {
	var cells []byte
	ops := make([]metric.VecBlockOperand, n)
	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for k := range v {
			v[k] = float32(r.NormFloat64() * 3)
		}
		start := len(cells)
		var err error
		cells, err = types.AppendBlockScaled(cells, f, v)
		require.NoError(t, err)
		ops[i].Cell, err = types.ParseBlockScaledCell(cells[start:])
		require.NoError(t, err)
	}
	return cells, ops
}

// TestBlockScaledMatmulMatchesCPU compares the cuBLASLt scores with the CPU kernel over
// the same cells.
func TestBlockScaledMatmulMatchesCPU(t *testing.T) {
	r := rand.New(rand.NewSource(20567))
	for _, f := range []types.BlockScaledFormat{types.BlockScaledMXFP8, types.BlockScaledNVFP4} {
		for _, dim := range []int{4, 33, 100, 768} {
			for _, nq := range []int{1, 5} {
				queries, qops := blockScaledCells(t, r, f, nq, dim)
				m, err := cuvs.NewBlockScaledMatmul(f, dim, nq, queries, 200)
				require.NoError(t, err)
				require.Equal(t, 256, m.MaxRows())
				rows := 300
				cells, rops := blockScaledCells(t, r, f, rows, dim)
				scores := make([]float32, rows*nq)
				for off := 0; off < rows; off += m.MaxRows() {
					end := min(rows, off+m.MaxRows())
					require.NoError(t, m.Run(cells[off*m.CellBytes():end*m.CellBytes()], scores[off*nq:]))
				}
				for i := 0; i < rows; i++ {
					for q := 0; q < nq; q++ {
						want, err := metric.VecBlockDot(&rops[i], &qops[q])
						require.NoError(t, err)
						got := float64(scores[i*nq+q])
						require.InDelta(t, want, got, 1e-4*math.Max(1, math.Abs(want)), "%s dim %d row %d query %d", f, dim, i, q)
					}
				}
				m.Close()
				m.Close()
			}
		}
	}
}

func TestBlockScaledMatmulErrors(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	queries, _ := blockScaledCells(t, r, types.BlockScaledMXFP8, 2, 64)
	_, err := cuvs.NewBlockScaledMatmul(types.BlockScaledMXFP8, 64, 3, queries, 10)
	require.Error(t, err)
	_, err = cuvs.NewBlockScaledMatmul(types.BlockScaledMXFP8, 64, 2, queries, 0)
	require.Error(t, err)

	m, err := cuvs.NewBlockScaledMatmul(types.BlockScaledMXFP8, 64, 2, queries, 10)
	require.NoError(t, err)
	defer m.Close()
	require.NoError(t, m.Run(nil, nil))
	cells, _ := blockScaledCells(t, r, types.BlockScaledMXFP8, 129, 64)
	require.Error(t, m.Run(cells[:m.CellBytes()+1], make([]float32, 4)))
	require.Error(t, m.Run(cells, make([]float32, 129*2)))
	require.Error(t, m.Run(cells[:m.CellBytes()], make([]float32, 1)))
}
