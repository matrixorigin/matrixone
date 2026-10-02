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
	"cmp"
	"math"
	"math/rand"
	"slices"
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
				m, err := cuvs.NewBlockScaledMatmul(int(f), dim, nq, queries, types.BlockScaledCellSize(f, dim), 200, 0)
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
	_, err := cuvs.NewBlockScaledMatmul(int(types.BlockScaledMXFP8), 64, 3, queries, types.BlockScaledCellSize(types.BlockScaledMXFP8, 64), 10, 0)
	require.Error(t, err)
	_, err = cuvs.NewBlockScaledMatmul(int(types.BlockScaledMXFP8), 64, 2, queries, types.BlockScaledCellSize(types.BlockScaledMXFP8, 64), 0, 0)
	require.Error(t, err)

	m, err := cuvs.NewBlockScaledMatmul(int(types.BlockScaledMXFP8), 64, 2, queries, types.BlockScaledCellSize(types.BlockScaledMXFP8, 64), 10, 0)
	require.NoError(t, err)
	defer m.Close()
	require.NoError(t, m.Run(nil, nil))
	cells, _ := blockScaledCells(t, r, types.BlockScaledMXFP8, 129, 64)
	require.Error(t, m.Run(cells[:m.CellBytes()+1], make([]float32, 4)))
	require.Error(t, m.Run(cells, make([]float32, 129*2)))
	require.Error(t, m.Run(cells[:m.CellBytes()], make([]float32, 1)))
	require.Error(t, m.RunTopK(cells[:m.CellBytes()], make([]float32, 2), make([]int32, 2), make([]float32, 2), make([]uint8, 2)))

	_, err = cuvs.NewBlockScaledMatmul(int(types.BlockScaledMXFP8), 64, 2, queries, types.BlockScaledCellSize(types.BlockScaledMXFP8, 64), 10, -1)
	require.Error(t, err)
	k, err := cuvs.NewBlockScaledMatmul(int(types.BlockScaledMXFP8), 64, 2, queries, types.BlockScaledCellSize(types.BlockScaledMXFP8, 64), 10, 3)
	require.NoError(t, err)
	defer k.Close()
	require.Equal(t, 3, k.TopK())
	require.NoError(t, k.RunTopK(nil, nil, nil, nil, nil))
	require.Error(t, k.RunTopK(cells[:k.CellBytes()+1], make([]float32, 6), make([]int32, 6), make([]float32, 2), make([]uint8, 2)))
	require.Error(t, k.RunTopK(cells[:k.CellBytes()], make([]float32, 5), make([]int32, 6), make([]float32, 2), make([]uint8, 2)))
	require.Error(t, k.RunTopK(cells, make([]float32, 6), make([]int32, 6), make([]float32, 129*2), make([]uint8, 2)))
}

// TestBlockScaledMatmulRunTopK compares RunTopK with the full scores of Run: kept rows carry
// their exact scores, every row above the k-th score is kept, and a query with rows tied at
// the k-th score left out is flagged with its full scores.
func TestBlockScaledMatmulRunTopK(t *testing.T) {
	r := rand.New(rand.NewSource(29554))
	for _, tc := range []struct {
		format, dim, rows, k int
		cells                func(n int) []byte
	}{
		{cuvs.BlockScaledMatmulMXFP8, 96, 300, 10, func(n int) []byte { c, _ := blockScaledCells(t, r, types.BlockScaledMXFP8, n, 96); return c }},
		{cuvs.BlockScaledMatmulNVFP4, 96, 300, 10, func(n int) []byte { c, _ := blockScaledCells(t, r, types.BlockScaledNVFP4, n, 96); return c }},
		// values in {0, 1, 2}: many rows tie at the k-th score
		{cuvs.BlockScaledMatmulI8, 4, 1000, 5, func(n int) []byte {
			c := make([]byte, n*4)
			for i := range c {
				c[i] = byte(r.Intn(3))
			}
			return c
		}},
		{cuvs.BlockScaledMatmulU8, 4, 1000, 5, func(n int) []byte {
			c := make([]byte, n*4)
			for i := range c {
				c[i] = byte(r.Intn(3))
			}
			return c
		}},
	} {
		const nq = 3
		queries := tc.cells(nq)
		cells := tc.cells(tc.rows)
		cellBytes := len(cells) / tc.rows
		m, err := cuvs.NewBlockScaledMatmul(tc.format, tc.dim, nq, queries, cellBytes, 512, tc.k)
		require.NoError(t, err)
		k := m.TopK()
		require.Equal(t, tc.k, k)
		for off := 0; off < tc.rows; off += m.MaxRows() {
			n := min(m.MaxRows(), tc.rows-off)
			tile := cells[off*cellBytes : (off+n)*cellBytes]
			scores := make([]float32, n*nq)
			require.NoError(t, m.Run(tile, scores))
			top, rows := make([]float32, nq*k), make([]int32, nq*k)
			full, tied := make([]float32, n*nq), make([]uint8, nq)
			require.NoError(t, m.RunTopK(tile, top, rows, full, tied))
			for q := 0; q < nq; q++ {
				col := make([]float32, n)
				for i := range col {
					col[i] = scores[i*nq+q]
				}
				sorted := append([]float32(nil), col...)
				slices.SortFunc(sorted, func(a, b float32) int { return cmp.Compare(b, a) })
				kth := sorted[min(k, n)-1]
				kept := make(map[int32]bool)
				keptAtKth := 0
				for j := 0; j < k; j++ {
					row := rows[q*k+j]
					if row < 0 {
						continue
					}
					require.False(t, kept[row])
					kept[row] = true
					require.Equal(t, col[row], top[q*k+j])
					if col[row] == kth {
						keptAtKth++
					}
				}
				require.Len(t, kept, min(k, n))
				allAtKth := 0
				for i, v := range col {
					if v > kth {
						require.True(t, kept[int32(i)])
					}
					if v == kth {
						allAtKth++
					}
				}
				require.Equal(t, allAtKth > keptAtKth, tied[q] != 0, "format %d query %d", tc.format, q)
				if tied[q] != 0 {
					require.Equal(t, col, full[q*n:(q+1)*n])
				}
			}
		}
		m.Close()
	}
}
