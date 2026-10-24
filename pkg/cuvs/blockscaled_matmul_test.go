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
	"encoding/binary"
	"fmt"
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
				m, err := cuvs.NewBlockScaledMatmul(int(f), dim, nq, queries, types.BlockScaledCellSize(f, dim), 200, 0, cuvs.BlockScaledMatmulInnerProduct)
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
	_, err := cuvs.NewBlockScaledMatmul(int(types.BlockScaledMXFP8), 64, 3, queries, types.BlockScaledCellSize(types.BlockScaledMXFP8, 64), 10, 0, cuvs.BlockScaledMatmulInnerProduct)
	require.Error(t, err)
	_, err = cuvs.NewBlockScaledMatmul(int(types.BlockScaledMXFP8), 64, 2, queries, types.BlockScaledCellSize(types.BlockScaledMXFP8, 64), 0, 0, cuvs.BlockScaledMatmulInnerProduct)
	require.Error(t, err)

	m, err := cuvs.NewBlockScaledMatmul(int(types.BlockScaledMXFP8), 64, 2, queries, types.BlockScaledCellSize(types.BlockScaledMXFP8, 64), 10, 0, cuvs.BlockScaledMatmulInnerProduct)
	require.NoError(t, err)
	defer m.Close()
	require.NoError(t, m.Run(nil, nil))
	cells, _ := blockScaledCells(t, r, types.BlockScaledMXFP8, 129, 64)
	require.Error(t, m.Run(cells[:m.CellBytes()+1], make([]float32, 4)))
	require.Error(t, m.Run(cells, make([]float32, 129*2)))
	require.Error(t, m.Run(cells[:m.CellBytes()], make([]float32, 1)))
	require.Error(t, m.RunTopK(cells[:m.CellBytes()], make([]float32, 2), make([]int32, 2), make([]float32, 2), make([]uint8, 2)))

	_, err = cuvs.NewBlockScaledMatmul(int(types.BlockScaledMXFP8), 64, 2, queries, types.BlockScaledCellSize(types.BlockScaledMXFP8, 64), 10, -1, cuvs.BlockScaledMatmulInnerProduct)
	require.Error(t, err)
	k, err := cuvs.NewBlockScaledMatmul(int(types.BlockScaledMXFP8), 64, 2, queries, types.BlockScaledCellSize(types.BlockScaledMXFP8, 64), 10, 3, cuvs.BlockScaledMatmulInnerProduct)
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
		m, err := cuvs.NewBlockScaledMatmul(tc.format, tc.dim, nq, queries, cellBytes, 512, tc.k, cuvs.BlockScaledMatmulInnerProduct)
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

// exactBlockScaled decodes c in float64: element * blockScale * global, without float32 rounding.
func exactBlockScaled(c types.BlockScaledCell) []float64 {
	out := make([]float64, c.Dim)
	for i := range out {
		var e, s float64
		if c.Format == types.BlockScaledMXFP8 {
			e = float64(types.Float8(c.Elems[i]).ToFloat32())
			s = math.Ldexp(1, int(c.Scales[i/32])-127)
		} else {
			e = float64(types.Float4(c.Elems[i/2] >> (4 * (i % 2))).ToFloat32())
			s = float64(types.Float8(c.Scales[i/16]).ToFloat32())
		}
		out[i] = e * s * float64(c.Global)
	}
	return out
}

// handBlockScaledCell builds a cell from a global scale, scale codes and one element code per element.
func handBlockScaledCell(f types.BlockScaledFormat, dim int, global float32, scales, codes []byte) []byte {
	cell := make([]byte, types.BlockScaledCellSize(f, dim))
	cell[0], cell[1] = 1, byte(f)
	binary.LittleEndian.PutUint32(cell[4:], uint32(dim))
	binary.LittleEndian.PutUint32(cell[8:], math.Float32bits(global))
	ns := types.BlockScaledScaleCount(f, dim)
	copy(cell[12:12+ns], scales)
	el := cell[12+ns:]
	for i, c := range codes {
		if f == types.BlockScaledMXFP8 {
			el[i] = c
		} else {
			el[i/2] |= (c & 0x0f) << (4 * (i % 2))
		}
	}
	return cell
}

// TestBlockScaledMatmulAgreesAcrossMagnitudes compares the engine and the CPU distance
// functions (VecBlockSQLDistance) with the exact float64 distance of the decoded values, for
// encoder-produced cells with element magnitudes from 1e-41 to 1e36 and hand-built cells
// (vecf4 with a subnormal global * block scale, vecf8 with E8M0 codes 0-120 and E4M3 subnormal
// elements). Where every nonzero decoded value is a normal float32: an exact distance in the
// float32 normal range is matched by both (inner product within 1e-4 of sum |x_i q_i|, squared
// L2 within 1e-4 of the squared norms, cosine within 1e-4); above the range both are infinite;
// below it both are under 2^-100. Squared L2 of a row and an equal query is not compared (the
// GEMM expansion near 0, see Decisions).
func TestBlockScaledMatmulAgreesAcrossMagnitudes(t *testing.T) {
	r := rand.New(rand.NewSource(20567))
	type tc struct {
		name        string
		f           types.BlockScaledFormat
		rows, query [][]byte
	}
	var cases []tc
	vec := func(dim int, scale float64) []float32 {
		v := make([]float32, dim)
		for i := range v {
			v[i] = float32(r.NormFloat64() * scale)
		}
		return v
	}
	for _, f := range []types.BlockScaledFormat{types.BlockScaledMXFP8, types.BlockScaledNVFP4} {
		for _, dim := range []int{32, 33, 768} {
			for _, sc := range []float64{1, 1e-10, 1e-20, 1e-25, 1e-30, 1e-35, 1e-38, 1e-41, 1e15, 1e19, 1e30, 1e36} {
				c := tc{name: fmt.Sprintf("%s dim %d scale %g", f, dim, sc), f: f}
				for i := 0; i < 5; i++ {
					cell, err := types.AppendBlockScaled(nil, f, vec(dim, sc))
					require.NoError(t, err, c.name)
					if i < 3 {
						c.rows = append(c.rows, cell)
					} else {
						c.query = append(c.query, cell)
					}
				}
				zero, err := types.AppendBlockScaled(nil, f, make([]float32, dim))
				require.NoError(t, err)
				c.rows = append(c.rows, zero)
				// a query equal to a row: a dot product of the vector's own magnitude
				c.query = append(c.query, c.rows[0])
				cases = append(cases, c)
			}
		}
	}
	// global scales whose float product overflows while the dot product is in range
	{
		x, q := make([]float32, 48), make([]float32, 48)
		x[0], x[16], q[16], q[32] = 5.4e22, 1e19, 1e19, 5.4e22
		xc, err := types.AppendBlockScaled(nil, types.BlockScaledNVFP4, x)
		require.NoError(t, err)
		qc, err := types.AppendBlockScaled(nil, types.BlockScaledNVFP4, q)
		require.NoError(t, err)
		cases = append(cases, tc{name: "vecf4 global product overflow", f: types.BlockScaledNVFP4, rows: [][]byte{xc}, query: [][]byte{qc}})
	}
	codes := func(dim int, code func(i int) byte) []byte {
		cs := make([]byte, dim)
		for i := range cs {
			cs[i] = code(i)
		}
		return cs
	}
	for _, g := range []float32{float32(math.Ldexp(1, -140)), math.SmallestNonzeroFloat32, float32(math.Ldexp(1, -126)), float32(math.Ldexp(1, -133))} {
		f := types.BlockScaledNVFP4
		rnd := func(i int) byte { return byte(r.Intn(16)) }
		cases = append(cases, tc{name: fmt.Sprintf("vecf4 global %g", g), f: f,
			rows:  [][]byte{handBlockScaledCell(f, 32, g, []byte{0x7e, 0x38}, codes(32, rnd)), handBlockScaledCell(f, 32, g, []byte{0x01, 0x7e}, codes(32, rnd))},
			query: [][]byte{handBlockScaledCell(f, 32, g, []byte{0x7e, 0x7e}, codes(32, rnd)), handBlockScaledCell(f, 32, 1, []byte{0x7e, 0x7e}, codes(32, rnd))}})
	}
	for _, s := range []byte{0, 1, 5, 20, 120} {
		f := types.BlockScaledMXFP8
		cases = append(cases, tc{name: fmt.Sprintf("vecf8 e8m0 %d", s), f: f,
			rows: [][]byte{
				handBlockScaledCell(f, 32, 1, []byte{s}, codes(32, func(i int) byte { return byte(1 + i%7) })),
				handBlockScaledCell(f, 32, 1, []byte{s}, codes(32, func(i int) byte { return byte(0x38 + i%8) })),
				handBlockScaledCell(f, 32, 1, []byte{s}, codes(32, func(i int) byte { return []byte{0x01, 0x7e}[i%2] })),
				handBlockScaledCell(f, 32, 1, []byte{s}, codes(32, func(i int) byte { return byte(0x80 | (1 + i%7)) })),
			},
			query: [][]byte{
				handBlockScaledCell(f, 32, 1, []byte{s}, codes(32, func(i int) byte { return byte(1 + (i+3)%7) })),
				handBlockScaledCell(f, 32, 1, []byte{s}, codes(32, func(i int) byte { return 0x38 })),
			}})
	}

	metrics := []struct {
		gpu int
		cpu metric.MetricType
	}{
		{cuvs.BlockScaledMatmulInnerProduct, metric.Metric_InnerProduct},
		{cuvs.BlockScaledMatmulCosine, metric.Metric_CosineDistance},
		{cuvs.BlockScaledMatmulL2sq, metric.Metric_L2sqDistance},
	}
	parse := func(cells [][]byte) ([]types.BlockScaledCell, []byte) {
		var out []types.BlockScaledCell
		var packed []byte
		for _, b := range cells {
			c, err := types.ParseBlockScaledCell(b)
			require.NoError(t, err)
			out = append(out, c)
			packed = append(packed, b...)
		}
		return out, packed
	}
	inDomain := func(v []float64) bool {
		for _, x := range v {
			if x != 0 && math.Abs(x) < 0x1p-126 {
				return false
			}
		}
		return true
	}
	checked := 0
	for _, c := range cases {
		rows, rowCells := parse(c.rows)
		qs, qCells := parse(c.query)
		dim := rows[0].Dim
		for _, mt := range metrics {
			eng, err := cuvs.NewBlockScaledMatmul(int(c.f), dim, len(qs), qCells, types.BlockScaledCellSize(c.f, dim), 256, 0, mt.gpu)
			require.NoError(t, err)
			scores := make([]float32, len(rows)*len(qs))
			require.NoError(t, eng.Run(rowCells, scores))
			eng.Close()
			cpuFn, err := metric.VecBlockSQLDistance(mt.cpu)
			require.NoError(t, err)
			for i := range rows {
				for j := range qs {
					xv, qv := exactBlockScaled(rows[i]), exactBlockScaled(qs[j])
					if !inDomain(xv) || !inDomain(qv) {
						continue
					}
					// squared L2 of a row and an equal query is the GEMM expansion near 0 (Decisions)
					if mt.cpu == metric.Metric_L2sqDistance && slices.Equal(rows[i].Elems, qs[j].Elems) &&
						slices.Equal(rows[i].Scales, qs[j].Scales) && rows[i].Global == qs[j].Global {
						continue
					}
					var dot, nx, nq, l2, mag float64
					for k := range xv {
						dot += xv[k] * qv[k]
						nx += xv[k] * xv[k]
						nq += qv[k] * qv[k]
						l2 += (xv[k] - qv[k]) * (xv[k] - qv[k])
						mag += math.Abs(xv[k] * qv[k])
					}
					want, tol := -dot, 1e-4*mag
					switch mt.cpu {
					case metric.Metric_CosineDistance:
						want, tol = 1, 1e-4
						if nx > 0 && nq > 0 {
							want = 1 - math.Max(-1, math.Min(1, dot/math.Sqrt(nx*nq)))
						}
					case metric.Metric_L2sqDistance:
						want, tol = l2, 1e-4*(nx+nq)
					}
					x, y := metric.VecBlockOperand{Cell: rows[i]}, metric.VecBlockOperand{Cell: qs[j]}
					cpu, err := cpuFn(&x, &y)
					require.NoError(t, err)
					gpu := -float64(scores[i*len(qs)+j])
					msg := fmt.Sprintf("%s metric %d row %d query %d: exact %g cpu %g gpu %g", c.name, mt.cpu, i, j, want, cpu, gpu)
					switch {
					case math.Abs(want) > 1.01*math.MaxFloat32:
						require.True(t, math.IsInf(cpu, 0), msg)
						require.True(t, math.IsInf(gpu, 0), msg)
					case math.Abs(want) > math.MaxFloat32:
					case want != 0 && math.Abs(want) < 0x1p-126:
						require.Less(t, math.Abs(cpu), 0x1p-100, msg)
						require.Less(t, math.Abs(gpu), 0x1p-100, msg)
					default:
						require.InDelta(t, want, cpu, tol, msg)
						require.InDelta(t, want, gpu, tol, msg)
					}
					checked++
				}
			}
		}
	}
	require.Greater(t, checked, 1000)
}
