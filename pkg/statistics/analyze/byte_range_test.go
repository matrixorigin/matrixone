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

package analyze

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/stretchr/testify/require"
)

func TestByteRangeSampler(t *testing.T) {
	seed := [32]byte{7}
	sample := func(n, k int, reverse bool) *statsinfo.ByteRangeDistribution {
		s := NewByteRangeSampler(k)
		for i := 0; i < n; i++ {
			row := i
			if reverse {
				row = n - 1 - i
			}
			var id [types.RowidSize]byte
			binary.BigEndian.PutUint64(id[:], uint64(row))
			value := []byte{"a"[0] + byte(row%3)}
			require.NoError(t, s.Observe(ByteRangePriority(seed, id), id[:], value, row%4 == 0))
		}
		require.LessOrEqual(t, len(s.rows), k)
		return s.Distribution(uint64(n))
	}
	for _, n := range []int{0, 1, 8, 9, 100} {
		a, b := sample(n, 8, false), sample(n, 8, true)
		require.Equal(t, a, b, "traversal order, n=%d", n)
		if n > 0 {
			require.Equal(t, n <= 8, a.CompleteSample)
			require.Equal(t, uint64(min(n, 8)), a.SampleRows)
		}
	}
	require.Nil(t, sample(9, 0, false))
	census := sample(8, 8, false)
	require.Len(t, census.Values, 6, "NULLs occupy rows; repeated non-NULLs retain their mass")
	require.Equal(t, [][]byte{[]byte("a"), []byte("a"), []byte("b"), []byte("b"), []byte("c"), []byte("c")}, census.Values)
	upper, ok := ByteRangeUpper(census, []byte("a"), []byte("b"))
	require.True(t, ok)
	require.Equal(t, 0.25, upper)
	var id [types.RowidSize]byte
	s := NewByteRangeSampler(1)
	raw := bytes.Repeat([]byte("x"), 100000)
	require.NoError(t, s.Observe(ByteRangePriority(seed, id), id[:], raw, false))
	d := s.Distribution(1)
	require.Len(t, d.Values[0], ByteRangeValueBytes)
	raw[0] = 'z'
	require.Equal(t, byte('x'), d.Values[0][0], "sample owns its bounded copy")
	d.Values[0][0] = 'y'
	require.Equal(t, byte('x'), s.Distribution(1).Values[0][0], "publication owns its copy")
	require.Error(t, s.Observe([16]byte{}, nil, nil, false))
	require.NotEqual(t, hash128(seed, domainRowSelection, id[:]), ByteRangePriority(seed, id))
	require.Equal(t, hash128(seed, "analyze/byte-range/v1", id[:]), ByteRangePriority(seed, id), "fixed framing preserves priority identity")
	for c := 1; c <= 20000; c++ {
		require.LessOrEqual(t, c*ByteRangeSampleCapacity(c), byteRangeTableSlots)
	}
	require.Zero(t, ByteRangeSampleCapacity(0))
}

func TestByteRangeHalfOpenContract(t *testing.T) {
	// Independent raw-byte census, with duplicates, NULL, and values extending
	// exactly equal 64-byte edges. Classify before truncation as the oracle.
	a := bytes.Repeat([]byte("a"), 64)
	b := bytes.Repeat([]byte("b"), 64)
	raw := [][]byte{{}, {0}, {0, 255}, []byte("a"), []byte("a"), a, append(slices.Clone(a), 255), b, append(slices.Clone(b), 0), {255}, []byte("中文")}
	d := &statsinfo.ByteRangeDistribution{Version: ByteRangeVersion, PopulationRows: uint64(len(raw) + 1), SampleRows: uint64(len(raw) + 1), CompleteSample: true}
	for _, v := range raw {
		d.Values = append(d.Values, slices.Clone(v[:min(len(v), 64)]))
	}
	slices.SortFunc(d.Values, bytes.Compare)
	bounds := [][]byte{{}, {0}, {0, 255}, []byte("a"), a, b, {255}, []byte("中"), []byte("文")}
	for _, lo := range bounds {
		for _, hi := range bounds {
			if bytes.Compare(lo, hi) >= 0 {
				continue
			}
			count := 0
			for _, v := range raw {
				if bytes.Compare(v, lo) >= 0 && bytes.Compare(v, hi) < 0 {
					count++
				}
			}
			upper, ok := ByteRangeUpper(d, lo, hi)
			require.True(t, ok)
			require.Equal(t, float64(count)/float64(len(raw)+1), upper, "[%q,%q)", lo, hi)
		}
	}
	d.CompleteSample = false
	upper, ok := ByteRangeUpper(d, []byte("a"), []byte("b"))
	require.True(t, ok)
	require.Equal(t, min(1, 4.0/12+2*math.Sqrt(math.Log(200)/(2*12))), upper)
	for _, mutate := range []func(*statsinfo.ByteRangeDistribution){
		func(d *statsinfo.ByteRangeDistribution) { d.Version++ },
		func(d *statsinfo.ByteRangeDistribution) { d.SampleRows = 0 },
		func(d *statsinfo.ByteRangeDistribution) { d.SampleRows = 2049; d.PopulationRows = 3000 },
		func(d *statsinfo.ByteRangeDistribution) { d.SampleRows = d.PopulationRows + 1 },
		func(d *statsinfo.ByteRangeDistribution) { d.Values = [][]byte{[]byte("z"), []byte("a")} },
		func(d *statsinfo.ByteRangeDistribution) { d.Values = [][]byte{bytes.Repeat([]byte("x"), 65)} },
		func(d *statsinfo.ByteRangeDistribution) { d.CompleteSample = true; d.PopulationRows++ },
	} {
		bad := *d
		mutate(&bad)
		_, ok := ByteRangeUpper(&bad, []byte("a"), []byte("b"))
		require.False(t, ok)
	}
	_, ok = ByteRangeUpper(d, a, append(slices.Clone(b), 'b'))
	require.False(t, ok)
	allNull := &statsinfo.ByteRangeDistribution{Version: 1, PopulationRows: 10, SampleRows: 10, CompleteSample: true}
	upper, ok = ByteRangeUpper(allNull, []byte{}, []byte{255})
	require.True(t, ok)
	require.Zero(t, upper)
}

func BenchmarkByteRangePriority(b *testing.B) {
	var seed [32]byte
	var id types.Rowid
	b.ReportAllocs()
	for b.Loop() {
		id[0]++
		_ = ByteRangePriority(seed, id)
	}
}
