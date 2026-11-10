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

package order

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestScanCutoffWitnessesAndConcurrency(t *testing.T) {
	for _, desc := range []bool{false, true} {
		t.Run(map[bool]string{false: "ascending", true: "descending"}[desc], func(t *testing.T) {
			mp := mpool.MustNewZero()
			inputs := make([]*vector.Vector, 0, 4)
			all := make([]int32, 0, 4*17)
			for worker := range 4 {
				v := vector.NewVec(types.T_int32.ToType())
				for i := range 17 {
					x := int32((worker*19+i*13)%53 - 26)
					require.NoError(t, vector.AppendFixed(v, x, false, mp))
					all = append(all, x)
				}
				inputs = append(inputs, v)
			}
			t.Cleanup(func() {
				for _, v := range inputs {
					v.Free(mp)
				}
				require.Zero(t, mp.CurrNB())
			})
			const k = 30 // each worker has fewer than K; the scope has enough
			cutoff := NewScanCutoff(k, types.T_int32.ToType(), desc)
			published := make(chan objectio.ZoneMap, len(inputs))
			var workers sync.WaitGroup
			for _, v := range inputs {
				workers.Go(func() {
					if bound, ok := cutoff.Observe(v, v.Length()); ok {
						published <- bound
					}
				})
			}
			workers.Wait()
			close(published)
			var values []objectio.ZoneMap
			for bound := range published {
				values = append(values, bound)
			}
			require.Len(t, values, 1)
			limit := types.DecodeInt32(values[0].GetMinBuf())
			witnesses := 0
			for _, x := range all {
				if (!desc && x <= limit) || (desc && x >= limit) {
					witnesses++
				}
			}
			require.GreaterOrEqual(t, witnesses, k)
			require.True(t, cutoff.Done())
		})
	}
}

func TestScanCutoffSortedOracle(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(32061))
	type row struct {
		key       int32
		secondary int
	}
	pruned := 0
	for trial := range 300 {
		desc := trial%2 == 0
		rows := 1 + rng.Intn(200)
		k := 1 + rng.Intn(rows+20)
		values := make([]row, rows)
		secondary := rng.Perm(rows)
		for i := range values {
			values[i] = row{key: int32(rng.Intn(41) - 20), secondary: secondary[i]} // deliberately many ties
		}
		cutoff := NewScanCutoff(uint64(k), types.T_int32.ToType(), desc)
		var bound int32
		publishedAt := 0
		var kept []row
		for i := 0; i < len(values); {
			end := min(len(values), i+1+rng.Intn(13))
			if publishedAt == 0 {
				v := vector.NewVec(types.T_int32.ToType())
				keys := make([]int32, end-i)
				for j, x := range values[i:end] {
					keys[j] = x.key
				}
				if trial%3 == 0 {
					sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
				}
				require.NoError(t, vector.AppendFixedList(v, keys, nil, mp))
				v.SetSorted(trial%3 == 0)
				b, ok := cutoff.Observe(v, end-i)
				v.Free(mp)
				kept = append(kept, values[i:end]...)
				if ok {
					bound, publishedAt = types.DecodeInt32(b.GetMinBuf()), end
				}
			} else {
				// A block is discarded only if every primary key is strictly
				// worse. Equality must retain secondary-key winners.
				worse := true
				for _, x := range values[i:end] {
					if (!desc && x.key <= bound) || (desc && x.key >= bound) {
						worse = false
						break
					}
				}
				if worse {
					pruned++
				} else {
					kept = append(kept, values[i:end]...)
				}
			}
			i = end
		}
		if k > rows {
			require.Zero(t, publishedAt)
			continue
		}
		require.Positive(t, publishedAt)
		witnesses := 0
		for _, x := range values[:publishedAt] {
			if (!desc && x.key <= bound) || (desc && x.key >= bound) {
				witnesses++
			}
		}
		require.GreaterOrEqual(t, witnesses, k, "only rows observed at publication count")
		sorted := append([]row(nil), values...)
		less := func(rs []row, i, j int) bool {
			if rs[i].key == rs[j].key {
				return rs[i].secondary < rs[j].secondary
			}
			if desc {
				return rs[i].key > rs[j].key
			}
			return rs[i].key < rs[j].key
		}
		sort.Slice(sorted, func(i, j int) bool { return less(sorted, i, j) })
		sort.Slice(kept, func(i, j int) bool { return less(kept, i, j) })
		require.GreaterOrEqual(t, len(kept), k)
		require.Equal(t, sorted[:k], kept[:k], "pruning must preserve the full two-key order")
	}
	require.Positive(t, pruned, "the oracle must exercise real block pruning")
	require.Zero(t, mp.CurrNB())
}

func TestScanCutoffRepresentations(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_int32.ToType()
	constant, err := vector.NewConstFixed(typ, int32(7), 5, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	cutoff := NewScanCutoff(5, typ, false)
	bound, ok := cutoff.Observe(constant, 5)
	require.True(t, ok)
	require.Equal(t, int32(7), types.DecodeInt32(bound.GetMinBuf()))

	invalid := vector.NewConstNull(typ, 2, mp)
	defer invalid.Free(mp)
	cutoff = NewScanCutoff(3, typ, false)
	_, ok = cutoff.Observe(invalid, 2)
	require.False(t, ok)
	require.True(t, cutoff.Done())
	_, ok = cutoff.Observe(constant, 5)
	require.False(t, ok)

	cutoff = NewScanCutoff(3, typ, false)
	_, ok = cutoff.Observe(constant, 4) // mismatched logical row count
	require.False(t, ok)
	require.True(t, cutoff.Done())
	require.True(t, NewScanCutoff(0, typ, false).Done())
}

func TestScanCutoffOutwardStringBounds(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	typ := types.T_varbinary.ToType()
	for _, desc := range []bool{false, true} {
		for _, sorted := range []bool{false, true} {
			name := map[bool]string{false: "constant", true: "sorted"}[sorted] + "/" +
				map[bool]string{false: "ascending", true: "descending"}[desc]
			t.Run(name, func(t *testing.T) {
				value := append(bytes.Repeat([]byte{'x'}, 30), []byte("tail")...)
				minimum := value
				var v *vector.Vector
				defer func() {
					if v != nil {
						v.Free(mp)
					}
				}()
				if sorted {
					v = vector.NewVec(typ)
					require.NoError(t, vector.AppendBytesList(v, [][]byte{nil, []byte("small"), value}, nil, mp))
					v.SetSorted(true)
					minimum = nil
				} else {
					var err error
					v, err = vector.NewConstBytes(typ, value, 3, mp)
					require.NoError(t, err)
				}
				cutoff := NewScanCutoff(3, typ, desc)
				bound, ok := cutoff.Observe(v, 3)
				require.True(t, ok)
				if desc {
					require.LessOrEqual(t, bytes.Compare(bound.GetMinBuf(), minimum), 0)
				} else {
					require.GreaterOrEqual(t, bytes.Compare(bound.GetMaxBuf(), value), 0)
				}
			})
		}
	}
	value := bytes.Repeat([]byte{0xff}, 31)
	v, err := vector.NewConstBytes(typ, value, 1, mp)
	require.NoError(t, err)
	defer v.Free(mp)
	cutoff := NewScanCutoff(1, typ, false)
	_, ok := cutoff.Observe(v, 1)
	require.False(t, ok)
	require.True(t, cutoff.Done())

	sorted := vector.NewVec(typ)
	defer sorted.Free(mp)
	require.NoError(t, vector.AppendBytes(sorted, value, false, mp))
	sorted.SetSorted(true)
	cutoff = NewScanCutoff(1, typ, false)
	_, ok = cutoff.Observe(sorted, 1)
	require.False(t, ok, "sorted metadata cannot make an unbounded string maximum safe")
	require.True(t, cutoff.Done())
}

func BenchmarkScanCutoffObserve(b *testing.B) {
	const rows = 8192
	for _, oid := range []types.T{types.T_int64, types.T_varbinary} {
		b.Run(oid.String(), func(b *testing.B) {
			mp := mpool.MustNewZero()
			v := vector.NewVec(oid.ToType())
			b.Cleanup(func() {
				v.Free(mp)
				require.Zero(b, mp.CurrNB())
				mpool.DeleteMPool(mp)
			})
			for i := range rows {
				if oid == types.T_int64 {
					require.NoError(b, vector.AppendFixed(v, int64(i), false, mp))
				} else {
					value := binary.BigEndian.AppendUint64(bytes.Repeat([]byte{'x'}, 56), uint64(i))
					require.NoError(b, vector.AppendBytes(v, value, false, mp))
				}
			}
			for _, sorted := range []bool{false, true} {
				name := "unmarked"
				if sorted {
					name = "sorted"
				}
				b.Run(name, func(b *testing.B) {
					v.SetSorted(sorted)
					cutoff := NewScanCutoff(math.MaxUint64, oid.ToType(), false)
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						cutoff.Observe(v, rows)
					}
					b.StopTimer()
					require.False(b, cutoff.Done(), "benchmark must keep collecting without pruning")
				})
			}
		})
	}
}

func TestScanCutoffTypeAdmission(t *testing.T) {
	require.True(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_int32), NotNullable: true}))
	require.False(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_int32)}))
	require.False(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_float64), NotNullable: true}))
	require.True(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_varchar), NotNullable: true, Charset: uint32(types.CharsetLegacy)}))
	require.False(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_varchar), NotNullable: true, Charset: uint32(types.CharsetUTF8MB4Bin)}))
	require.False(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_varchar), NotNullable: true, Charset: 255}))
	require.False(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_varchar), NotNullable: true, PadSpace: true}))
}
