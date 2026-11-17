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
		for _, nullsLead := range []bool{false, true} {
			name := map[bool]string{false: "ascending", true: "descending"}[desc] + "/" +
				map[bool]string{false: "nulls-trail", true: "nulls-lead"}[nullsLead]
			t.Run(name, func(t *testing.T) {
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
				cutoff := NewScanCutoff(k, types.T_int32.ToType(), desc, nullsLead)
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
		nullsLead := !desc
		rows := 1 + rng.Intn(200)
		k := 1 + rng.Intn(rows+20)
		values := make([]row, rows)
		secondary := rng.Perm(rows)
		for i := range values {
			values[i] = row{key: int32(rng.Intn(41) - 20), secondary: secondary[i]} // deliberately many ties
		}
		cutoff := NewScanCutoff(uint64(k), types.T_int32.ToType(), desc, nullsLead)
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
	cutoff := NewScanCutoff(5, typ, false, true)
	bound, ok := cutoff.Observe(constant, 5)
	require.True(t, ok)
	require.Equal(t, int32(7), types.DecodeInt32(bound.GetMinBuf()))

	invalid := vector.NewConstNull(typ, 2, mp)
	defer invalid.Free(mp)

	// An all-NULL batch certifies its NULL rows under a leading placement
	// but contributes no bound; unlike the previous wholesale-disable rule,
	// later usable batches may still publish.
	cutoff = NewScanCutoff(3, typ, false, true)
	_, ok = cutoff.Observe(invalid, 2)
	require.False(t, ok)
	require.False(t, cutoff.Done(), "class-(b) batches no longer disable the summary")
	require.Equal(t, uint64(2), cutoff.certified)
	_, ok = cutoff.Observe(constant, 5)
	require.True(t, ok, "witnesses certified structurally by NULL rows count toward K")

	// The same stream under a trailing placement certifies no NULL row.
	cutoff = NewScanCutoff(3, typ, false, false)
	_, ok = cutoff.Observe(invalid, 2)
	require.False(t, ok)
	require.False(t, cutoff.Done())
	require.Zero(t, cutoff.certified)
	_, ok = cutoff.Observe(invalid, 2)
	require.False(t, ok)
	require.False(t, cutoff.Done(), "a never-certifying stream never publishes")
	_, ok = cutoff.Observe(constant, 5)
	require.True(t, ok)

	// A length-mismatched batch is an unusable non-NULL batch: its non-NULL
	// rows never count and it contributes no bound, but it does not disable.
	cutoff = NewScanCutoff(3, typ, false, true)
	_, ok = cutoff.Observe(constant, 4)
	require.False(t, ok)
	require.False(t, cutoff.Done(), "class-(c) batches no longer disable the summary")
	require.Zero(t, cutoff.certified)
	_, ok = cutoff.Observe(constant, 5)
	require.True(t, ok)

	// Empty batches witness nothing.
	cutoff = NewScanCutoff(3, typ, false, true)
	_, ok = cutoff.Observe(constant, 0)
	require.False(t, ok)
	require.False(t, cutoff.Done())
	require.Zero(t, cutoff.certified)

	// Structurally invalid key vectors still disable outright.
	cutoff = NewScanCutoff(3, typ, false, true)
	_, ok = cutoff.Observe(nil, 5)
	require.False(t, ok)
	require.True(t, cutoff.Done(), "a nil key vector proves an invalid scope")
	_, ok = cutoff.Observe(constant, 5)
	require.False(t, ok)

	require.True(t, NewScanCutoff(0, typ, false, true).Done())
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
				cutoff := NewScanCutoff(3, typ, desc, false)
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

	// Truncated maxima are usable in no direction under ASC counting: they
	// publish nothing, their non-NULL rows never certify, and the summary
	// stays alive for any later usable batch.
	value := bytes.Repeat([]byte{0xff}, 31)
	v, err := vector.NewConstBytes(typ, value, 1, mp)
	require.NoError(t, err)
	defer v.Free(mp)
	cutoff := NewScanCutoff(1, typ, false, true)
	_, ok := cutoff.Observe(v, 1)
	require.False(t, ok)
	require.False(t, cutoff.Done(), "a truncated batch must leave the summary alive")
	require.Zero(t, cutoff.certified, "class-(c) non-NULL rows never certify")

	small := vector.NewVec(typ)
	defer small.Free(mp)
	require.NoError(t, vector.AppendBytes(small, []byte("m"), false, mp))
	require.NoError(t, vector.AppendBytes(small, []byte("m"), false, mp))
	cutoff = NewScanCutoff(1, typ, false, true)
	_, ok = cutoff.Observe(v, 1)
	require.False(t, ok)
	require.False(t, cutoff.Done(), "class-(c) before the first usable bound must not disable")
	// The truncated batch never certified, so a single usable row is enough.
	bound, ok := cutoff.Observe(small, 2)
	require.True(t, ok, "a usable batch after a class-(c) batch publishes")
	require.Equal(t, []byte("m"), bound.GetMaxBuf())
	require.True(t, cutoff.Done())

	sorted := vector.NewVec(typ)
	defer sorted.Free(mp)
	require.NoError(t, vector.AppendBytes(sorted, value, false, mp))
	sorted.SetSorted(true)
	cutoff = NewScanCutoff(1, typ, false, true)
	_, ok = cutoff.Observe(sorted, 1)
	require.False(t, ok, "sorted metadata cannot make an unbounded string maximum safe")
	require.False(t, cutoff.Done())
	require.Zero(t, cutoff.certified)

	// A batch with NULLs keeps its raw extremum fast paths disabled: the
	// extremum must come from the NULL-skipping scan, never from a NULL
	// row's raw bytes.
	sortedNulls := vector.NewVec(typ)
	defer sortedNulls.Free(mp)
	require.NoError(t, vector.AppendBytes(sortedNulls, nil, true, mp))
	require.NoError(t, vector.AppendBytes(sortedNulls, value, false, mp))
	require.NoError(t, vector.AppendBytes(sortedNulls, []byte("mid"), false, mp))
	sortedNulls.SetSorted(true)
	cutoff = NewScanCutoff(1, typ, false, true)
	_, ok = cutoff.Observe(sortedNulls, 3)
	require.False(t, ok, "truncated extrema never certify, even with NULL rows present")
	require.Equal(t, uint64(1), cutoff.certified, "the NULL row certified structurally under L")
	require.False(t, cutoff.Done())
	_, ok = cutoff.Observe(small, 2)
	require.True(t, ok, "certified NULL witnesses plus a usable bound publish")
	require.True(t, cutoff.Done())
}

func TestScanCutoffTruncatedCounterexampleShape(t *testing.T) {
	// Reviewer counterexample from the design's certified-witness section:
	// two batches whose stored maxima are truncations of a repeated huge
	// value, then a mid-value batch. Counting class-(c) non-NULL rows would
	// publish the mid-value bound with fewer than K rows provably at or
	// head-ward of it, and a later zero-null block entirely beyond that
	// bound would be skipped although it supplies the true page.
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	typ := types.T_varbinary.ToType()
	huge := bytes.Repeat([]byte{0xff}, 31)
	mid := []byte("mid")

	truncBatch := func() *vector.Vector {
		v := vector.NewVec(typ)
		require.NoError(t, vector.AppendBytes(v, huge, false, mp))
		require.NoError(t, vector.AppendBytes(v, huge, false, mp))
		require.NoError(t, vector.AppendBytes(v, huge, false, mp))
		return v
	}
	smallBatch := func(rows int) *vector.Vector {
		v := vector.NewVec(typ)
		require.NoError(t, vector.AppendBytes(v, mid, false, mp))
		for range rows - 1 {
			require.NoError(t, vector.AppendBytes(v, []byte("p"), false, mp))
		}
		return v
	}

	const k = 6
	cutoff := NewScanCutoff(k, typ, false, false)
	for range 2 {
		v := truncBatch()
		_, ok := cutoff.Observe(v, 3)
		v.Free(mp)
		require.False(t, ok)
		require.False(t, cutoff.Done())
	}
	require.Zero(t, cutoff.certified, "six truncated rows certify nothing")
	require.False(t, cutoff.initialized, "no bound may derive from an unusable batch")
	v := smallBatch(1)
	_, ok := cutoff.Observe(v, 1)
	v.Free(mp)
	require.False(t, ok, "the mid-value bound cannot publish over uncertified rows")
	require.False(t, cutoff.Done())
	require.Equal(t, uint64(1), cutoff.certified)
	require.True(t, cutoff.initialized, "a usable batch initializes the running bound without publishing it")

	// Only usable batches can certify non-NULL rows, so the summary needs
	// enough of them; the page-supplying blocks beyond every published bound
	// (all rows greater than it) are the ones never skipped in error.
	cutoff = NewScanCutoff(k, typ, false, false)
	for i, rows := range []int{1, 2, 3} {
		v := smallBatch(rows)
		_, ok := cutoff.Observe(v, rows)
		v.Free(mp)
		if i < 2 {
			require.False(t, ok, "witnesses below K cannot publish")
		} else {
			require.True(t, ok, "usable batches alone certify the sixth witness")
		}
	}
	require.True(t, cutoff.Done())

	// Under a leading placement, certified NULL rows can carry K, but the
	// bound itself still has to come from a usable batch.
	nulls := vector.NewVec(typ)
	defer nulls.Free(mp)
	for range 6 {
		require.NoError(t, vector.AppendBytes(nulls, nil, true, mp))
	}
	cutoff = NewScanCutoff(k, typ, false, true)
	_, ok = cutoff.Observe(nulls, 6)
	require.False(t, ok, "K certified NULL witnesses publish nothing without a usable bound")
	require.False(t, cutoff.Done())
	require.Equal(t, uint64(k), cutoff.certified)
	v = smallBatch(1)
	bound, ok := cutoff.Observe(v, 1)
	v.Free(mp)
	require.True(t, ok, "NULL witnesses plus a usable extremum publish")
	require.Equal(t, mid, bound.GetMaxBuf())
	require.True(t, cutoff.Done())
}

func TestScanCutoffUninitializableDecimal256(t *testing.T) {
	// Decimal256 zone maps cannot initialize (fixed ZM layout cannot store
	// the payload), so every non-NULL batch is class (c): no bound, no
	// non-NULL witnesses, and the stream fails open.
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	typ := types.T_decimal256.ToType()
	batch := func(nulls bool) *vector.Vector {
		v := vector.NewVec(typ)
		for range 4 {
			d := types.Decimal256{}
			require.NoError(t, vector.AppendFixed(v, d, nulls, mp))
		}
		return v
	}
	for _, nullsLead := range []bool{false, true} {
		cutoff := NewScanCutoff(4, typ, false, nullsLead)
		v := batch(false)
		_, ok := cutoff.Observe(v, 4)
		v.Free(mp)
		require.False(t, ok)
		require.False(t, cutoff.Done(), "class (c) keeps the summary alive")
		require.Zero(t, cutoff.certified, "non-NULL decimal256 rows never certify")
		require.False(t, cutoff.initialized)
		v = batch(true)
		_, ok = cutoff.Observe(v, 4)
		v.Free(mp)
		if nullsLead {
			require.Equal(t, uint64(4), cutoff.certified, "NULL rows certify structurally under L")
			require.False(t, ok, "all-NULL visible rows certify nothing publishable")
		} else {
			require.Zero(t, cutoff.certified)
		}
		require.False(t, cutoff.initialized, "no usable bound was ever seen")
	}
}

func TestScanCutoffNullableWitnessOracle(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	rng := rand.New(rand.NewSource(32469))
	type nrow struct {
		key  int32
		null bool
	}
	type nblock struct {
		rows []nrow
	}
	var nullsLead, keyDesc bool
	rankLess := func(a, b nrow) bool {
		if a.null && b.null {
			return false
		}
		if a.null {
			return nullsLead
		}
		if b.null {
			return !nullsLead
		}
		if keyDesc {
			return a.key > b.key
		}
		return a.key < b.key
	}
	page := func(rows []nrow, k int) map[nrow]int {
		sort.SliceStable(rows, func(i, j int) bool { return rankLess(rows[i], rows[j]) })
		if len(rows) > k {
			rows = rows[:k]
		}
		counts := map[nrow]int{}
		for _, r := range rows {
			counts[r]++
		}
		return counts
	}
	publications, skippedBlocks := 0, 0
	for trial := range 600 {
		keyDesc = trial%2 == 0
		nullsLead = trial%4 < 2
		var blocks []nblock
		rows := 0
		for b := range 1 + rng.Intn(6) {
			size := 1 + rng.Intn(9)
			blk := nblock{rows: make([]nrow, 0, size)}
			for range size {
				null := rng.Intn(4) == 0
				key := int32(b*200 + rng.Intn(150) - 10)
				blk.rows = append(blk.rows, nrow{key: key, null: null})
			}
			// Head-concentrated clean tails make the leading placement's
			// null-count skip structurally provable on some trials.
			if trial%2 == 0 && b > 1 {
				for i := range blk.rows {
					blk.rows[i].null = false
				}
			}
			blocks = append(blocks, blk)
			rows += len(blk.rows)
		}
		if rows == 0 {
			continue
		}
		k := 1 + rng.Intn(rows*3/2)
		cutoff := NewScanCutoff(uint64(k), types.T_int32.ToType(), keyDesc, nullsLead)
		var bound int32
		published := false
		var kept []nrow
		expectedCertified := uint64(0)
		for _, blk := range blocks {
			if published {
				// Receiver rule: an interval skip under a leading placement
				// additionally keeps any block carrying NULLs.
				skip := true
				for _, r := range blk.rows {
					if r.null {
						if nullsLead {
							skip = false
							break
						}
						continue
					}
					if (!keyDesc && r.key <= bound) || (keyDesc && r.key >= bound) {
						skip = false
						break
					}
				}
				if skip {
					skippedBlocks++
					continue
				}
				kept = append(kept, blk.rows...)
				continue
			}
			v := vector.NewVec(types.T_int32.ToType())
			nulls := 0
			for _, r := range blk.rows {
				require.NoError(t, vector.AppendFixed(v, r.key, r.null, mp))
				if r.null {
					nulls++
				}
			}
			b, ok := cutoff.Observe(v, v.Length())
			v.Free(mp)
			kept = append(kept, blk.rows...)
			// Per-class certified counting asserted against the internal
			// counter each step, until saturation at the limit hides the
			// per-batch split.
			expected := 0
			if nulls < len(blk.rows) {
				expected = len(blk.rows) - nulls
				if nullsLead {
					expected = len(blk.rows)
				}
			} else if nullsLead {
				expected = nulls
			}
			expectedCertified += uint64(expected)
			if cutoff.certified < uint64(k) {
				require.Equal(t, min(expectedCertified, uint64(k)), cutoff.certified, "per-class witness counting")
			}
			if ok {
				publications++
				published = true
				require.True(t, cutoff.initialized, "publication requires an initialized bound")
				if keyDesc {
					bound = types.DecodeInt32(b.GetMinBuf())
				} else {
					bound = types.DecodeInt32(b.GetMaxBuf())
				}
				// Witness inequality, asserted directly: at publication at
				// least K observed rows provably rank at or head-ward of the
				// bound. NULL rows certify structurally only under a
				// leading placement; values certify through themselves.
				witnesses := 0
				for _, r := range kept {
					if r.null {
						if nullsLead {
							witnesses++
						}
						continue
					}
					if (!keyDesc && r.key <= bound) || (keyDesc && r.key >= bound) {
						witnesses++
					}
				}
				require.GreaterOrEqual(t, witnesses, k, "every published bound carries K provable witnesses")
			} else {
				require.False(
					t, cutoff.initialized && cutoff.certified >= uint64(k),
					"no publication without a usable bound and K certified witnesses")
			}
		}
		if k > rows {
			require.False(t, published, "a scope wider than the input never certifies K")
		}
		full := make([]nrow, 0, rows)
		for _, blk := range blocks {
			full = append(full, blk.rows...)
		}
		require.Equal(t, page(full, k), page(kept, k), "pruning must never change the page")
	}
	require.Positive(t, publications, "the oracle must exercise publication")
	require.Positive(t, skippedBlocks, "the oracle must exercise real block skipping")
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
					cutoff := NewScanCutoff(math.MaxUint64, oid.ToType(), false, true)
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
	require.True(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_int32)}), "nullable keys are admitted")
	require.False(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_float64), NotNullable: true}))
	require.True(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_varchar), NotNullable: true, Charset: uint32(types.CharsetLegacy)}))
	require.True(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetLegacy)}))
	require.False(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_varchar), NotNullable: true, Charset: uint32(types.CharsetUTF8MB4Bin)}))
	require.False(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_varchar), NotNullable: true, Charset: 255}))
	require.False(t, SupportsScanCutoffType(plan.Type{Id: int32(types.T_varchar), NotNullable: true, PadSpace: true}))
}
