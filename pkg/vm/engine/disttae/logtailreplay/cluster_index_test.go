// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtailreplay

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
)

// encodeIssueClusterKey builds the raw serial bytes of the reported
// four-part composite cluster key (int32 tenant, int32 device, int32 metric,
// int64 ts-ordinal), matching the serial_full pre-insert projection.
func encodeIssueClusterKey(
	packer *types.Packer,
	tenant, device, metric int32,
	tsOrdinal int64,
) []byte {
	packer.Reset()
	packer.EncodeInt32(tenant)
	packer.EncodeInt32(device)
	packer.EncodeInt32(metric)
	packer.EncodeInt64(tsOrdinal)
	return packer.Bytes()
}

// encodeIssueClusterPrefix builds the raw serial bytes of the leading three
// parts, the bound the planner derives for a range predicate on the last
// cluster part.
func encodeIssueClusterPrefix(
	packer *types.Packer,
	tenant, device, metric int32,
) []byte {
	packer.Reset()
	packer.EncodeInt32(tenant)
	packer.EncodeInt32(device)
	packer.EncodeInt32(metric)
	return packer.Bytes()
}

// encodeVarlenClusterKey builds a two-part key (int32, varchar). The varchar
// part may contain 0x00 bytes to exercise the packer's escape encoding.
func encodeVarlenClusterKey(
	packer *types.Packer,
	a int32,
	s string,
) []byte {
	packer.Reset()
	packer.EncodeInt32(a)
	packer.EncodeStringType([]byte(s))
	return packer.Bytes()
}

// encodeClusterKeyForFilter wraps raw serial bytes into the encoded-key
// domain of the in-memory indexes; prefix predicates additionally strip the
// trailing terminator exactly like the mem filter does.
func encodeClusterKeyForFilter(
	packer *types.Packer,
	serial []byte,
	stripped bool,
) []byte {
	packer.Reset()
	packer.EncodeStringType(serial)
	key := packer.Bytes()
	if stripped {
		key = key[:len(key)-1]
	}
	return key
}

// clusterTestCompoundAttr is the compound attr name applyClusterRows uses.
const clusterTestCompoundAttr = "__mo_cbkey_012tenant_id012device_id012metric_id"

type clusterRow struct {
	rowID  types.Rowid
	ts     types.TS
	serial []byte // raw serial bytes of the composite cluster key
	pk     int64
}

// clusterVecOwner centralizes ownership of the original engine vectors backing
// a synthetic replay batch. VectorToProtoVector only wraps the Data/Area of
// these vectors and PartitionState retains those aliasing views (cantFree),
// so the bytes can only be returned by freeing the originals, after the
// state's readers have finished. Registering with t.Cleanup covers failure
// paths as well.
type clusterVecOwner struct {
	pool *mpool.MPool
	vecs []*vector.Vector
}

func newClusterVecOwner(t testing.TB, pool *mpool.MPool, vs ...*vector.Vector) *clusterVecOwner {
	o := &clusterVecOwner{pool: pool, vecs: append([]*vector.Vector(nil), vs...)}
	t.Cleanup(o.Free)
	return o
}

func (o *clusterVecOwner) track(vs ...*vector.Vector) {
	o.vecs = append(o.vecs, vs...)
}

func (o *clusterVecOwner) Free() {
	for _, v := range o.vecs {
		v.Free(o.pool)
	}
	o.vecs = nil
}

// drainClusterPool registers a pool's leak proof: registered first, it runs
// last, after every fixture cleanup has freed the original vectors, and
// asserts both bytes and objects of OnHeapOutstanding return to baseline
// before the pool is deleted. Deleting a normal MustNewZero pool does not
// unregister its allocations from the global pointer shards, so a leak here
// would silently outlive the pool without this assertion.
func drainClusterPool(tb testing.TB, pool *mpool.MPool) {
	tb.Cleanup(func() {
		defer mpool.DeleteMPool(pool)
		bytes, objects := pool.OnHeapOutstanding()
		require.Zero(tb, bytes, "replay fixture vectors must return their bytes to the pool")
		require.Zero(tb, objects, "replay fixture vectors must return their objects to the pool")
	})
}

func applyClusterRows(t *testing.T, state *PartitionState, packer *types.Packer, pool *mpool.MPool, rows []clusterRow) {
	applyClusterRowsWithAttrs(t, state, packer, pool, rows, []string{"rowid", "time", "pk", clusterTestCompoundAttr})
}

func applyClusterDeletes(
	t *testing.T,
	state *PartitionState,
	packer *types.Packer,
	pool *mpool.MPool,
	rows []clusterRow,
) {
	t.Helper()
	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())
	pkVec := vector.NewVec(types.T_int64.ToType())
	tbRowIDVec := vector.NewVec(types.T_Rowid.ToType())
	newClusterVecOwner(t, pool, rowIDVec, tsVec, pkVec, tbRowIDVec)
	for _, row := range rows {
		vector.AppendFixed(rowIDVec, row.rowID, false, pool)
		vector.AppendFixed(tsVec, row.ts, false, pool)
		vector.AppendFixed(pkVec, row.pk, false, pool)
		vector.AppendFixed(tbRowIDVec, types.RandomRowid(), false, pool)
	}
	state.HandleRowsDelete(context.Background(), &api.Batch{
		Attrs: []string{"rowid", "time", "pk", "tbrowid"},
		Vecs: []api.Vector{
			mustVectorToProto(rowIDVec),
			mustVectorToProto(tsVec),
			mustVectorToProto(pkVec),
			mustVectorToProto(tbRowIDVec),
		},
	}, packer, pool)
}

// countIntervalCandidates walks the cluster index exactly like a seek-based
// prefix interval predicate does: seek the encoded lower bound, count every
// entry whose key stays within the upper bound under prefix comparison. It
// is the candidate-visit oracle proving the index seeks instead of scanning.
func countIntervalCandidates(
	state *PartitionState,
	ts types.TS,
	lb, ub []byte,
) int {
	iter := state.rowClusterIndex.Iter()
	defer iter.Release()
	cnt := 0
	for ok := iter.Seek(&PrimaryIndexEntry{
		Bytes:      lb,
		Time:       ts,
		RowEntryID: math.MaxInt64,
	}); ok; ok = iter.Next() {
		if types.PrefixCompare(iter.Item().Bytes, ub) > 0 {
			break
		}
		cnt++
	}
	return cnt
}

func clusterIterRowIDs(
	t *testing.T,
	iter RowsIter,
) []types.Rowid {
	t.Helper()
	var rowIDs []types.Rowid
	for iter.Next() {
		rowIDs = append(rowIDs, iter.Entry().RowID)
	}
	require.NoError(t, iter.Close())
	return rowIDs
}

// TestClusterKeyIndexMatchesSelectedRows is the focused regression for the
// reported shape: a selective composite cluster-key predicate over committed
// replay rows must examine only the matching interval, independent of how
// many unrelated rows the replay state retains.
func TestClusterKeyIndexMatchesSelectedRows(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	for _, scale := range []int{32} {
		state := NewPartitionState("", false, 42, false)
		sid := objectio.NewSegmentid()

		// Deterministic issue dataset: devices 0..999, metrics 0..4, 20 ts
		// ordinals; value is derived from the ts ordinal. Insertion order is
		// shuffled deterministically.
		idx := make([]int, scale)
		for i := range idx {
			idx[i] = i
		}
		rnd := rand.New(rand.NewSource(int64(scale)))
		rnd.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })

		// At the reported scale the dataset is the exact issue shape: 1,000
		// devices x 5 metrics x 20 ts ordinals with tenant 0. Smaller scales
		// keep the two matching rows fixed and pad with unrelated devices so
		// the candidate-visit count stays scale-independent.
		keyOf := func(i int) (device, metric int32, tsOrdinal int64) {
			switch i {
			case 0:
				return 0, 0, 18
			case 1:
				return 0, 0, 19
			default:
				return 1 + int32(i%999), int32(i % 5), int64(i % 18)
			}
		}

		rows := make([]clusterRow, 0, scale)
		matched := make(map[types.Rowid]int, 2)
		for n, i := range idx {
			device, metric, tsOrdinal := keyOf(i)
			rowID := objectio.NewRowid(objectio.NewBlockid(sid, uint16(i%128), 0), uint32(i))
			if device == 0 && metric == 0 && tsOrdinal >= 18 {
				matched[rowID] = i
			}
			rows = append(rows, clusterRow{
				rowID: rowID,
				ts:    types.BuildTS(int64(1000+n), 0),
				serial: encodeIssueClusterKey(
					packer, 0 /*tenant*/, device, metric, tsOrdinal),
				pk: int64(i),
			})
		}
		applyClusterRows(t, state, packer, pool, rows)

		require.Equal(t, scale, state.rows.Len())
		require.Equal(t, scale, clusterIndexLen(state))
		require.Len(t, matched, 2)

		// Predicate: tenant=0 AND device=0 AND metric=0 AND ts >= 18, encoded
		// exactly like the planner's prefix_in_range: lb carries the bound on
		// the last part, ub is the three-part prefix.
		lb := encodeClusterKeyForFilter(
			packer, encodeIssueClusterKey(packer, 0, 0, 0, 18), true)
		ub := encodeClusterKeyForFilter(
			packer, encodeIssueClusterPrefix(packer, 0, 0, 0), true)

		maxTS := types.BuildTS(int64(1000+scale), 0)
		require.Equal(t, 2, countIntervalCandidates(state, maxTS, lb, ub))

		// The cluster iterator returns exactly the two matching rows.
		iter := state.NewClusterKeyIter(maxTS, function.PREFIX_BETWEEN, [][]byte{lb, ub})
		rowIDs := clusterIterRowIDs(t, iter)
		require.Len(t, rowIDs, 2)
		for _, rowID := range rowIDs {
			_, ok := matched[rowID]
			require.True(t, ok, "returned row %s must be a predicate match", rowID)
		}

		// The exhaustive iterator still visits every row: the pruned path and
		// the general path agree on the full dataset.
		rowsIter := state.NewRowsIter(maxTS, nil, false)
		visited := 0
		for rowsIter.Next() {
			visited++
		}
		require.NoError(t, rowsIter.Close())
		require.Equal(t, scale, visited)
	}
}

// clusterPredicate is one differential case: the natural semantics over raw
// serial bytes (the oracle) plus the encoded filter keys and op handed to the
// cluster iterator.
type clusterPredicate struct {
	name  string
	op    int
	keys  [][]byte
	match func(serial []byte) bool
	specs []readutil.MemPKFilterSpec // optional multi-spec form
}

// TestClusterKeyIterMatchesRowsIter differentially compares every supported
// cluster predicate against the authoritative NewRowsIter plus natural
// predicate evaluation, across snapshots that include deletes and future
// commits. The row-identity validation must make indexed iteration agree
// with the exhaustive path in every combination.
func TestClusterKeyIterMatchesRowsIter(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	state := NewPartitionState("", false, 42, false)
	sid := objectio.NewSegmentid()

	// A small key domain with guaranteed duplicate cluster values, including
	// a 0x00 byte inside the varchar part to exercise escape encoding.
	strs := []string{"", "a", "b", "ab", "a\x00b", "bb"}
	type key struct {
		a        int32
		s        string
		nullPart bool
		ord      int
	}
	var keys []key
	for a := int32(0); a < 3; a++ {
		for _, s := range strs {
			for rep := 0; rep < 3; rep++ {
				keys = append(keys, key{a, s, false, len(keys)})
			}
		}
	}
	// Rows whose second cluster part is NULL: the serial carries the null
	// marker, which never byte-equals a typed part encoding. The oracle
	// below is the pruning predicate's byte domain (a candidate superset is
	// legal; the residual SQL filter narrows it upstream), and these rows
	// pin that NULL components neither vanish from nor corrupt that domain.
	for a := int32(0); a < 2; a++ {
		for rep := 0; rep < 3; rep++ {
			keys = append(keys, key{a, "", true, len(keys)})
		}
	}

	serialOf := func(k key) []byte {
		if k.nullPart {
			packer.Reset()
			packer.EncodeInt32(k.a)
			packer.EncodeNull()
			return packer.Bytes()
		}
		return encodeVarlenClusterKey(packer, k.a, k.s)
	}

	// Insert everything at increasing timestamps; delete every third key
	// afterwards; re-insert some deleted values under fresh row ids (the
	// update shape).
	rowSerial := make(map[types.Rowid][]byte)
	insertRows := make([]clusterRow, 0, len(keys))
	nextPK := int64(0)
	for i, k := range keys {
		rowID := objectio.NewRowid(objectio.NewBlockid(sid, uint16(i%8), 0), uint32(i))
		serial := serialOf(k)
		rowSerial[rowID] = serial
		insertRows = append(insertRows, clusterRow{
			rowID:  rowID,
			ts:     types.BuildTS(int64(10+i), 0),
			serial: serial,
			pk:     nextPK,
		})
		nextPK++
	}
	applyClusterRows(t, state, packer, pool, insertRows)

	var deleteRows []clusterRow
	var updateRows []clusterRow
	for i, k := range keys {
		if i%3 != 0 {
			continue
		}
		rowID := objectio.NewRowid(objectio.NewBlockid(sid, uint16(i%8), 0), uint32(i))
		deleteRows = append(deleteRows, clusterRow{
			rowID: rowID,
			ts:    types.BuildTS(int64(100+i), 0),
			pk:    int64(i),
		})

		// re-insert the same cluster value under a fresh row id later
		newRowID := objectio.NewRowid(objectio.NewBlockid(sid, uint16(i%8+8), 0), uint32(i))
		rowSerial[newRowID] = serialOf(k)
		updateRows = append(updateRows, clusterRow{
			rowID:  newRowID,
			ts:     types.BuildTS(int64(200+i), 0),
			serial: serialOf(k),
			pk:     nextPK,
		})
		nextPK++
	}
	applyClusterDeletes(t, state, packer, pool, deleteRows)
	applyClusterRows(t, state, packer, pool, updateRows)

	// Predicates. Bounds are raw serial values from the domain plus one
	// outside value.
	outside := encodeVarlenClusterKey(packer, 9, "zz")
	kA0 := encodeVarlenClusterKey(packer, 0, "")
	kB := encodeVarlenClusterKey(packer, 1, "ab")
	prefix0 := encodeVarlenClusterKey(packer, 0, "")
	prefix0Full := encodeVarlenClusterKey(packer, 0, "ab")
	prefix1 := encodeVarlenClusterKey(packer, 1, "")
	// part-boundary prefix: the serial of the int part alone, the shape the
	// planner derives for `a = 0`
	prefixPart0 := func() []byte {
		packer.Reset()
		packer.EncodeInt32(0)
		return packer.Bytes()
	}()

	enc := func(serial []byte, stripped bool) []byte {
		return encodeClusterKeyForFilter(packer, serial, stripped)
	}
	encPrefix := func(serial []byte) []byte {
		// A prefix bound keeps the leading parts only: strip the last part's
		// encoding by re-encoding the partial tuple.
		return encodeClusterKeyForFilter(packer, serial, true)
	}

	predicates := []clusterPredicate{
		{
			name: "equal",
			op:   function.EQUAL,
			keys: [][]byte{enc(kB, false)},
			match: func(serial []byte) bool {
				return bytes.Equal(serial, kB)
			},
		},
		{
			name: "prefix-eq",
			op:   function.PREFIX_EQ,
			keys: [][]byte{encPrefix(prefix0)},
			match: func(serial []byte) bool {
				return bytes.HasPrefix(serial, prefix0)
			},
		},
		{
			name: "prefix-eq-part-boundary",
			op:   function.PREFIX_EQ,
			keys: [][]byte{encPrefix(prefixPart0)},
			match: func(serial []byte) bool {
				return bytes.HasPrefix(serial, prefixPart0)
			},
		},
		{
			name: "between-closed",
			op:   function.BETWEEN,
			keys: [][]byte{enc(kA0, false), enc(kB, false)},
			match: func(serial []byte) bool {
				return bytes.Compare(serial, kA0) >= 0 && bytes.Compare(serial, kB) <= 0
			},
		},
		{
			name: "between-left-open",
			op:   readutil.RangeLeftOpen,
			keys: [][]byte{enc(kA0, false), enc(kB, false)},
			match: func(serial []byte) bool {
				return bytes.Compare(serial, kA0) > 0 && bytes.Compare(serial, kB) <= 0
			},
		},
		{
			name: "between-right-open",
			op:   readutil.RangeRightOpen,
			keys: [][]byte{enc(kA0, false), enc(kB, false)},
			match: func(serial []byte) bool {
				return bytes.Compare(serial, kA0) >= 0 && bytes.Compare(serial, kB) < 0
			},
		},
		{
			name: "between-both-open",
			op:   readutil.RangeBothOpen,
			keys: [][]byte{enc(kA0, false), enc(kB, false)},
			match: func(serial []byte) bool {
				return bytes.Compare(serial, kA0) > 0 && bytes.Compare(serial, kB) < 0
			},
		},
		{
			name: "prefix-between-closed",
			op:   function.PREFIX_BETWEEN,
			keys: [][]byte{encPrefix(prefix0), encPrefix(prefix1)},
			match: func(serial []byte) bool {
				return types.PrefixCompare(serial, prefix0) >= 0 &&
					types.PrefixCompare(serial, prefix1) <= 0
			},
		},
		{
			name: "prefix-range-left-open",
			op:   readutil.PrefixRangeLeftOpen,
			keys: [][]byte{encPrefix(prefix0), encPrefix(prefix1)},
			match: func(serial []byte) bool {
				return types.PrefixCompare(serial, prefix0) > 0 &&
					types.PrefixCompare(serial, prefix1) <= 0
			},
		},
		{
			name: "prefix-range-right-open",
			op:   readutil.PrefixRangeRightOpen,
			keys: [][]byte{encPrefix(prefix0), encPrefix(prefix1)},
			match: func(serial []byte) bool {
				return types.PrefixCompare(serial, prefix0) >= 0 &&
					types.PrefixCompare(serial, prefix1) < 0
			},
		},
		{
			name: "prefix-range-both-open",
			op:   readutil.PrefixRangeBothOpen,
			keys: [][]byte{encPrefix(prefix0), encPrefix(prefix1)},
			match: func(serial []byte) bool {
				return types.PrefixCompare(serial, prefix0) > 0 &&
					types.PrefixCompare(serial, prefix1) < 0
			},
		},
		{
			name: "great-equal",
			op:   function.GREAT_EQUAL,
			keys: [][]byte{enc(kB, false)},
			match: func(serial []byte) bool {
				return bytes.Compare(serial, kB) >= 0
			},
		},
		{
			name: "less-than",
			op:   function.LESS_THAN,
			keys: [][]byte{enc(kB, false)},
			match: func(serial []byte) bool {
				return bytes.Compare(serial, kB) < 0
			},
		},
		{
			name: "in",
			op:   function.IN,
			keys: func() [][]byte {
				ks := [][]byte{enc(kA0, false), enc(kB, false), enc(outside, false)}
				sort.Slice(ks, func(i, j int) bool {
					return bytes.Compare(ks[i], ks[j]) < 0
				})
				return ks
			}(),
			match: func(serial []byte) bool {
				return bytes.Equal(serial, kA0) || bytes.Equal(serial, kB) ||
					bytes.Equal(serial, outside)
			},
		},
		{
			name: "prefix-in",
			op:   function.PREFIX_IN,
			keys: func() [][]byte {
				ks := [][]byte{encPrefix(prefix0), encPrefix(prefix1)}
				sort.Slice(ks, func(i, j int) bool {
					return bytes.Compare(ks[i], ks[j]) < 0
				})
				return ks
			}(),
			match: func(serial []byte) bool {
				return bytes.HasPrefix(serial, prefix0) || bytes.HasPrefix(serial, prefix1)
			},
		},
		{
			name: "union-of-two",
			specs: []readutil.MemPKFilterSpec{
				{Op: function.EQUAL, Keys: [][]byte{enc(kB, false)}},
				{Op: function.PREFIX_BETWEEN, Keys: [][]byte{encPrefix(prefix0), encPrefix(prefix1)}},
			},
			match: func(serial []byte) bool {
				return bytes.Equal(serial, kB) ||
					(types.PrefixCompare(serial, prefix0) >= 0 &&
						types.PrefixCompare(serial, prefix1) <= 0)
			},
		},
		{
			name: "no-match",
			op:   function.EQUAL,
			keys: [][]byte{enc(outside, false)},
			match: func(serial []byte) bool {
				return bytes.Equal(serial, outside)
			},
		},
		{
			name: "prefix-beyond-part0",
			op:   function.PREFIX_EQ,
			keys: [][]byte{encPrefix(prefix0Full)},
			match: func(serial []byte) bool {
				return bytes.HasPrefix(serial, prefix0Full)
			},
		},
	}

	snapshots := []types.TS{
		types.BuildTS(5, 0),    // before all inserts: empty state
		types.BuildTS(15, 0),   // during inserts
		types.BuildTS(5000, 0), // after all inserts, before deletes
		types.BuildTS(150, 0),  // during deletes
		types.BuildTS(180, 0),  // after deletes, before re-inserts
		types.BuildTS(5000, 0), // after re-inserts (superset snapshot)
		types.BuildTS(300, 0),  // around re-inserts
	}

	for _, snap := range snapshots {
		for _, pred := range predicates {
			name := fmt.Sprintf("snap=%s/pred=%s", snap.ToString(), pred.name)
			t.Run(name, func(t *testing.T) {
				// Oracle: the exhaustive rows iterator plus natural predicate
				// evaluation on the raw serial bytes.
				expected := make(map[types.Rowid]struct{})
				rowsIter := state.NewRowsIter(snap, nil, false)
				for rowsIter.Next() {
					entry := rowsIter.Entry()
					serial, ok := rowSerial[entry.RowID]
					require.True(t, ok, "row %s must have a cluster key", entry.RowID)
					if pred.match(serial) {
						expected[entry.RowID] = struct{}{}
					}
				}
				require.NoError(t, rowsIter.Close())

				var got []types.Rowid
				if pred.specs != nil {
					got = clusterIterRowIDs(
						t, state.newClusterKeyIterWithFilters(snap, pred.specs))
				} else {
					got = clusterIterRowIDs(
						t, state.NewClusterKeyIter(snap, pred.op, pred.keys))
				}

				gotSet := make(map[types.Rowid]struct{}, len(got))
				for _, rowID := range got {
					require.NotContains(t, gotSet, rowID, "duplicate row returned")
					gotSet[rowID] = struct{}{}
				}
				require.Equal(t, expected, gotSet)
			})
		}
	}
}

// TestClusterKeyIndexGCTrackedWithRows proves the cluster index is cleaned
// when an appendable object is dropped and its replay rows are garbage
// collected; a stale entry would resurrect pruned rows.
func TestClusterKeyIndexGCTrackedWithRows(t *testing.T) {
	ctx := context.Background()
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	state := NewPartitionState("", false, 42, false)
	sid := objectio.NewSegmentid()

	var rows []clusterRow
	for i := 0; i < 10; i++ {
		rowID := objectio.NewRowid(objectio.NewBlockid(sid, 0, 0), uint32(i))
		rows = append(rows, clusterRow{
			rowID:  rowID,
			ts:     types.BuildTS(int64(10+i), 0),
			serial: encodeVarlenClusterKey(packer, int32(i%3), fmt.Sprintf("v%d", i%4)),
			pk:     int64(i),
		})
	}
	applyClusterRows(t, state, packer, pool, rows)
	require.Equal(t, 10, state.rows.Len())
	require.Equal(t, 10, clusterIndexLen(state))
	require.Equal(t, 10, state.rowPrimaryKeyIndex.Len())

	// Drop the appendable object holding the block; every row committed
	// before the drop time is GCed together with its index entries.
	objID := objectio.NewObjectidWithSegmentIDAndNum(sid, 0)
	stats := objectio.NewObjectStatsWithObjectID(objID, true, false, false)
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 10))
	require.NoError(t, objectio.SetObjectStatsSize(stats, 4096))
	err := state.HandleObjectEntry(ctx, nil, objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(9, 0),
		DeleteTime:  types.BuildTS(100, 0),
	}, false)
	require.NoError(t, err)

	require.Equal(t, 0, state.rows.Len())
	require.Equal(t, 0, clusterIndexLen(state))
	require.Equal(t, 0, state.rowPrimaryKeyIndex.Len())

	// A cluster iteration over the emptied state yields nothing even though
	// the predicate matches every previously indexed key.
	lb := encodeClusterKeyForFilter(packer, encodeVarlenClusterKey(packer, 0, ""), true)
	ub := encodeClusterKeyForFilter(packer, encodeVarlenClusterKey(packer, 9, ""), true)
	iter := state.NewClusterKeyIter(
		types.BuildTS(1000, 0), function.PREFIX_BETWEEN, [][]byte{lb, ub})
	require.False(t, iter.Next())
	require.NoError(t, iter.Close())
}

// TestClusterKeyIndexSnapshotCopyIsolation proves that publishing new replay
// state (Copy) never mutates the cluster index of the older snapshot, and
// that both snapshots iterate their own rows.
func TestClusterKeyIndexSnapshotCopyIsolation(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	state := NewPartitionState("", false, 42, false)
	sid := objectio.NewSegmentid()

	var rows []clusterRow
	for i := 0; i < 6; i++ {
		rowID := objectio.NewRowid(objectio.NewBlockid(sid, 0, 0), uint32(i))
		rows = append(rows, clusterRow{
			rowID:  rowID,
			ts:     types.BuildTS(int64(10+i), 0),
			serial: encodeVarlenClusterKey(packer, 0, fmt.Sprintf("k%d", i)),
			pk:     int64(i),
		})
	}
	applyClusterRows(t, state, packer, pool, rows)

	snapshot := state.Copy()
	require.Equal(t, clusterIndexLen(state), clusterIndexLen(snapshot))

	// Mutate the live state: more rows, plus a delete of an old row.
	var extra []clusterRow
	for i := 6; i < 10; i++ {
		rowID := objectio.NewRowid(objectio.NewBlockid(sid, 1, 0), uint32(i))
		extra = append(extra, clusterRow{
			rowID:  rowID,
			ts:     types.BuildTS(int64(50+i), 0),
			serial: encodeVarlenClusterKey(packer, 0, fmt.Sprintf("k%d", i)),
			pk:     int64(i),
		})
	}
	applyClusterRows(t, state, packer, pool, extra)

	deletedRowID := objectio.NewRowid(objectio.NewBlockid(sid, 0, 0), uint32(0))
	applyClusterDeletes(t, state, packer, pool, []clusterRow{{
		rowID: deletedRowID,
		ts:    types.BuildTS(90, 0),
		pk:    0,
	}})

	lb := encodeClusterKeyForFilter(packer, encodeVarlenClusterKey(packer, 0, ""), true)
	ub := encodeClusterKeyForFilter(packer, encodeVarlenClusterKey(packer, 9, ""), true)

	// The published snapshot still sees all six original rows.
	snapIter := snapshot.NewClusterKeyIter(
		types.BuildTS(1000, 0), function.PREFIX_BETWEEN, [][]byte{lb, ub})
	snapRows := clusterIterRowIDs(t, snapIter)
	require.Len(t, snapRows, 6)
	require.Equal(t, 6, clusterIndexLen(snapshot))

	// The live state sees the survivors plus the new rows: 5 + 4.
	liveIter := state.NewClusterKeyIter(
		types.BuildTS(1000, 0), function.PREFIX_BETWEEN, [][]byte{lb, ub})
	liveRows := clusterIterRowIDs(t, liveIter)
	require.Len(t, liveRows, 9)
	require.NotContains(t, liveRows, deletedRowID)
	require.Equal(t, 10, clusterIndexLen(state))
}

// TestClusterKeyIndexRepeatedReplayIdempotent proves that replaying the
// same insert batch twice leaves exactly one cluster entry per retained
// row version. noData states retain fake-PK behavior without a cluster index.
func TestClusterKeyIndexRepeatedReplayIdempotent(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	for _, noData := range []bool{false, true} {
		state := NewPartitionState("", noData, 42, false)
		sid := objectio.NewSegmentid()

		var rows []clusterRow
		for i := 0; i < 5; i++ {
			rowID := objectio.NewRowid(objectio.NewBlockid(sid, 0, 0), uint32(i))
			rows = append(rows, clusterRow{
				rowID:  rowID,
				ts:     types.BuildTS(int64(10+i), 0),
				serial: encodeVarlenClusterKey(packer, int32(i%2), "dup"),
				pk:     int64(i),
			})
		}
		applyClusterRows(t, state, packer, pool, rows)
		snapshot := state.Copy()
		pivot := &RowEntry{BlockID: rows[0].rowID.CloneBlockID(), RowID: rows[0].rowID, Time: rows[0].ts}
		before, ok := snapshot.rows.Get(pivot)
		require.True(t, ok)
		retainedBatch := before.Batch
		applyClusterRows(t, state, packer, pool, rows)
		current, ok := state.rows.Get(pivot)
		require.True(t, ok)
		require.NotSame(t, before, current, "replay may not mutate a published entry")
		require.Same(t, retainedBatch, before.Batch)
		require.Equal(t, 5, state.rows.Len())
		wantIndex := 5
		if noData {
			wantIndex = 0
		}
		require.Equal(t, wantIndex, clusterIndexLen(state))
		require.Equal(t, 5, state.rowPrimaryKeyIndex.Len())

		// Duplicate cluster values must all survive: five rows share two
		// distinct keys.
		lb := encodeClusterKeyForFilter(packer, encodeVarlenClusterKey(packer, 0, "dup"), false)
		ub := encodeClusterKeyForFilter(packer, encodeVarlenClusterKey(packer, 1, "dup"), false)
		iter := state.NewClusterKeyIterWithFilters(types.BuildTS(100, 0), *clusterTestKey(clusterTestCompoundAttr, 1), []readutil.MemPKFilterSpec{{Op: function.BETWEEN, Keys: [][]byte{lb, ub}}})
		got := clusterIterRowIDs(t, iter)
		require.Len(t, got, 5)
	}
}

// TestClusterKeyIndexIgnoresAmbiguousCompoundAttrs proves the fail-open
// rule: when a batch carries more than one __mo_cbkey_-prefixed attribute,
// no cluster index is built and reads stay on the exhaustive path.
func TestClusterKeyIndexIgnoresAmbiguousCompoundAttrs(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	state := NewPartitionState("", false, 42, false)

	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())
	pkVec := vector.NewVec(types.T_int64.ToType())
	clusterVec := vector.NewVec(types.T_varchar.ToType())
	otherVec := vector.NewVec(types.T_varchar.ToType())
	newClusterVecOwner(t, pool, rowIDVec, tsVec, pkVec, clusterVec, otherVec)
	rowID := types.RandomRowid()
	vector.AppendFixed(rowIDVec, rowID, false, pool)
	vector.AppendFixed(tsVec, types.BuildTS(1, 0), false, pool)
	vector.AppendFixed(pkVec, int64(1), false, pool)
	vector.AppendBytes(clusterVec, []byte("v"), false, pool)
	vector.AppendBytes(otherVec, []byte("w"), false, pool)

	state.HandleRowsInsert(context.Background(), &api.Batch{
		Attrs: []string{
			"rowid",
			"time",
			"pk",
			"__mo_cbkey_012tenant_id012device_id",
			"__mo_cbkey_usercopy",
		},
		Vecs: []api.Vector{
			mustVectorToProto(rowIDVec),
			mustVectorToProto(tsVec),
			mustVectorToProto(pkVec),
			mustVectorToProto(clusterVec),
			mustVectorToProto(otherVec),
		},
	}, 0, packer, pool)

	require.Equal(t, 1, state.rows.Len())
	require.Equal(t, 0, clusterIndexLen(state))
	// The row entry carries no cluster bytes, so GC paths skip the cluster
	// index removal without touching it.
	entry, ok := state.rows.Get(&RowEntry{
		BlockID: rowID.CloneBlockID(),
		RowID:   rowID,
		Time:    types.BuildTS(1, 0),
	})
	require.True(t, ok)
	require.NotNil(t, entry.Batch)

	// The P1 regression: the ambiguous batch makes the index incomplete, and
	// the guarded read entry must fail open to the general row iteration so
	// the committed row is still returned instead of being missed.
	require.False(t, state.ClusterIndexReadyFor(*clusterTestKey("__mo_cbkey_012tenant_id012device_id", 1)))
	require.False(t, state.ClusterIndexReadyFor(*clusterTestKey("__mo_cbkey_usercopy", 1)))
	iter := state.NewClusterKeyIterWithFilters(
		types.BuildTS(100, 0), *clusterTestKey("__mo_cbkey_012tenant_id012device_id", 1),
		[]readutil.MemPKFilterSpec{
			{Op: function.PREFIX_BETWEEN, Keys: [][]byte{
				encodeClusterKeyForFilter(packer, []byte("v"), true),
				encodeClusterKeyForFilter(packer, []byte("w"), true),
			}},
		})
	got := clusterIterRowIDs(t, iter)
	require.Equal(t, []types.Rowid{rowID}, got,
		"the fallback must return the committed row the incomplete index would miss")
}

// applyClusterRowsWithAttrs replays rows under a caller-chosen attr layout,
// for batches that shift the compound column position, omit it, or add
// same-prefix or filler user columns. Non-cluster user attrs get an int32
// filler vector.
func makeClusterReplayBatch(t testing.TB, pool *mpool.MPool, rows []clusterRow, attrs []string) *api.Batch {
	t.Helper()
	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())
	pkVec := vector.NewVec(types.T_int64.ToType())
	clusterVec := vector.NewVec(types.T_varchar.ToType())
	fixtures := newClusterVecOwner(t, pool, rowIDVec, tsVec, pkVec, clusterVec)
	for _, row := range rows {
		vector.AppendFixed(rowIDVec, row.rowID, false, pool)
		vector.AppendFixed(tsVec, row.ts, false, pool)
		vector.AppendFixed(pkVec, row.pk, false, pool)
		vector.AppendBytes(clusterVec, row.serial, false, pool)
	}
	vecs := []api.Vector{
		mustVectorToProto(rowIDVec),
		mustVectorToProto(tsVec),
		mustVectorToProto(pkVec),
	}
	for _, attr := range attrs[3:] {
		if strings.HasPrefix(attr, catalog.PrefixCBColName) {
			vecs = append(vecs, mustVectorToProto(clusterVec))
		} else {
			filler := vector.NewVec(types.T_int32.ToType())
			for range rows {
				vector.AppendFixed(filler, int32(1), false, pool)
			}
			fixtures.track(filler)
			vecs = append(vecs, mustVectorToProto(filler))
		}
	}
	require.Equal(t, len(attrs), len(vecs))
	return &api.Batch{
		Attrs: attrs,
		Vecs:  vecs,
	}
}

func applyClusterRowsWithAttrs(t *testing.T, state *PartitionState, packer *types.Packer, pool *mpool.MPool, rows []clusterRow, attrs []string) {
	// This fixture declares its chosen final column; production declaration
	// admission is independently tested against the catalog and dispatcher.
	var key *readutil.ClusterKeyDescriptor
	if len(attrs) > 3 {
		key = clusterTestKey(attrs[len(attrs)-1], uint32(len(attrs)-3))
	}
	state.HandleRowsInsert(context.Background(), makeClusterReplayBatch(t, pool, rows, attrs), 0, packer, pool, key)
}

// TestClusterKeyIterFallbackWhenIndexUnusable drives every schema-era shape
// that can leave retained versions unindexed and proves the guarded read
// entry returns the complete result set through the general iteration.
func TestClusterKeyIterFallbackWhenIndexUnusable(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	plainAttrs := []string{"rowid", "time", "pk"}
	clusterAttrsA := []string{"rowid", "time", "pk", "__mo_cbkey_006tenant006device"}
	clusterAttrsB := []string{"rowid", "time", "pk", "__mo_cbkey_003key_a"}

	mkRows := func(base int, count int, a int32, s string) []clusterRow {
		sid := objectio.NewSegmentid()
		rows := make([]clusterRow, 0, count)
		for i := 0; i < count; i++ {
			rows = append(rows, clusterRow{
				rowID:  objectio.NewRowid(objectio.NewBlockid(sid, uint16(i%4), 0), uint32(base+i)),
				ts:     types.BuildTS(int64(base+i), 0),
				serial: encodeVarlenClusterKey(packer, a, s),
				pk:     int64(base + i),
			})
		}
		return rows
	}

	// A predicate on the leading int part matching every replayed row: any
	// fallback must return all of them, proving no committed version is missed.
	prefixSpec := func(a int32) []readutil.MemPKFilterSpec {
		packer.Reset()
		packer.EncodeInt32(a)
		return []readutil.MemPKFilterSpec{
			{Op: function.PREFIX_EQ, Keys: [][]byte{
				encodeClusterKeyForFilter(packer, packer.Bytes(), true),
			}},
		}
	}

	for _, tc := range []struct {
		name     string
		setup    func(state *PartitionState) int // returns expected total rows
		queryCol string
	}{
		{
			name: "user column shadows the compound prefix",
			setup: func(state *PartitionState) int {
				rows := mkRows(0, 3, 0, "k")
				rowIDVec := vector.NewVec(types.T_Rowid.ToType())
				tsVec := vector.NewVec(types.T_TS.ToType())
				pkVec := vector.NewVec(types.T_int64.ToType())
				clusterVec := vector.NewVec(types.T_varchar.ToType())
				otherVec := vector.NewVec(types.T_int32.ToType())
				newClusterVecOwner(t, pool, rowIDVec, tsVec, pkVec, clusterVec, otherVec)
				for _, row := range rows {
					vector.AppendFixed(rowIDVec, row.rowID, false, pool)
					vector.AppendFixed(tsVec, row.ts, false, pool)
					vector.AppendFixed(pkVec, row.pk, false, pool)
					vector.AppendBytes(clusterVec, row.serial, false, pool)
					vector.AppendFixed(otherVec, int32(1), false, pool)
				}
				state.HandleRowsInsert(context.Background(), &api.Batch{
					Attrs: []string{"rowid", "time", "pk",
						"__mo_cbkey_006tenant006device", "__mo_cbkey_usercopy"},
					Vecs: []api.Vector{
						mustVectorToProto(rowIDVec), mustVectorToProto(tsVec),
						mustVectorToProto(pkVec), mustVectorToProto(clusterVec),
						mustVectorToProto(otherVec),
					},
				}, 0, packer, pool)
				return len(rows)
			},
			queryCol: "__mo_cbkey_006tenant006device",
		},
		{
			name: "compound column added after plain batches",
			setup: func(state *PartitionState) int {
				plain := mkRows(0, 3, 0, "k")
				clustered := mkRows(10, 3, 0, "k")
				applyClusterRowsWithAttrs(t, state, packer, pool, plain, plainAttrs)
				applyClusterRowsWithAttrs(t, state, packer, pool, clustered, clusterAttrsA)
				return len(plain) + len(clustered)
			},
			queryCol: "__mo_cbkey_006tenant006device",
		},
		{
			name: "compound column renamed between eras, queried with old name",
			setup: func(state *PartitionState) int {
				oldEra := mkRows(0, 3, 0, "k")
				newEra := mkRows(10, 3, 0, "k")
				applyClusterRowsWithAttrs(t, state, packer, pool, oldEra, clusterAttrsA)
				applyClusterRowsWithAttrs(t, state, packer, pool, newEra, clusterAttrsB)
				return len(oldEra) + len(newEra)
			},
			queryCol: "__mo_cbkey_006tenant006device",
		},
		{
			name: "compound column renamed between eras, queried with new name",
			setup: func(state *PartitionState) int {
				oldEra := mkRows(0, 3, 0, "k")
				newEra := mkRows(10, 3, 0, "k")
				applyClusterRowsWithAttrs(t, state, packer, pool, oldEra, clusterAttrsA)
				applyClusterRowsWithAttrs(t, state, packer, pool, newEra, clusterAttrsB)
				return len(oldEra) + len(newEra)
			},
			queryCol: "__mo_cbkey_003key_a",
		},
		{
			name: "compound column dropped after clustered batches",
			setup: func(state *PartitionState) int {
				clustered := mkRows(0, 3, 0, "k")
				plain := mkRows(10, 3, 0, "k")
				applyClusterRowsWithAttrs(t, state, packer, pool, clustered, clusterAttrsA)
				applyClusterRowsWithAttrs(t, state, packer, pool, plain, plainAttrs)
				return len(clustered) + len(plain)
			},
			queryCol: "__mo_cbkey_006tenant006device",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := NewPartitionState("", false, 42, false)
			want := tc.setup(state)
			require.False(t, state.ClusterIndexReadyFor(*clusterTestKey(tc.queryCol, 1)),
				"the era tracking must disable pruning for an unusable index")

			iter := state.NewClusterKeyIterWithFilters(
				types.BuildTS(1000, 0), *clusterTestKey(tc.queryCol, 1), prefixSpec(0))
			got := clusterIterRowIDs(t, iter)
			require.Len(t, got, want,
				"the fallback must return every committed row of both eras")
			require.Equal(t, want, state.rows.Len())
		})
	}
}

// TestClusterKeyIndexResolvesCompoundAttrIndependentlyOfPosition proves the
// compound column is located by attr name, so a sequence-number gap or a
// reordered schema that moves the hidden column still gets indexed.
func TestClusterKeyIndexResolvesCompoundAttrIndependentlyOfPosition(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	state := NewPartitionState("", false, 42, false)
	sid := objectio.NewSegmentid()
	var rows []clusterRow
	for i := 0; i < 4; i++ {
		rows = append(rows, clusterRow{
			rowID:  objectio.NewRowid(objectio.NewBlockid(sid, 0, 0), uint32(i)),
			ts:     types.BuildTS(int64(10+i), 0),
			serial: encodeVarlenClusterKey(packer, 0, "k"),
			pk:     int64(i),
		})
	}
	// The compound column sits after an extra user column, not directly
	// after the fixed prefix.
	applyClusterRowsWithAttrs(t, state, packer, pool, rows,
		[]string{"rowid", "time", "pk", "extra", "__mo_cbkey_001a"})

	require.Equal(t, 4, clusterIndexLen(state))
	require.True(t, state.ClusterIndexReadyFor(*clusterTestKey("__mo_cbkey_001a", 2)))

	lb := encodeClusterKeyForFilter(packer, encodeVarlenClusterKey(packer, 0, "k"), false)
	iter := state.NewClusterKeyIterWithFilters(
		types.BuildTS(100, 0), *clusterTestKey("__mo_cbkey_001a", 2),
		[]readutil.MemPKFilterSpec{
			{Op: function.EQUAL, Keys: [][]byte{lb}},
		})
	require.Len(t, clusterIterRowIDs(t, iter), 4)
}

// TestClusterKeyIterCloseIsIdempotent guards the iterator resource
// lifecycle: closing twice and iterating after close must be safe, matching
// the primary-key iterator contract.
func TestClusterKeyIterCloseIsIdempotent(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	state := NewPartitionState("", false, 42, false)
	var rows []clusterRow
	for i := 0; i < 4; i++ {
		rowID := objectio.NewRowid(objectio.NewBlockid(objectio.NewSegmentid(), 0, 0), uint32(i))
		rows = append(rows, clusterRow{
			rowID:  rowID,
			ts:     types.BuildTS(int64(10+i), 0),
			serial: encodeVarlenClusterKey(packer, 0, "k"),
			pk:     int64(i),
		})
	}
	applyClusterRows(t, state, packer, pool, rows)

	lb := encodeClusterKeyForFilter(packer, encodeVarlenClusterKey(packer, 0, "k"), false)
	iter := state.NewClusterKeyIter(
		types.BuildTS(100, 0), function.EQUAL, [][]byte{lb})
	require.True(t, iter.Next())
	require.NoError(t, iter.Close())
	require.NoError(t, iter.Close())
	require.False(t, iter.Next())
}

// TestClusterKeyIterSeeksPastOpenLowerBoundDuplicates targets the open
// lower bound: when N rows share the excluded key (or prefix) and only two
// rows satisfy the predicate, the iterator must skip the whole excluded run
// with a bounded seek instead of walking it, so the measured advances stay
// constant while the run grows. The planner reaches these kinds through
// strict bounds (event_ts > x binds a left-open prefix_in_range).
func TestClusterKeyIterSeeksPastOpenLowerBoundDuplicates(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	for _, scale := range []int{32} {
		for _, tc := range []struct {
			name string
			op   int
		}{
			{"full-key-greater-than", function.GREAT_THAN},
			{"full-key-left-open", readutil.RangeLeftOpen},
			{"full-key-both-open", readutil.RangeBothOpen},
			{"prefix-left-open", readutil.PrefixRangeLeftOpen},
			{"prefix-both-open", readutil.PrefixRangeBothOpen},
		} {
			t.Run(fmt.Sprintf("%s/%d", tc.name, scale), func(t *testing.T) {
				state := NewPartitionState("", false, 42, false)
				sid := objectio.NewSegmentid()

				var rows []clusterRow
				next := 0
				mkRow := func(a int32, str string) clusterRow {
					row := clusterRow{
						rowID:  objectio.NewRowid(objectio.NewBlockid(sid, uint16(next%8), 0), uint32(next)),
						ts:     types.BuildTS(int64(10+next), 0),
						serial: encodeVarlenClusterKey(packer, a, str),
						pk:     int64(next),
					}
					next++
					return row
				}
				fullKey := tc.op == function.GREAT_THAN || tc.op == readutil.RangeLeftOpen || tc.op == readutil.RangeBothOpen
				if fullKey {
					// scale rows share the excluded full key (0,"dup").
					for i := 0; i < scale; i++ {
						rows = append(rows, mkRow(0, "dup"))
					}
				} else {
					// scale rows carry the excluded int-part prefix with
					// varying string suffixes: a real prefix run.
					for i := 0; i < scale; i++ {
						rows = append(rows, mkRow(0, fmt.Sprintf("dup-%d", i)))
					}
				}
				match1 := mkRow(1, "m1")
				match2 := mkRow(1, "m2")
				rows = append(rows, match1, match2)
				applyClusterRows(t, state, packer, pool, rows)

				var lb, ub []byte
				if fullKey {
					lb = encodeClusterKeyForFilter(packer, encodeVarlenClusterKey(packer, 0, "dup"), false)
					ubTop := "m2"
					if tc.op == readutil.RangeBothOpen {
						// both-open also excludes the upper bound, so park it
						// above the second match.
						ubTop = "m3"
					}
					ub = encodeClusterKeyForFilter(
						packer, encodeVarlenClusterKey(packer, 1, ubTop), false)
				} else {
					// The prefix bound pins the leading int part only: every
					// a=0 serial carries it, whatever the string suffix.
					partPrefix := func(a int32) []byte {
						packer.Reset()
						packer.EncodeInt32(a)
						return packer.Bytes()
					}
					lb = encodeClusterKeyForFilter(packer, partPrefix(0), true)
					ub = encodeClusterKeyForFilter(packer, partPrefix(2), true)
				}

				iter := state.NewClusterKeyIter(
					types.BuildTS(int64(10+next), 0), tc.op, [][]byte{lb, ub})
				var got []types.Rowid
				for iter.Next() {
					got = append(got, iter.Entry().RowID)
				}
				require.NoError(t, iter.Close())
				require.Len(t, got, 2, "only the two rows above the open bound match")
				require.ElementsMatch(t,
					[]types.Rowid{match1.rowID, match2.rowID}, got)
			})
		}
	}
}

// TestClusterKeyIterMatchesRowsIterRandomReplay shuffles the replay order of
// inserts, deletes, and re-inserts and proves the cluster iteration still
// equals the exhaustive oracle for every order. Row identities are fixed
// before the operations are built, so every delete targets exactly the row
// its insert created; re-inserts (the update shape) use fresh identities and
// may carry a changed cluster value. A same-timestamp mode deletes each
// targeted row at its insert timestamp.
func TestClusterKeyIterMatchesRowsIterRandomReplay(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	const n = 30
	insertSeg := objectio.NewSegmentid()
	reinsertSeg := objectio.NewSegmentid()
	insertRowID := func(i int) types.Rowid {
		return objectio.NewRowid(objectio.NewBlockid(insertSeg, uint16(i%4), 0), uint32(i))
	}
	reinsertRowID := func(i int) types.Rowid {
		return objectio.NewRowid(objectio.NewBlockid(reinsertSeg, uint16(i%4+8), 0), uint32(i))
	}
	serialOfRow := func(i int) []byte {
		return encodeVarlenClusterKey(packer, int32(i%3), fmt.Sprintf("k%d", i%5))
	}
	updatedSerialOfRow := func(i int) []byte {
		// the update shape: the re-inserted row migrates to another cluster
		// value, so both the old and the new key must behave correctly.
		return encodeVarlenClusterKey(packer, int32((i+1)%3), fmt.Sprintf("k%d", (i+7)%5))
	}

	buildOps := func(sameTS bool) []func(state *PartitionState) {
		var ops []func(state *PartitionState)
		for i := 0; i < n; i++ {
			rowID := insertRowID(i)
			ts := types.BuildTS(int64(10+i), 0)
			serial := serialOfRow(i)
			ops = append(ops, func(state *PartitionState) {
				applyClusterRows(t, state, packer, pool, []clusterRow{{
					rowID: rowID, ts: ts, serial: serial, pk: int64(i),
				}})
			})
		}
		for i := 0; i < n; i += 3 {
			rowID := insertRowID(i)
			ts := types.BuildTS(int64(100+i), 0)
			if sameTS {
				ts = types.BuildTS(int64(10+i), 0)
			}
			ops = append(ops, func(state *PartitionState) {
				applyClusterDeletes(t, state, packer, pool, []clusterRow{{
					rowID: rowID, ts: ts, pk: int64(i),
				}})
			})
		}
		for i := 0; i < n; i += 3 {
			rowID := reinsertRowID(i)
			ts := types.BuildTS(int64(200+i), 0)
			serial := serialOfRow(i)
			if i%2 == 0 {
				serial = updatedSerialOfRow(i)
			}
			ops = append(ops, func(state *PartitionState) {
				applyClusterRows(t, state, packer, pool, []clusterRow{{
					rowID: rowID, ts: ts, serial: serial, pk: int64(1000 + i),
				}})
			})
		}
		return ops
	}

	kB := encodeVarlenClusterKey(packer, 1, "k1")
	prefix0 := encodeVarlenClusterKey(packer, 0, "")
	prefix1 := encodeVarlenClusterKey(packer, 1, "")
	prefix2 := encodeVarlenClusterKey(packer, 2, "")
	predicates := []clusterPredicate{
		{
			name: "equal",
			op:   function.EQUAL,
			keys: [][]byte{encodeClusterKeyForFilter(packer, kB, false)},
			match: func(serial []byte) bool {
				return bytes.Equal(serial, kB)
			},
		},
		{
			name: "prefix-between",
			op:   function.PREFIX_BETWEEN,
			keys: [][]byte{
				encodeClusterKeyForFilter(packer, prefix0, true),
				encodeClusterKeyForFilter(packer, prefix1, true),
			},
			match: func(serial []byte) bool {
				return types.PrefixCompare(serial, prefix0) >= 0 &&
					types.PrefixCompare(serial, prefix1) <= 0
			},
		},
		{
			name: "union-of-two",
			specs: []readutil.MemPKFilterSpec{
				{Op: function.EQUAL, Keys: [][]byte{encodeClusterKeyForFilter(packer, kB, false)}},
				{Op: function.PREFIX_BETWEEN, Keys: [][]byte{
					encodeClusterKeyForFilter(packer, prefix1, true),
					encodeClusterKeyForFilter(packer, prefix2, true),
				}},
			},
			match: func(serial []byte) bool {
				return bytes.Equal(serial, kB) ||
					(types.PrefixCompare(serial, prefix1) >= 0 &&
						types.PrefixCompare(serial, prefix2) <= 0)
			},
		},
	}

	snapshots := []types.TS{
		types.BuildTS(15, 0),
		types.BuildTS(150, 0),
		types.BuildTS(250, 0),
		types.BuildTS(5000, 0),
	}

	// serialOfEntry reads a row version's cluster bytes from the retained
	// replay batch, the ground truth the index must agree with.
	serialOfEntry := func(entry *RowEntry) []byte {
		vec := entry.Batch.Vecs[len(entry.Batch.Vecs)-1]
		return vec.GetBytesAt(int(entry.Offset))
	}

	countLive := func(state *PartitionState, snap types.TS) int {
		iter := state.NewRowsIter(snap, nil, false)
		count := 0
		for iter.Next() {
			count++
		}
		require.NoError(t, iter.Close())
		return count
	}

	for _, mode := range []struct {
		name   string
		sameTS bool
	}{
		{"later-delete-ts", false},
		{"same-delete-ts", true},
	} {
		for seed := int64(1); seed <= 5; seed++ {
			state := NewPartitionState("", false, 42, false)
			ops := buildOps(mode.sameTS)
			rnd := rand.New(rand.NewSource(seed))
			rnd.Shuffle(len(ops), func(i, j int) { ops[i], ops[j] = ops[j], ops[i] })
			for _, op := range ops {
				op(state)
			}

			// Independent visibility assertions: the deletes must remove
			// exactly the targeted inserts, and the re-inserts must add the
			// rows back, regardless of arrival order.
			deleted := 0
			for i := 0; i < n; i += 3 {
				deleted++
			}
			if mode.sameTS {
				require.Equal(t, n-deleted, countLive(state, types.BuildTS(int64(10+n), 0)),
					"same-timestamp deletes must hide their rows")
				require.Equal(t, n-deleted, countLive(state, types.BuildTS(150, 0)))
			} else {
				require.Equal(t, n, countLive(state, types.BuildTS(int64(10+n), 0)),
					"before the deletes every insert must be visible")
				require.Equal(t, n-deleted, countLive(state, types.BuildTS(150, 0)),
					"each delete must remove exactly its own insert")
			}
			require.Equal(t, n, countLive(state, types.BuildTS(250, 0)),
				"re-inserts must restore the deleted rows")

			for _, snap := range snapshots {
				for _, pred := range predicates {
					expected := make(map[types.Rowid]struct{})
					oracle := state.NewRowsIter(snap, nil, false)
					for oracle.Next() {
						entry := oracle.Entry()
						if pred.match(serialOfEntry(entry)) {
							expected[entry.RowID] = struct{}{}
						}
					}
					require.NoError(t, oracle.Close())

					var iter RowsIter
					if pred.specs != nil {
						iter = state.newClusterKeyIterWithFilters(snap, pred.specs)
					} else {
						iter = state.NewClusterKeyIter(snap, pred.op, pred.keys)
					}
					got := make(map[types.Rowid]struct{})
					for iter.Next() {
						got[iter.Entry().RowID] = struct{}{}
					}
					require.NoError(t, iter.Close())
					require.Equal(t, expected, got,
						"mode=%s seed=%d snap=%s pred=%s", mode.name, seed, snap.ToString(), pred.name)
				}
			}
		}
	}
}

// BenchmarkClusterIndexInsertMaintenance measures the replay-side cost of
// maintaining the cluster index: the same insert batches with and without the
// compound cluster column.
func BenchmarkClusterIndexInsertMaintenance(b *testing.B) {
	for _, mode := range []struct {
		name      string
		compound  bool
		wantReady bool
	}{
		{"plain", false, false},
		{"cluster", true, true},
	} {
		b.Run(mode.name, func(b *testing.B) {
			pool := mpool.MustNewZero()
			// Original fixture vectors are released each iteration below;
			// the drain proof asserts the pool empties before deletion.
			drainClusterPool(b, pool)
			packer := types.NewPacker()
			defer packer.Close()

			const batchRows = 1000
			rowIDs := make([]types.Rowid, batchRows)
			for i := range rowIDs {
				rowIDs[i] = objectio.NewRowid(
					objectio.NewBlockid(objectio.NewSegmentid(), uint16(i%8), 0), uint32(i))
			}
			serials := make([][]byte, batchRows)
			for i := range serials {
				packer.Reset()
				packer.EncodeInt32(int32(i % 8))
				packer.EncodeStringType([]byte(fmt.Sprintf("k%d", i%64)))
				serials[i] = append([]byte(nil), packer.Bytes()...)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				state := NewPartitionState("", false, 42, false)
				// Each iteration owns and releases its original fixture
				// vectors: the state only retains aliasing views, so the
				// bytes must return to the pool instead of accumulating
				// per iteration.
				fixtures := &clusterVecOwner{pool: pool}
				for batch := 0; batch < 10; batch++ {
					rowIDVec := vector.NewVec(types.T_Rowid.ToType())
					tsVec := vector.NewVec(types.T_TS.ToType())
					pkVec := vector.NewVec(types.T_int64.ToType())
					clusterVec := vector.NewVec(types.T_varchar.ToType())
					fixtures.track(rowIDVec, tsVec, pkVec, clusterVec)
					for i := 0; i < batchRows; i++ {
						vector.AppendFixed(rowIDVec, rowIDs[i], false, pool)
						vector.AppendFixed(tsVec,
							types.BuildTS(int64(batch*batchRows+i), 0), false, pool)
						vector.AppendFixed(pkVec, int64(batch*batchRows+i), false, pool)
						vector.AppendBytes(clusterVec, serials[i], false, pool)
					}
					attrs := []string{"rowid", "time", "pk"}
					vecs := []api.Vector{
						mustVectorToProto(rowIDVec),
						mustVectorToProto(tsVec),
						mustVectorToProto(pkVec),
					}
					if mode.compound {
						attrs = append(attrs, clusterTestCompoundAttr)
						vecs = append(vecs, mustVectorToProto(clusterVec))
					}
					state.HandleRowsInsert(context.Background(), &api.Batch{
						Attrs: attrs,
						Vecs:  vecs,
					}, 0, packer, pool, clusterTestKey(clusterTestCompoundAttr, 1))
				}
				fixtures.Free()
			}
		})
	}
}

// TestClusterKeyIndexIgnoresNonVarCharPrefixedUserColumn replays an ordinary
// non-clustered table whose only prefixed attribute is a legal user column of
// a type the key encoder does not support (GEOMETRY, non-NULL values). The
// replay must succeed, the rows must stay visible, and no index may be built:
// a prefixed name alone does not identify the compound column.
func TestClusterKeyIndexIgnoresNonVarCharPrefixedUserColumn(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()

	state := NewPartitionState("", false, 42, false)
	sid := objectio.NewSegmentid()

	rowIDVec := vector.NewVec(types.T_Rowid.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())
	pkVec := vector.NewVec(types.T_int64.ToType())
	geomVec := vector.NewVec(types.T_geometry.ToType())
	newClusterVecOwner(t, pool, rowIDVec, tsVec, pkVec, geomVec)
	var rowIDs []types.Rowid
	for i := 0; i < 4; i++ {
		rowID := objectio.NewRowid(objectio.NewBlockid(sid, uint16(i), 0), uint32(i))
		rowIDs = append(rowIDs, rowID)
		vector.AppendFixed(rowIDVec, rowID, false, pool)
		vector.AppendFixed(tsVec, types.BuildTS(int64(10+i), 0), false, pool)
		vector.AppendFixed(pkVec, int64(i), false, pool)
		// Non-NULL geometry payloads: WKB-shaped bytes.
		vector.AppendBytes(geomVec, []byte{0x01, 0x02, byte(i), 0x00, 0x00, 0x00}, false, pool)
	}
	state.HandleRowsInsert(context.Background(), &api.Batch{
		Attrs: []string{"rowid", "time", "pk", "__mo_cbkey_usercopy"},
		Vecs: []api.Vector{
			mustVectorToProto(rowIDVec),
			mustVectorToProto(tsVec),
			mustVectorToProto(pkVec),
			mustVectorToProto(geomVec),
		},
	}, 0, packer, pool)

	require.Equal(t, 4, state.rows.Len(),
		"replaying a table with a prefixed GEOMETRY user column must not panic or lose rows")
	require.Equal(t, 0, clusterIndexLen(state))
	require.False(t, state.ClusterIndexReadyFor(*clusterTestKey("__mo_cbkey_usercopy", 1)))

	// Row visibility is unchanged through the general iteration.
	got := make(map[types.Rowid]struct{})
	iter := state.NewRowsIter(types.BuildTS(100, 0), nil, false)
	for iter.Next() {
		got[iter.Entry().RowID] = struct{}{}
	}
	require.NoError(t, iter.Close())
	require.Len(t, got, 4)
	for _, rowID := range rowIDs {
		require.Contains(t, got, rowID)
	}
}

func clusterTestKey(name string, seq uint32) *readutil.ClusterKeyDescriptor {
	return &readutil.ClusterKeyDescriptor{TableID: 42, Name: name, Seqnum: seq, Through: types.MaxTs()}
}
func clusterIndexLen(state *PartitionState) int {
	if state.rowClusterIndex == nil {
		return 0
	}
	return state.rowClusterIndex.Len()
}

func TestClusterKeyDeclarationAndDisabledPayloadCost(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()
	row := clusterRow{rowID: types.RandomRowid(), ts: types.BuildTS(1, 0), pk: 1, serial: []byte(strings.Repeat("x", 32768))}
	for _, name := range []string{"__mo_cbkey_usercopy", clusterTestCompoundAttr} {
		t.Run("undeclared/"+name, func(t *testing.T) {
			state := NewPartitionState("", false, 42, false)
			input := makeClusterReplayBatch(t, pool, []clusterRow{row}, []string{"rowid", "time", "pk", name})
			before := packer.Allocated()
			state.HandleRowsInsert(t.Context(), input, 0, packer, pool)
			require.Nil(t, state.rowClusterIndex)
			require.False(t, state.WantsClusterKeyMetadata())
			require.Equal(t, before, packer.Allocated(), "no declaration means no payload encoding")
			iter := state.NewRowsIter(types.MaxTs(), nil, false)
			defer iter.Close()
			require.True(t, iter.Next())
			require.Equal(t, row.rowID, iter.Entry().RowID)
			require.False(t, iter.Next())
		})
	}
	state := NewPartitionState("", false, 42, false)
	small := row
	small.serial = []byte("small")
	applyClusterRows(t, state, packer, pool, []clusterRow{small})
	snapshot := state.Copy()
	plain := small
	plain.rowID = types.RandomRowid()
	applyClusterRowsWithAttrs(t, state, packer, pool, []clusterRow{plain}, []string{"rowid", "time", "pk"})
	require.Nil(t, state.rowClusterIndex)
	before := packer.Allocated()
	row.rowID = types.RandomRowid()
	applyClusterRows(t, state, packer, pool, []clusterRow{row})
	require.Equal(t, before, packer.Allocated(), "disabled state skips payload-sized work")
	require.Nil(t, state.rowClusterIndex)
	require.Equal(t, 3, state.rows.Len())
	require.True(t, snapshot.ClusterIndexReadyFor(*clusterTestKey(clusterTestCompoundAttr, 1)))
	require.Equal(t, 1, clusterIndexLen(snapshot), "disablement affects only the mutable copy")
}

func TestClusterKeySameTimestampDeleteReleasesInsertion(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()
	state := NewPartitionState("", false, 42, false)
	row := clusterRow{rowID: types.RandomRowid(), ts: types.BuildTS(1, 0), pk: 1, serial: []byte("one")}
	applyClusterRows(t, state, packer, pool, []clusterRow{row})
	snapshot := state.Copy()
	applyClusterDeletes(t, state, packer, pool, []clusterRow{row})
	require.Zero(t, clusterIndexLen(state), "a replaced insertion no longer owns a cluster entry")
	require.Equal(t, 1, clusterIndexLen(snapshot))
	iter := state.NewRowsIter(types.MaxTs(), nil, false)
	defer iter.Close()
	require.False(t, iter.Next())
}

// Cost fallback may return a superset: the consumer's residual predicate still
// decides SQL matches. Direct-index tests above independently prove MVCC.
func TestClusterKeyReadSelection(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()
	state := NewPartitionState("", false, 42, false)
	var rows []clusterRow
	for i := 0; i < 32; i++ {
		value := int32(0)
		if i == 31 {
			value = 1
		}
		rows = append(rows, clusterRow{rowID: types.RandomRowid(), ts: types.BuildTS(10, 0), pk: int64(i), serial: encodeVarlenClusterKey(packer, value, "dup")})
	}
	applyClusterRows(t, state, packer, pool, rows)
	dup := encodeClusterKeyForFilter(packer, rows[0].serial, false)
	rare := encodeClusterKeyForFilter(packer, rows[31].serial, false)
	for _, tc := range []struct {
		name       string
		filters    []readutil.MemPKFilterSpec
		indexed    bool
		candidates int
	}{
		{"selective", []readutil.MemPKFilterSpec{{Op: function.EQUAL, Keys: [][]byte{rare}}}, true, 1},
		{"broad_duplicates", []readutil.MemPKFilterSpec{{Op: function.EQUAL, Keys: [][]byte{dup}}}, false, 32},
		{"excluded_duplicates", []readutil.MemPKFilterSpec{{Op: function.GREAT_THAN, Keys: [][]byte{dup}}}, true, 1},
		{"overlap_conservative", []readutil.MemPKFilterSpec{{Op: function.EQUAL, Keys: [][]byte{rare}}, {Op: function.EQUAL, Keys: [][]byte{rare}}}, true, 1},
		{"many_intervals", make([]readutil.MemPKFilterSpec, 5), false, 32},
		{"many_keys", []readutil.MemPKFilterSpec{{Op: function.IN, Keys: make([][]byte, 9)}}, false, 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			iter := state.NewClusterKeyIterWithFilters(types.MaxTs(), *clusterTestKey(clusterTestCompoundAttr, 1), tc.filters)
			_, exhaustive := iter.(*rowsIter)
			require.Equal(t, !tc.indexed, exhaustive)
			require.Len(t, clusterIterRowIDs(t, iter), tc.candidates)
		})
	}
	// A historical snapshot must not hide retained future candidates from the
	// cost probe; its iterator still filters their visibility correctly.
	require.False(t, state.clusterCandidatesSelective([]readutil.MemPKFilterSpec{{Op: function.EQUAL, Keys: [][]byte{dup}}}))
}

func TestClusterKeyRejectsUnprovenCoverage(t *testing.T) {
	pool := mpool.MustNewZero()
	drainClusterPool(t, pool)
	packer := types.NewPacker()
	defer packer.Close()
	row := clusterRow{rowID: types.RandomRowid(), ts: types.BuildTS(10, 0), pk: 1, serial: []byte("key")}
	input := makeClusterReplayBatch(t, pool, []clusterRow{row}, []string{"rowid", "time", "pk", clusterTestCompoundAttr})
	for _, tc := range []struct {
		name   string
		change func(*readutil.ClusterKeyDescriptor)
	}{
		{"wrong_table", func(k *readutil.ClusterKeyDescriptor) { k.TableID++ }},
		{"wrong_sequence", func(k *readutil.ClusterKeyDescriptor) { k.Seqnum = 0 }},
		{"wrong_name", func(k *readutil.ClusterKeyDescriptor) { k.Name = "other" }},
		{"before_schema", func(k *readutil.ClusterKeyDescriptor) { k.Effective = types.BuildTS(11, 0) }},
		{"after_waterline", func(k *readutil.ClusterKeyDescriptor) { k.Through = types.BuildTS(9, 0) }},
		{"missing_waterline", func(k *readutil.ClusterKeyDescriptor) { k.Through = types.TS{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := NewPartitionState("", false, 42, false)
			key := clusterTestKey(clusterTestCompoundAttr, 1)
			tc.change(key)
			state.HandleRowsInsert(t.Context(), input, 0, packer, pool, key)
			require.Nil(t, state.rowClusterIndex)
			require.False(t, state.WantsClusterKeyMetadata())
			require.Equal(t, []types.Rowid{row.rowID}, clusterIterRowIDs(t, state.NewClusterKeyIterWithFilters(types.MaxTs(), *key, nil)))
		})
	}
}
