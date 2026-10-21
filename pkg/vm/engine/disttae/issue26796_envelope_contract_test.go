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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// TestIssue26796EnvelopeWrittenOnceAndKeptByLifecycle verifies the envelope
// contract of the fix: written at insert time, widened (never narrowed) across
// statement-boundary merges that combine batches, and cleared when the entry
// is released.
func TestIssue26796EnvelopeWrittenOnceAndKeptByLifecycle(t *testing.T) {
	txn, pool := newIssue26796Txn(t)

	issue26796InsertBatch(t, txn, 5, 1, 9)
	require.Equal(t, 1, len(txn.writes))
	txn.Lock()
	entry := &txn.writes[0]
	require.True(t, entry.pkRange.inited)

	// envelope must equal the byte-wise min/max of the encoded keys
	var packer *types.Packer
	put := pool.Get(&packer)
	keys := readutil.EncodePrimaryKeyVector(entry.bat.Vecs[entry.pkCheckPos], packer)
	put.Put()
	minKey, maxKey := string(keys[0]), string(keys[0])
	for _, key := range keys[1:] {
		if string(key) < minKey {
			minKey = string(key)
		}
		if string(key) > maxKey {
			maxKey = string(key)
		}
	}
	require.Equal(t, minKey, string(entry.pkRange.min))
	require.Equal(t, maxKey, string(entry.pkRange.max))
	txn.Unlock()

	// >=30 small same-table entries force the merge path to coalesce batches
	for pk := int64(10); pk < int64(44); pk++ {
		issue26796InsertOne(t, txn, pk, pk)
	}
	txn.Lock()
	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.Unlock()

	allMerged := true
	for i := range txn.writes {
		if txn.writes[i].bat == nil || txn.writes[i].typ != INSERT {
			continue
		}
		if !txn.writes[i].pkRange.inited {
			allMerged = false
		}
	}
	require.True(t, allMerged,
		"merged insert batches must keep a conservative envelope")
	require.LessOrEqual(t, string(txn.writes[0].pkRange.min), minKey,
		"merged envelope must still cover the earliest key")
	require.GreaterOrEqual(t, string(txn.writes[0].pkRange.max), maxKey,
		"merged envelope must still cover the latest key")

	// release must clear the envelope with the batch
	txn.Lock()
	txn.releaseWorkspaceEntryBatchLocked(0)
	require.False(t, txn.writes[0].pkRange.inited)
	require.Zero(t, txn.writes[0].keyIndexAccountedSize,
		"releasing an entry must refund its retained index bytes")
	require.Nil(t, txn.writes[0].bat)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.Unlock()
}

// issue26796ProbeMeta snapshots one insert entry's probe metadata so a
// reference read can strip optimization state and restore it afterwards.
type issue26796ProbeMeta struct {
	idx        int
	tableId    uint64
	pkRange    insertKeyRange
	keyOffsets *insertKeyOffsets
}

// issue26796RequireSameRows drives the pruned read, then the unpruned read
// (all envelopes stripped, the exact pre-fix code path) and requires identical
// output rows. Reference-stripped state is RESTORED before returning, so later
// probes in a scenario keep exercising the optimized path; the restored state
// is required back to exactly what was saved.
func issue26796RequireSameRows(
	t *testing.T,
	txn *Transaction,
	pool *fileservice.Pool[*types.Packer],
	base readutil.BasePKFilter,
) (pruned []int64, prunedCnt issue26796Counters) {
	t.Helper()
	filter := issue26796Filter(t, pool, base)
	pruned, prunedCnt = issue26796PointRead(t, txn, filter)

	// strip all envelopes -> identical to the pre-fix behavior
	txn.Lock()
	saved := make([]issue26796ProbeMeta, 0, len(txn.writes))
	for i := range txn.writes {
		if txn.writes[i].bat != nil && txn.writes[i].typ == INSERT {
			saved = append(saved, issue26796ProbeMeta{
				idx:        i,
				tableId:    txn.writes[i].tableId,
				pkRange:    txn.writes[i].pkRange,
				keyOffsets: txn.writes[i].keyOffsets,
			})
			txn.writes[i].pkRange = insertKeyRange{}
		}
	}
	txn.Unlock()

	filter = issue26796Filter(t, pool, base)
	unpruned, _ := issue26796PointRead(t, txn, filter)

	// restore: without this, every later probe of the scenario would silently
	// run the legacy path and prove nothing about the optimized reader.
	txn.Lock()
	for _, s := range saved {
		require.Equal(t, s.tableId, txn.writes[s.idx].tableId,
			"reference read must not change the workspace shape")
		require.Equal(t, insertKeyRange{}, txn.writes[s.idx].pkRange,
			"the reference read must leave envelopes stripped until restored")
		txn.writes[s.idx].pkRange = s.pkRange
		// the reference read cannot hydrate: hydration requires the envelope.
		require.True(t, txn.writes[s.idx].keyOffsets == s.keyOffsets,
			"reference read must not rebuild dropped indexes")
		txn.writes[s.idx].keyOffsets = s.keyOffsets
	}
	txn.Unlock()

	require.ElementsMatch(t, unpruned, pruned,
		"envelope pruning changed the rows returned for filter op %d", base.Op)
	return pruned, prunedCnt
}

// TestIssue26796PruningKeepsReadSemantics is the counterexample matrix: for
// every supported filter kind and workspace shape, the pruned read must return
// exactly the rows the unpruned scan returns, including boundary keys,
// envelope-hit-but-row-miss keys, repeated primary keys, and partially deleted
// batches whose envelope is a stale superset.
func TestIssue26796PruningKeepsReadSemantics(t *testing.T) {
	enc := func(v int64) []byte { return types.EncodeFixed(v) }

	type filterCase struct {
		name string
		base readutil.BasePKFilter
	}
	loadRows := func(t *testing.T, txn *Transaction) {
		issue26796InsertBatch(t, txn, 5, 1, 9)
		for pk := int64(10); pk < int64(60); pk++ {
			issue26796InsertOne(t, txn, pk, pk)
		}
	}
	cases := []filterCase{
		{name: "equal-hit", base: readutil.BasePKFilter{Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(17)}},
		{name: "equal-miss", base: readutil.BasePKFilter{Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(10000)}},
		// 6 lies inside the batch {5,1,9} envelope but matches no row
		{name: "equal-envelope-hit-row-miss", base: readutil.BasePKFilter{Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(6)}},
		{name: "equal-batch-min-boundary", base: readutil.BasePKFilter{Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(1)}},
		{name: "equal-batch-max-boundary", base: readutil.BasePKFilter{Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(9)}},
		{name: "between", base: readutil.BasePKFilter{Valid: true, Op: function.BETWEEN, Oid: types.T_int64, LB: enc(6), UB: enc(20)}},
		{name: "between-open", base: readutil.BasePKFilter{Valid: true, Op: readutil.RangeBothOpen, Oid: types.T_int64, LB: enc(1), UB: enc(9)}},
		{name: "gt", base: readutil.BasePKFilter{Valid: true, Op: function.GREAT_THAN, Oid: types.T_int64, LB: enc(40)}},
		{name: "ge", base: readutil.BasePKFilter{Valid: true, Op: function.GREAT_EQUAL, Oid: types.T_int64, LB: enc(40)}},
		{name: "lt", base: readutil.BasePKFilter{Valid: true, Op: function.LESS_THAN, Oid: types.T_int64, LB: enc(5)}},
		{name: "le", base: readutil.BasePKFilter{Valid: true, Op: function.LESS_EQUAL, Oid: types.T_int64, LB: enc(5)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			txn, pool := newIssue26796Txn(t)
			loadRows(t, txn)
			issue26796RequireSameRows(t, txn, pool, tc.base)
		})
	}

	t.Run("in-filter", func(t *testing.T) {
		mp := mpool.MustNewZeroNoFixed()
		vec := vector.NewVec(types.T_int64.ToType())
		for _, v := range []int64{9, 33, 90} {
			require.NoError(t, vector.AppendFixed(vec, v, false, mp))
		}
		txn, pool := newIssue26796Txn(t)
		loadRows(t, txn)
		got, _ := issue26796RequireSameRows(t, txn, pool, readutil.BasePKFilter{
			Valid: true, Op: function.IN, Oid: types.T_int64, Vec: vec,
		})
		require.ElementsMatch(t, []int64{9, 33}, got)
	})

	t.Run("disjuncts", func(t *testing.T) {
		txn, pool := newIssue26796Txn(t)
		loadRows(t, txn)
		got, _ := issue26796RequireSameRows(t, txn, pool, readutil.BasePKFilter{
			Valid: true,
			Disjuncts: []readutil.BasePKFilter{
				{Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(1)},
				{Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(44)},
			},
		})
		require.ElementsMatch(t, []int64{1, 44}, got)
	})

	t.Run("repeated-pk-multiple-live-versions", func(t *testing.T) {
		// A primary key updated twice contributes two workspace versions; the
		// in-memory scan surfaces both and later stages resolve freshness.
		// Pruning must surface both exactly like the unpruned scan.
		txn, pool := newIssue26796Txn(t)
		issue26796InsertOne(t, txn, 7, 70)
		issue26796InsertOne(t, txn, 7, 71)
		issue26796InsertBatch(t, txn, 2, 8)
		got, _ := issue26796RequireSameRows(t, txn, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(7),
		})
		require.ElementsMatch(t, []int64{70, 71}, got,
			"both in-transaction versions of pk=7 must remain readable")
	})

	t.Run("partially-shrunk-batch-keeps-envelope-superset", func(t *testing.T) {
		// Deleting rows only shrinks the live rows inside the envelope; the
		// stale envelope stays a conservative superset and must not return or
		// hide rows incorrectly.
		txn, pool := newIssue26796Txn(t)
		issue26796InsertBatch(t, txn, 1, 2, 3)
		bat := txn.writes[0].bat
		txn.Lock()
		txn.addBatchSelectionsLocked(bat, []int64{1})
		require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		txn.Unlock()
		require.Equal(t, 2, bat.RowCount(),
			"the selected row (pk=2) must be shrunk out of the batch")

		got, _ := issue26796RequireSameRows(t, txn, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(2),
		})
		require.Empty(t, got, "deleted row must not be readable")

		got, cnt := issue26796RequireSameRows(t, txn, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(3),
		})
		require.Equal(t, []int64{3}, got)
		require.NotZero(t, cnt.IndexProbes,
			"the surviving row's probe must run on the shrunk batch's rebuilt index")
		require.LessOrEqual(t, cnt.EncodePKRows, uint64(2),
			"the rebuilt index covers only the surviving rows")
	})

	t.Run("after-merge-fat-batches", func(t *testing.T) {
		// After the statement-boundary merge coalesces singleton entries into
		// fat batches, boundary keys of the merged envelope must still read
		// correctly from both pruned and unpruned paths.
		txn, pool := newIssue26796Txn(t)
		for pk := int64(1); pk <= int64(64); pk++ {
			issue26796InsertOne(t, txn, pk, pk)
		}
		txn.Lock()
		require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
		txn.Unlock()
		multiRowEntries := 0
		for i := range txn.writes {
			if txn.writes[i].bat != nil && txn.writes[i].typ == INSERT &&
				txn.writes[i].bat.RowCount() > 1 {
				multiRowEntries++
			}
		}
		require.Positive(t, multiRowEntries, "this scenario must exercise merged multi-row batches")

		for _, key := range []int64{1, 17, 64, 65} {
			got, cnt := issue26796RequireSameRows(t, txn, pool, readutil.BasePKFilter{
				Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(key),
			})
			if key == 65 {
				require.Empty(t, got,
					"keys outside the merged envelope must read nothing")
				require.Zero(t, cnt.IndexProbes,
					"out-of-envelope keys skip the batch before the index")
			} else {
				require.Equal(t, []int64{key}, got)
				require.NotZero(t, cnt.IndexProbes,
					"probe %d must answer from the merged batch's index", key)
			}
		}
	})

	t.Run("cross-table-and-miss-entries", func(t *testing.T) {
		// Entries of other tables, nil batches, and empty batches must not
		// affect this table's pruned read.
		txn, pool := newIssue26796Txn(t)
		issue26796InsertBatch(t, txn, 5, 1, 9)
		other := batch.NewWithSize(2)
		other.Attrs = []string{issue26796PKName, "value"}
		other.SetVector(0, vector.NewVec(types.T_int64.ToType()))
		other.SetVector(1, vector.NewVec(types.T_int64.ToType()))
		require.NoError(t, vector.AppendFixed(other.GetVector(0), int64(17), false, txn.proc.Mp()))
		require.NoError(t, vector.AppendFixed(other.GetVector(1), int64(17), false, txn.proc.Mp()))
		other.SetRowCount(1)
		txn.Lock()
		txn.appendWorkspaceEntryLocked(Entry{
			typ:        INSERT,
			tableId:    issue26796TblID + 1,
			databaseId: issue26796DBID,
			bat:        other,
		})
		txn.writes = append(txn.writes, Entry{typ: INSERT, tableId: issue26796TblID, databaseId: issue26796DBID})
		txn.Unlock()

		got, _ := issue26796RequireSameRows(t, txn, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64, LB: enc(17),
		})
		require.Empty(t, got, "entries of other tables must not surface")
	})
}

// issue26796MergeTwoForTest mirrors the production statement-boundary merge
// for two small entries: mergeWorkspaceEntryBatchesLocked followed by the
// consecutive rowid regeneration foo() performs, which keeps the workspace
// batch's rowid offsets 0-based and consecutive as the reader path requires.
func issue26796MergeTwoForTest(t *testing.T, txn *Transaction, dstIdx, srcIdx int64) {
	t.Helper()
	txn.Lock()
	defer txn.Unlock()
	require.NoError(t, txn.mergeWorkspaceEntryBatchesLocked(context.Background(), dstIdx, srcIdx))
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	dst := &txn.writes[dstIdx]
	rowIdVector, err := txn.batchAllocNewRowIds(dst.bat.RowCount())
	require.NoError(t, err)
	defer rowIdVector.Free(txn.proc.Mp())
	rowIds := vector.MustFixedColNoTypeCheck[objectio.Rowid](rowIdVector)
	for x := range dst.bat.RowCount() {
		require.NoError(t, vector.SetFixedAtWithTypeCheck[objectio.Rowid](dst.bat.Vecs[0], x, rowIds[x]))
	}
}

// TestIssue26796KeyIndexLifecycle verifies the exact-key index contract:
// hydrated lazily on first point probe, invalidated by every batching mutation
// (merge, shrink, release), and always answering the CURRENT batch layout.
func TestIssue26796KeyIndexLifecycle(t *testing.T) {
	t.Run("hydrates_once_and_answers_repeated_probes", func(t *testing.T) {
		txn, pool := newIssue26796Txn(t)
		issue26796InsertBatch(t, txn, 7, 3, 9, 12)

		mk := func(key int64) *readutil.MemPKFilter {
			return issue26796Filter(t, pool, readutil.BasePKFilter{
				Valid: true, Op: function.EQUAL, Oid: types.T_int64,
				LB: types.EncodeFixed(key),
			})
		}

		// first probe hydrates the index; the cold probe pays one construction
		values, d := issue26796PointRead(t, txn, mk(3))
		require.Equal(t, []int64{3}, values)
		require.Zero(t, d.FilterVectorRows, "exact probe must not run the per-row filter")
		require.Equal(t, uint64(1), d.IndexBuilds, "the first exact probe builds the index once")
		require.Equal(t, uint64(4), d.EncodePKRows, "cold construction encodes exactly the batch's rows")

		require.True(t, txn.writes[0].keyOffsets != nil,
			"the first exact probe should have hydrated the key index")
		require.Positive(t, txn.writes[0].keyIndexAccountedSize,
			"hydration must charge the retained index bytes to the workspace budget")

		values, d = issue26796PointRead(t, txn, mk(9))
		require.Equal(t, []int64{9}, values)
		require.Zero(t, d.EncodePKRows, "repeated probes reuse the hydrated index without re-encoding")
		require.Zero(t, d.IndexBuilds)
		require.NotZero(t, d.IndexProbes)

		// definitive miss also encodes nothing
		values, d = issue26796PointRead(t, txn, mk(8))
		require.Empty(t, values)
		require.Zero(t, d.EncodePKRows)
		require.Zero(t, d.IndexBuilds)
		require.NotZero(t, d.IndexProbes, "a miss inside the envelope still probes the index")
	})

	t.Run("merge_invalidates_and_rehydrates_correctly", func(t *testing.T) {
		txn, pool := newIssue26796Txn(t)
		issue26796InsertBatch(t, txn, 1, 9)
		issue26796InsertBatch(t, txn, 20, 30)

		filter := issue26796Filter(t, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64,
			LB: types.EncodeFixed(int64(1)),
		})
		values, d := issue26796PointRead(t, txn, filter)
		require.Equal(t, []int64{1}, values)
		require.Zero(t, d.FilterVectorRows)
		require.True(t, txn.writes[0].keyOffsets != nil)

		// coalescing both batches invalidates the stale indexes
		issue26796MergeTwoForTest(t, txn, 0, 1)
		require.Nil(t, txn.writes[0].keyOffsets,
			"a merged batch must drop its stale key index")
		require.Nil(t, txn.writes[1].keyOffsets)
		require.Zero(t, txn.writes[0].keyIndexAccountedSize,
			"merging must refund the invalidated index bytes")

		for _, key := range []int64{9, 20, 30, 5} {
			got, cnt := issue26796RequireSameRows(t, txn, pool, readutil.BasePKFilter{
				Valid: true, Op: function.EQUAL, Oid: types.T_int64,
				LB: types.EncodeFixed(key),
			})
			if key == 5 {
				require.Empty(t, got)
			} else {
				require.Equal(t, []int64{key}, got)
			}
			// pruned probes must run on the rebuilt index, none on legacy
			require.Equal(t, uint64(1), cnt.IndexProbes,
				"probe %d must be answered by the merged batch's re-hydrated index", key)
			require.Zero(t, cnt.FilterVectorRows,
				"probe %d must never run the per-row filter for the pruned read", key)
		}
		require.Positive(t, txn.writes[0].keyIndexAccountedSize,
			"the re-hydrated index must be charged again")
	})

	t.Run("shrink_invalidates_index", func(t *testing.T) {
		txn, pool := newIssue26796Txn(t)
		issue26796InsertBatch(t, txn, 1, 2, 3)
		filter := issue26796Filter(t, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64,
			LB: types.EncodeFixed(int64(3)),
		})
		values, d := issue26796PointRead(t, txn, filter)
		require.Equal(t, []int64{3}, values)
		require.Zero(t, d.FilterVectorRows)
		require.True(t, txn.writes[0].keyOffsets != nil)

		bat := txn.writes[0].bat
		txn.Lock()
		txn.addBatchSelectionsLocked(bat, []int64{0})
		require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
		txn.Unlock()
		require.Nil(t, txn.writes[0].keyOffsets,
			"a shrunk batch must drop its stale key index")
		require.Zero(t, txn.writes[0].keyIndexAccountedSize,
			"shrinking must refund the invalidated index bytes")

		got, cnt := issue26796RequireSameRows(t, txn, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64,
			LB: types.EncodeFixed(int64(1)),
		})
		require.Empty(t, got, "the shrunk row must no longer be found")
		require.Equal(t, uint64(1), cnt.IndexProbes,
			"the deleted key still lies inside the stale-superset envelope, so the index must be probed and miss definitively")
		require.LessOrEqual(t, cnt.EncodePKRows, uint64(2),
			"the one cold rebuild after the shrink covers only the surviving rows")

		got, cnt = issue26796RequireSameRows(t, txn, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64,
			LB: types.EncodeFixed(int64(2)),
		})
		require.Equal(t, []int64{2}, got)
		require.Equal(t, uint64(1), cnt.IndexProbes,
			"the surviving row's probe must run on the post-shrink rebuilt index")
	})

	t.Run("repeated_pk_keeps_all_offsets", func(t *testing.T) {
		// once inlined into one batch by the merge, a repeated primary key must
		// surface every live offset
		txn, pool := newIssue26796Txn(t)
		issue26796InsertOne(t, txn, 7, 70)
		issue26796InsertOne(t, txn, 7, 71)
		issue26796MergeTwoForTest(t, txn, 0, 1)

		got, d := issue26796PointRead(t, txn, issue26796Filter(t, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64,
			LB: types.EncodeFixed(int64(7)),
		}))
		require.ElementsMatch(t, []int64{70, 71}, got)
		require.Zero(t, d.FilterVectorRows, "in-bat repeated keys must be answered by the index")
		require.NotZero(t, d.IndexProbes)
	})
}

// TestIssue26796KeyIndexMemoryAccounting verifies that the retained exact-key
// index participates in the workspace memory budget (issue 26796 review):
// charged before allocation, visible to the checkable totals, refunded by every
// invalidation and retirement path, and never charged without a live index.
func TestIssue26796KeyIndexMemoryAccounting(t *testing.T) {
	// estimates mirror insertKeyOffsetsEstimateSize: map base, one key copy
	// plus one amortized bucket and one uint32 offset slot per row.
	estimateFromKeys := func(keys [][]byte) uint64 {
		size := uint64(64)
		for _, k := range keys {
			size += uint64(len(k)) + 48 + 4
		}
		return size
	}
	keysOf := func(t *testing.T, txn *Transaction, pool *fileservice.Pool[*types.Packer], idx int) [][]byte {
		t.Helper()
		txn.Lock()
		defer txn.Unlock()
		entry := &txn.writes[idx]
		var packer *types.Packer
		put := pool.Get(&packer)
		defer put.Put()
		return readutil.EncodePrimaryKeyVector(entry.bat.Vecs[entry.pkCheckPos], packer)
	}
	probe := func(t *testing.T, txn *Transaction, pool *fileservice.Pool[*types.Packer], key int64) ([]int64, issue26796Counters) {
		return issue26796PointRead(t, txn, issue26796Filter(t, pool, readutil.BasePKFilter{
			Valid: true, Op: function.EQUAL, Oid: types.T_int64,
			LB: types.EncodeFixed(key),
		}))
	}

	txn, pool := newIssue26796Txn(t)
	issue26796InsertBatch(t, txn, 7, 3, 9, 12) // 4 rows
	issue26796InsertBatch(t, txn, 5, 6)        // 2 rows

	txn.Lock()
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	batBytes := txn.workspaceSize
	approxBytes := txn.approximateInMemInsertSize
	require.Zero(t, txn.writes[0].keyIndexAccountedSize)
	txn.Unlock()

	// hydration must charge both budgets before answering
	values, d := probe(t, txn, pool, 9)
	require.Equal(t, []int64{9}, values)
	require.Equal(t, uint64(1), d.IndexBuilds)

	charge4 := estimateFromKeys(keysOf(t, txn, pool, 0))
	txn.Lock()
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.Equal(t, charge4, txn.writes[0].keyIndexAccountedSize,
		"the hydrated index is charged by its deterministic estimate")
	require.Equal(t, batBytes+charge4, txn.workspaceSize,
		"workspace memory budget must include retained index bytes")
	require.Equal(t, approxBytes+charge4, txn.approximateInMemInsertSize,
		"the insert-side spill budget must see the retained index bytes too")
	txn.Unlock()

	// warm probes do not grow any budget
	values, d = probe(t, txn, pool, 7)
	require.Equal(t, []int64{7}, values)
	require.Zero(t, d.IndexBuilds)
	txn.Lock()
	require.Equal(t, batBytes+charge4, txn.workspaceSize)
	txn.Unlock()

	// merging refunds the invalidated index bytes of both sides
	issue26796MergeTwoForTest(t, txn, 0, 1)
	txn.Lock()
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.Zero(t, txn.writes[0].keyIndexAccountedSize,
		"merging must refund the invalidated index bytes")
	require.Nil(t, txn.writes[0].keyOffsets)
	require.Nil(t, txn.writes[1].bat, "the merged-away entry is retired")
	mergedBatBytes := txn.workspaceSize
	txn.Unlock()

	// rehydration on the merged batch charges for the merged layout
	values, d = probe(t, txn, pool, 6)
	require.Equal(t, []int64{6}, values)
	require.Equal(t, uint64(1), d.IndexBuilds)
	charge6 := estimateFromKeys(keysOf(t, txn, pool, 0))
	txn.Lock()
	require.Equal(t, charge6, txn.writes[0].keyIndexAccountedSize)
	require.Equal(t, mergedBatBytes+charge6, txn.workspaceSize)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.Unlock()

	// shrink refunds again
	txn.Lock()
	txn.addBatchSelectionsLocked(txn.writes[0].bat, []int64{0})
	require.NoError(t, txn.mergeTxnWorkspaceLocked(context.Background()))
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.Zero(t, txn.writes[0].keyIndexAccountedSize)
	txn.Unlock()

	// release refunds and leaves no residual charges
	values, d = probe(t, txn, pool, 9)
	require.Equal(t, []int64{9}, values)
	require.Equal(t, uint64(1), d.IndexBuilds)
	txn.Lock()
	require.Positive(t, txn.writes[0].keyIndexAccountedSize)
	txn.releaseWorkspaceEntryBatchLocked(0)
	require.Zero(t, txn.writes[0].keyIndexAccountedSize)
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	txn.Unlock()
}

// issue26796FakeMembership implements engine.MembershipFilter as an exact set
// over int64 keys, standing in for the docfilter membership filters that bind
// __mo_index_pri_col on index tables: probe rows must pass it in addition to
// the primary-key predicate.
type issue26796FakeMembership struct {
	accept map[int64]bool
}

func (m *issue26796FakeMembership) Test(data []byte) bool {
	return m.accept[types.DecodeInt64(data)]
}

func (m *issue26796FakeMembership) TestVector(
	v *vector.Vector,
	cb func(exist bool, isnull bool, row int),
) []uint8 {
	col := vector.MustFixedColNoTypeCheck[int64](v)
	for i, val := range col {
		cb(m.accept[val], false, i)
	}
	return nil
}

func (m *issue26796FakeMembership) Valid() bool { return true }
func (m *issue26796FakeMembership) Exact() bool { return true }
func (m *issue26796FakeMembership) Free()       {}

var _ engine.MembershipFilter = (*issue26796FakeMembership)(nil)

// TestIssue26796MembershipFilterGatesExactProbePath pins the review finding:
// an attached membership filter is an independent, possibly exact predicate
// over another column; an exact primary-key probe may never bypass it. While a
// membership filter is attached, the reader must stay on the FilterVector plus
// membership path; with no membership filter, the same probe takes the index.
func TestIssue26796MembershipFilterGatesExactProbePath(t *testing.T) {
	const (
		issue26796SecTableID = 43
		issue26796SecTable   = "sec_tbl"
	)

	tableDef := func() *pbplan.TableDef {
		return &pbplan.TableDef{
			Cols: []*pbplan.ColDef{
				{Name: issue26796PKName, Seqnum: 0, Typ: pbplan.Type{Id: int32(types.T_int64)}},
				{Name: "value", Seqnum: 1, Typ: pbplan.Type{Id: int32(types.T_int64)}},
				{Name: catalog.IndexTablePrimaryColName, Seqnum: 2, Typ: pbplan.Type{Id: int32(types.T_int64)}},
			},
			Pkey: &pbplan.PrimaryKeyDef{PkeyColName: issue26796PKName},
		}
	}

	insert := func(t *testing.T, txn *Transaction, pk, value, memberCol int64) {
		t.Helper()
		bat := batch.NewWithSize(3)
		bat.Attrs = []string{issue26796PKName, "value", catalog.IndexTablePrimaryColName}
		bat.SetVector(0, vector.NewVec(types.T_int64.ToType()))
		bat.SetVector(1, vector.NewVec(types.T_int64.ToType()))
		bat.SetVector(2, vector.NewVec(types.T_int64.ToType()))
		require.NoError(t, vector.AppendFixed(bat.GetVector(0), pk, false, txn.proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.GetVector(1), value, false, txn.proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.GetVector(2), memberCol, false, txn.proc.Mp()))
		bat.SetRowCount(1)
		_, err := txn.WriteBatch(
			INSERT, "", 1, issue26796DBID, issue26796SecTableID,
			issue26796DB, issue26796SecTable, bat, DNStore{},
		)
		require.NoError(t, err)
	}

	newTxn := func(t *testing.T) (*Transaction, *fileservice.Pool[*types.Packer]) {
		t.Helper()
		txn, pool := newIssue26796TxnWithTable(t, issue26796SecTable, issue26796SecTableID, tableDef())
		insert(t, txn, 1, 11, 101)
		insert(t, txn, 2, 22, 102)
		insert(t, txn, 3, 33, 103)
		return txn, pool
	}

	// mkFilter attaches an exact-set membership filter over __mo_index_pri_col
	// (the column setFilterHint binds for index tables) to an exact pk probe.
	mkMembership := func(vals ...int64) *issue26796FakeMembership {
		m := &issue26796FakeMembership{accept: make(map[int64]bool)}
		for _, v := range vals {
			m.accept[v] = true
		}
		return m
	}
	mkFilter := func(t *testing.T, pool *fileservice.Pool[*types.Packer], pk int64, m *issue26796FakeMembership) *readutil.MemPKFilter {
		t.Helper()
		hint := engine.FilterHint{}
		if m != nil {
			hint.BF = m
		}
		filter, err := readutil.NewMemPKFilter(
			tableDef(),
			types.MaxTs().ToTimestamp(),
			pool,
			readutil.BasePKFilter{
				Valid: true, Op: function.EQUAL, Oid: types.T_int64,
				LB: types.EncodeFixed(pk),
			},
			hint,
		)
		require.NoError(t, err)
		return &filter
	}

	point := func(t *testing.T, txn *Transaction, filter *readutil.MemPKFilter) ([]int64, issue26796Counters) {
		return issue26796PointReadTable(t, txn, filter, issue26796DBID, issue26796SecTableID, issue26796SecTable)
	}

	t.Run("accepting membership still surfaces the row", func(t *testing.T) {
		txn, pool := newTxn(t)
		filter := mkFilter(t, pool, 2, mkMembership(101, 102, 103))
		require.True(t, filter.HasMemberFilter(), "the filter must carry an attached membership filter")

		values, d := point(t, txn, filter)
		require.Equal(t, []int64{22}, values,
			"an accepted membership row must surface exactly like the unpruned path")
		require.Zero(t, d.IndexProbes,
			"an attached membership filter must keep the read on the FilterVector+membership path")
		require.Zero(t, d.IndexBuilds, "no index may hydrate behind a membership filter")
		txn.Lock()
		require.Nil(t, txn.writes[1].keyOffsets)
		txn.Unlock()
	})

	t.Run("rejecting membership drops the pk-matching row", func(t *testing.T) {
		txn, pool := newTxn(t)
		filter := mkFilter(t, pool, 2, mkMembership())

		values, d := point(t, txn, filter)
		require.Empty(t, values,
			"a pk hit whose membership value is excluded must not survive")
		require.Zero(t, d.IndexProbes)
		require.Zero(t, d.IndexBuilds)
	})

	t.Run("partial membership keeps only passing rows", func(t *testing.T) {
		txn, pool := newTxn(t)
		for _, tc := range []struct {
			pk   int64
			want []int64
		}{{1, []int64{11}}, {2, []int64{}}, {3, []int64{}}} {
			values, d := point(t, txn, mkFilter(t, pool, tc.pk, mkMembership(101)))
			require.Equal(t, tc.want, values)
			require.Zero(t, d.IndexProbes)
			require.Zero(t, d.IndexBuilds)
		}
	})

	t.Run("without membership the same probe takes the index", func(t *testing.T) {
		txn, pool := newTxn(t)
		filter := mkFilter(t, pool, 2, nil)
		require.False(t, filter.HasMemberFilter())

		values, d := point(t, txn, filter)
		require.Equal(t, []int64{22}, values)
		require.NotZero(t, d.IndexProbes,
			"without a membership filter the exact probe takes the index path")
		require.Equal(t, uint64(1), d.IndexBuilds)
		txn.Lock()
		require.NotNil(t, txn.writes[1].keyOffsets)
		txn.Unlock()
	})
}
