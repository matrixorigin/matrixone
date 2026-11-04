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

// The DATA BRANCH / CLONE generation boundary for a copied fulltext2 index
// (#28847): RestoreTable copies the already-built index (a tag=0 base at the
// copied recency) and then maintains the copy from a CDC range that starts
// BEFORE the clone's own row appends. A branch DELETE of a row inherited from
// that copied base must shadow the copied posting, because the covered search
// path projects straight from the index and has no base-table join to mask a
// stale entry.
package fulltext2

import (
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// branchIncludeTypes is the INCLUDE(tag) schema of the issue's index: carrying it
// makes the search covered, so a stale posting would be returned to the user
// rather than filtered by a join.
var branchIncludeTypes = []int32{int32(types.T_int64)}

// mkCopiedBase models the index state RestoreTable copies onto the branch: the
// inherited rows at the copied base recency (0), below every tail chunk_id.
func mkCopiedBase(t *testing.T, docs []TokenizedDoc) *Segment {
	t.Helper()
	seg, err := BuildSegmentFromTokenized("copied-base", int32(types.T_int64), docs,
		WithIncludeTypes(branchIncludeTypes))
	require.NoError(t, err)
	seg.Recency = 0
	return seg
}

// buildBranchTail drives the CDC blobs through one TailBuilder with the INCLUDE
// schema, then assembles the queryable Index from the Finish-order frames exactly
// like LoadTailSegments does (recency = position, above the copied base).
func buildBranchTail(t *testing.T, base *Segment, cdcs ...*Cdc) *Index {
	t.Helper()
	tb, err := NewTailBuilder(int32(types.T_int64), 1000, 0, "", wsTokenize,
		WithIncludeTypes(branchIncludeTypes))
	require.NoError(t, err)
	defer tb.Cleanup()
	for _, c := range cdcs {
		require.NoError(t, tb.AddBatch(c))
	}
	segs, err := tb.Finish()
	require.NoError(t, err)
	return framesToIndex(t, base, segs)
}

// sortedIDs normalizes a search's BM25-ranked pks so assertions test membership,
// not scoring order.
func sortedIDs(t *testing.T, idx *Index, word string) []any {
	t.Helper()
	ids := queryIDs(t, idx, word)
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].(int64) < ids[j].(int64)
	})
	return ids
}

// liveDocIncludes returns pk -> INCLUDE values for every live doc the index
// resolves, the ground truth a covered scan projects from.
func liveDocIncludes(t *testing.T, idx *Index) map[int64][]any {
	t.Helper()
	out := map[int64][]any{}
	for d, err := range idx.ReconstructLiveDocs(false) {
		require.NoError(t, err)
		out[d.Pk.(int64)] = d.Include
	}
	return out
}

// issue28847CopiedBase is the source table's synced index: pk1 (deleted on the
// branch) and pk2 (retained) both carry the token the branch query searches for
// or its sibling, plus their INCLUDE(tag) values.
func issue28847CopiedBase(t *testing.T) *Segment {
	t.Helper()
	return mkCopiedBase(t, []TokenizedDoc{
		{Pk: int64(1), Terms: []string{"alpha", "inherited", "row"},
			Positions: []int32{0, 6, 14}, Include: []any{int64(10)}},
		{Pk: int64(2), Terms: []string{"beta", "retained", "row"},
			Positions: []int32{0, 5, 14}, Include: []any{int64(20)}},
	})
}

// TestCopiedBaseShadowedByBranchDelete is the #28847 proof at the index layer:
// a branch DELETE of an inherited row, delivered as its own tail frame, removes
// the copied posting from BOTH the covered search and the resolved live set,
// while the retained inherited row and the newly inserted branch row survive.
func TestCopiedBaseShadowedByBranchDelete(t *testing.T) {
	del := NewCdc(int32(types.T_int64))
	del.Delete(int64(1))
	ins := NewCdc(int32(types.T_int64))
	ins.Insert(int64(3), "alpha branch new", []any{int64(30)})

	idx := buildBranchTail(t, issue28847CopiedBase(t), del, ins)

	// Covered search: the deleted inherited row must not come back.
	require.Equal(t, []any{int64(3)}, sortedIDs(t, idx, "alpha"),
		"the branch delete must shadow the copied posting of pk1")
	require.Equal(t, []any{int64(2)}, sortedIDs(t, idx, "beta"),
		"a retained inherited row is unaffected")

	// Liveness ground truth, with INCLUDE values intact for the covered path.
	live := liveDocIncludes(t, idx)
	require.NotContains(t, live, int64(1), "pk1 is dead")
	require.Equal(t, []any{int64(20)}, live[2], "pk2 keeps its copied INCLUDE value")
	require.Equal(t, []any{int64(30)}, live[3], "pk3 keeps its branch INCLUDE value")
}

// TestCopiedBaseWithoutDeleteFrameReturnsGhost is the counterexample that keeps
// the fix honest: the index layer alone cannot detect the missing tombstone. If
// the CDC net-effect filter coalesces the clone's insert+delete pair away (the
// pre-fix behaviour), the copied posting is returned by the covered search. This
// is exactly the ghost row the issue reports, so it pins that the engine must
// DELIVER the delete frame rather than the index having to infer it.
func TestCopiedBaseWithoutDeleteFrameReturnsGhost(t *testing.T) {
	ins := NewCdc(int32(types.T_int64))
	ins.Insert(int64(3), "alpha branch new", []any{int64(30)})

	idx := buildBranchTail(t, issue28847CopiedBase(t), ins)

	require.Equal(t, []any{int64(1), int64(3)}, sortedIDs(t, idx, "alpha"),
		"without the tombstone the copied posting stays queryable")
	require.Contains(t, liveDocIncludes(t, idx), int64(1))
}

// TestCopiedBaseTailRecencyIsAboveBase pins the generation invariant the shadowing
// depends on: every tail frame produced for the branch ranks above the copied
// base, so a branch tombstone always wins over a copied posting. If this ever
// inverted, the delete would be ignored and the ghost would return.
func TestCopiedBaseTailRecencyIsAboveBase(t *testing.T) {
	base := issue28847CopiedBase(t)
	del := NewCdc(int32(types.T_int64))
	del.Delete(int64(1))

	tb, err := NewTailBuilder(int32(types.T_int64), 1000, 0, "", wsTokenize,
		WithIncludeTypes(branchIncludeTypes))
	require.NoError(t, err)
	defer tb.Cleanup()
	require.NoError(t, tb.AddBatch(del))
	frames, err := tb.Finish()
	require.NoError(t, err)
	require.NotEmpty(t, frames, "the branch delete must produce a tail frame")

	// framesToIndex assigns recency = 100 + position; assert the ordering it
	// relies on rather than the literal constant.
	idx := framesToIndex(t, base, frames)
	require.Empty(t, sortedIDs(t, idx, "alpha"),
		"the copied posting of pk1 is shadowed by the higher-recency delete")

	// A delete ranked BELOW the copied base must NOT shadow it: recency, not mere
	// presence, is what makes the tombstone win.
	below := NewIndex([]*Segment{base}, map[any]int64{normalizeKey(int64(1)): -1})
	require.Equal(t, []any{int64(1)}, sortedIDs(t, below, "alpha"),
		"an older delete cannot shadow a newer copied posting")
}

// TestCopiedBaseDeleteThenReinsertAcrossFrames covers the opposite ordering on a
// copied base: a branch DELETE of an inherited row followed by a re-INSERT of the
// same pk must leave the NEW version live (no lingering tombstone) and drop the
// copied terms.
func TestCopiedBaseDeleteThenReinsertAcrossFrames(t *testing.T) {
	del := NewCdc(int32(types.T_int64))
	del.Delete(int64(1))
	ins := NewCdc(int32(types.T_int64))
	ins.Insert(int64(1), "alpha reborn", []any{int64(99)})

	idx := buildBranchTail(t, issue28847CopiedBase(t), del, ins)

	require.Equal(t, []any{int64(1)}, sortedIDs(t, idx, "alpha"),
		"the re-inserted row is live again")
	require.Empty(t, sortedIDs(t, idx, "inherited"),
		"the copied version's terms are superseded")
	require.Equal(t, []any{int64(99)}, liveDocIncludes(t, idx)[1],
		"the live copy carries the branch INCLUDE value")
}

// TestCopiedBaseUpdateInheritedRow covers the branch UPDATE of an inherited row
// (#27950 regression at the index layer): the copied terms must disappear and the
// new terms resolve to the same pk.
func TestCopiedBaseUpdateInheritedRow(t *testing.T) {
	del := NewCdc(int32(types.T_int64))
	del.Delete(int64(1))
	ins := NewCdc(int32(types.T_int64))
	ins.Insert(int64(1), "gamma updated", []any{int64(11)})

	idx := buildBranchTail(t, issue28847CopiedBase(t), del, ins)

	require.Empty(t, sortedIDs(t, idx, "alpha"), "the inherited text is gone")
	require.Equal(t, []any{int64(1)}, sortedIDs(t, idx, "gamma"), "the updated text is live")
	require.Equal(t, []any{int64(2)}, sortedIDs(t, idx, "beta"), "the untouched row survives")
}

// TestDeleteOfKeyAbsentFromIndexIsNoOp pins the tolerance the trailing-delete
// policy relies on. Keeping the delete of an insert-then-delete key means a sink
// can be handed a tombstone for a pk it never received — on a normal table that
// row was created and destroyed inside one CDC range, so neither the copied base
// nor any tail frame ever held it. Liveness keys deletes by pk and only consults
// them for pks that exist in a segment, so the extra tombstone must be inert: no
// error, no lost doc, no phantom.
func TestDeleteOfKeyAbsentFromIndexIsNoOp(t *testing.T) {
	base := issue28847CopiedBase(t)

	// A tail whose only frame deletes pk 9, which no segment holds.
	del := NewCdc(int32(types.T_int64))
	del.Delete(int64(9))
	idx := buildBranchTail(t, base, del)

	require.Equal(t, []any{int64(1)}, sortedIDs(t, idx, "alpha"), "an unrelated tombstone removes nothing")
	require.Equal(t, []any{int64(2)}, sortedIDs(t, idx, "beta"))
	live := liveDocIncludes(t, idx)
	require.Len(t, live, 2, "both copied rows are still live")
	require.Equal(t, []any{int64(10)}, live[1])
	require.Equal(t, []any{int64(20)}, live[2])

	// The same tolerance when the absent tombstone is mixed with real work: the
	// branch window that both deletes a phantom pk and inserts a live row.
	phantom := NewCdc(int32(types.T_int64))
	phantom.Delete(int64(9))
	ins := NewCdc(int32(types.T_int64))
	ins.Insert(int64(3), "alpha branch new", []any{int64(30)})
	mixed := buildBranchTail(t, issue28847CopiedBase(t), phantom, ins)

	require.Equal(t, []any{int64(1), int64(3)}, sortedIDs(t, mixed, "alpha"))
	require.Len(t, liveDocIncludes(t, mixed), 3)
	require.NotContains(t, liveDocIncludes(t, mixed), int64(9))
}
