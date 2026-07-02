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

package wand

import "testing"

// TestWandTailFrames round-trips insert-segment and delete frames through the
// tag=1 CdcTail codec (FrameSegment/FrameDeletes -> AssembleFrames) and asserts
// the assembled segments carry their frame chunk_id and drive correct liveness:
// an UPDATE (same pk in a later segment) dedups to the newest, and a DELETE at a
// higher chunk_id kills the older copy.
func TestWandTailFrames(t *testing.T) {
	// segA: docs 5,6 ; segB: doc 5 (an update of pk 5) ; then delete pk 6.
	segA := buildSeg(t, 0, map[int64][]string{5: {"x"}, 6: {"x"}})
	segB := buildSeg(t, 0, map[int64][]string{5: {"x"}})
	defer segA.Free()
	defer segB.Free()

	fa, err := FrameSegment(segA)
	if err != nil {
		t.Fatal(err)
	}
	fb, err := FrameSegment(segB)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := FrameDeletes(testPkType, []DeleteRecord{{Pk: int64(6)}})
	if err != nil {
		t.Fatal(err)
	}

	// Frames in chunk_id order: segA@1, segB@2, delete(6)@3.
	frames := []TailFrame{
		{ChunkId: 1, Data: fa},
		{ChunkId: 2, Data: fb},
		{ChunkId: 3, Data: fd},
	}
	segs, deletes, err := AssembleFrames(frames)
	if err != nil {
		t.Fatal(err)
	}
	defer freeSegs(segs)

	if len(segs) != 2 {
		t.Fatalf("want 2 assembled segments, got %d", len(segs))
	}
	// chunk_id is assigned from the frame position, not persisted in the blob.
	if segs[0].ChunkId != 1 || segs[1].ChunkId != 2 {
		t.Fatalf("segment chunk_ids not set from frames: %d, %d", segs[0].ChunkId, segs[1].ChunkId)
	}
	if deletes[normalizeKey(int64(6))] != 3 {
		t.Fatalf("delete fold wrong: want {6:3}, got %v", deletes)
	}

	// Liveness: pk 5 is owned by segB (chunk 2); pk 6 lives only in segA
	// (chunk 1) but is deleted at chunk 3 (> 1) → dead. Only pk 5 survives.
	live := ComputeLiveness(segs, deletes)
	got := pkCounts(SearchSegmentsLive(segs, []string{"x"}, 10, nil, live))
	if got[5] != 1 || len(got) != 1 {
		t.Fatalf("assembled-frame search: want {5:1}, got %v", got)
	}
}

// TestWandTailFramesBadDispatch guards the frame-kind dispatch: a corrupt frame
// is rejected (not silently mis-decoded as the wrong kind).
func TestWandTailFramesBadDispatch(t *testing.T) {
	fd, err := FrameDeletes(testPkType, []DeleteRecord{{Pk: int64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	fd[len(fd)-20] ^= 0xff // corrupt inside the framed payload/crc region
	if _, _, err := AssembleFrames([]TailFrame{{ChunkId: 1, Data: fd}}); err == nil {
		t.Fatal("expected AssembleFrames to reject a corrupt frame")
	}
}

// TestWandSearchSegsLive exercises the multi-segment load-adapter search core
// (searchSegsLive): a tag=0 base segment (recency below the tail) plus a tag=1
// delta that updates one doc and adds another, with a delete — asserting
// liveness (base < tail so tail wins) and that a per-segment WHERE prefilter is
// applied against each segment's own pks (no cross-segment ord confusion).
func TestWandSearchSegsLive(t *testing.T) {
	base := buildSeg(t, baseChunkId, map[int64][]string{1: {"x"}, 2: {"x"}, 3: {"x"}})
	tail := buildSeg(t, 2, map[int64][]string{2: {"x"}, 4: {"x"}}) // pk 2 updated, 4 new
	defer base.Free()
	defer tail.Free()
	segs := []*WandModel{base, tail}
	deletes := map[any]int64{normalizeKey(int64(3)): 3} // delete pk 3 at chunk 3 (> base -1)

	wantSet := func(got, want map[int64]int) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("want %v, got %v", want, got)
		}
		for pk, n := range want {
			if got[pk] != n {
				t.Fatalf("pk %d: want %d, got %d (full %v)", pk, n, got[pk], got)
			}
		}
	}

	// No filter: pk 2 owned by tail (chunk 2 > base -1); pk 3 deleted → {1,2,4}.
	wantSet(pkCounts(searchSegsLive(segs, deletes, []string{"x"}, 10, nil)), map[int64]int{1: 1, 2: 1, 4: 1})

	// Per-segment prefilter allowing only {1,4} → {1,4}, evaluated on each
	// segment's own ord→pk map.
	allow := map[int64]bool{1: true, 4: true}
	mkAllow := func(m *WandModel) Membership { return &ordMembership{m: m, allowPk: allow} }
	wantSet(pkCounts(searchSegsLive(segs, deletes, []string{"x"}, 10, mkAllow)), map[int64]int{1: 1, 4: 1})
}
