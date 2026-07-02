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

import (
	"os"
	"strings"
	"testing"
)

// TestTailBuilderStreamsCappedSegments drives docs through the streaming sinker
// builder across several batches and asserts it (1) seals capacity-capped segments
// spilled to files (peak memory = one open segment, the fix for buffering all
// events), (2) collects deletes, and (3) the spilled framed files reassemble —
// exactly as the load path does (split at MaxChunkSize + reassembleFrames +
// AssembleFrames) — into searchable segments covering every doc.
func TestTailBuilderStreamsCappedSegments(t *testing.T) {
	tokenize := func(s string) []string { return strings.Fields(s) }
	tb, err := NewTailBuilder(testPkType, 3, tokenize) // capacity 3 → multiple segments
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Cleanup()

	mk := func(pairs ...any) *WandCdc {
		c := NewWandCdc(testPkType)
		for i := 0; i < len(pairs); i += 2 {
			c.Insert(int64(pairs[i].(int)), pairs[i+1].(string))
		}
		return c
	}
	// 7 insert docs across 3 batches → ceil(7/3) = 3 sealed segments ([1,2,3],
	// [4,5,6], [7]); a batch also carries a delete of an unrelated pk.
	if err := tb.AddBatch(mk(1, "x y", 2, "x", 3, "y")); err != nil {
		t.Fatal(err)
	}
	if err := tb.AddBatch(mk(4, "x", 5, "z", 6, "x y z")); err != nil {
		t.Fatal(err)
	}
	last := mk(7, "x")
	last.Delete(int64(99))
	if err := tb.AddBatch(last); err != nil {
		t.Fatal(err)
	}

	segs, deletes, err := tb.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 3 {
		t.Fatalf("want 3 capacity-capped segments, got %d", len(segs))
	}
	if len(deletes) != 1 || normalizeKey(deletes[0].Pk) != normalizeKey(int64(99)) {
		t.Fatalf("deletes not collected: %v", deletes)
	}

	// Reassemble the spilled files exactly as loadTailFrames does.
	var chunks []TailChunk
	cid := int64(0)
	for _, seg := range segs {
		framed, e := os.ReadFile(seg.Path)
		if e != nil {
			t.Fatal(e)
		}
		if len(framed) != seg.FrameLen {
			t.Fatalf("FrameLen %d != spilled file size %d", seg.FrameLen, len(framed))
		}
		cs := splitFrameChunks(cid, framed)
		chunks = append(chunks, cs...)
		cid += int64(len(cs))
	}
	frames, err := reassembleFrames(chunks)
	if err != nil {
		t.Fatal(err)
	}
	models, delMap, err := AssembleFrames(frames)
	if err != nil {
		t.Fatal(err)
	}
	defer freeSegs(models)
	if len(models) != 3 {
		t.Fatalf("want 3 reassembled models, got %d", len(models))
	}
	for i, m := range models {
		if m.N > 3 {
			t.Fatalf("segment %d exceeds capacity: N=%d", i, m.N)
		}
	}

	live := ComputeLiveness(models, delMap)
	got := pkCounts(SearchSegmentsLive(models, []string{"x"}, 100, nil, live))
	want := map[int64]int{1: 1, 2: 1, 4: 1, 6: 1, 7: 1} // docs containing "x"
	if len(got) != len(want) {
		t.Fatalf("search x: want %v, got %v", want, got)
	}
	for pk := range want {
		if got[pk] != 1 {
			t.Fatalf("search x: pk %d missing (got %v)", pk, got)
		}
	}
}

// TestTailBuilderEmptyAndCleanup covers the corner cases: an all-delete stream
// yields no segments (but collects deletes), a segment whose rows have no
// searchable tokens is dropped (not spilled), and Cleanup removes the temp dir.
func TestTailBuilderEmptyAndCleanup(t *testing.T) {
	tokenize := func(s string) []string { return strings.Fields(s) }
	tb, err := NewTailBuilder(testPkType, 100, tokenize)
	if err != nil {
		t.Fatal(err)
	}

	c := NewWandCdc(testPkType)
	c.Insert(int64(1), "") // no tokens → contributes no doc
	c.Delete(int64(7))
	if err := tb.AddBatch(c); err != nil {
		t.Fatal(err)
	}
	segs, deletes, err := tb.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 0 {
		t.Fatalf("empty-token stream should spill no segments, got %d", len(segs))
	}
	if len(deletes) != 1 {
		t.Fatalf("want 1 delete, got %d", len(deletes))
	}

	dir := tb.dir
	if _, e := os.Stat(dir); e != nil {
		t.Fatalf("temp dir should exist before Cleanup: %v", e)
	}
	tb.Cleanup()
	if _, e := os.Stat(dir); !os.IsNotExist(e) {
		t.Fatalf("Cleanup should remove temp dir, stat err=%v", e)
	}
}
