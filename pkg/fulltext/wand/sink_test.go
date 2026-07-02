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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// TestWandCdcRoundTrip round-trips the binary CDC blob for both int64 and
// varchar pks — the varchar case is the reason the blob is binary (typed
// encodePk) and not JSON, which would corrupt a non-integer pk.
func TestWandCdcRoundTrip(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		c := NewWandCdc(testPkType)
		c.Insert(int64(1), "营养 早餐")
		c.Upsert(int64(2), "视频")
		c.Delete(int64(3))
		buf, err := c.Encode()
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeWandCdc(buf)
		if err != nil {
			t.Fatal(err)
		}
		if got.PkType != c.PkType || len(got.Events) != 3 {
			t.Fatalf("header/count wrong: %+v", got)
		}
		want := c.Events
		for i, e := range got.Events {
			if e.Op != want[i].Op || e.Pk.(int64) != want[i].Pk.(int64) || e.Text != want[i].Text {
				t.Fatalf("event %d mismatch: want %+v got %+v", i, want[i], e)
			}
		}
		// corruption is detected.
		buf[10] ^= 0xff
		if _, err := DecodeWandCdc(buf); err == nil {
			t.Fatal("expected checksum mismatch")
		}
	})

	t.Run("varchar", func(t *testing.T) {
		c := NewWandCdc(int32(types.T_varchar))
		c.Insert([]byte("doc-a"), "hello")
		c.Delete([]byte("doc-b"))
		buf, err := c.Encode()
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeWandCdc(buf)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Events) != 2 ||
			string(got.Events[0].Pk.([]byte)) != "doc-a" || got.Events[0].Text != "hello" ||
			string(got.Events[1].Pk.([]byte)) != "doc-b" {
			t.Fatalf("varchar round-trip wrong: %+v", got.Events)
		}
	})
}

// TestBuildTailFrames drives one CDC batch (2 inserts + a delete) into tag=1
// frames, checks the delete frame is emitted first (lower chunk_id), then
// assembles them alongside a base segment holding the deleted pk and asserts the
// search reflects the delete + the new docs.
func TestBuildTailFrames(t *testing.T) {
	tokenize := func(s string) []string { return strings.Fields(s) }
	c := NewWandCdc(testPkType)
	c.Insert(int64(1), "x y")
	c.Insert(int64(2), "x")
	c.Delete(int64(3))

	frames, next, err := BuildTailFrames(c, 1<<20, 5, tokenize)
	if err != nil {
		t.Fatal(err)
	}
	// delete frame @5 (first), one insert segment @6; next free chunk_id = 7.
	if next != 7 || len(frames) != 2 || frames[0].ChunkId != 5 || frames[1].ChunkId != 6 {
		t.Fatalf("frame layout wrong: next=%d frames=%d ids=%v", next, len(frames),
			[]int64{frames[0].ChunkId, frames[1].ChunkId})
	}

	// Assemble the tail alongside a base segment that holds pk 3 → the tail's
	// delete (chunk 5 > base -1) drops it; the new docs 1,2 are searchable.
	base := buildSeg(t, baseChunkId, map[int64][]string{3: {"x"}})
	defer base.Free()
	tailSegs, deletes, err := AssembleFrames(frames)
	if err != nil {
		t.Fatal(err)
	}
	defer freeSegs(tailSegs)
	segs := append([]*WandModel{base}, tailSegs...)

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
	// "x" matches docs 1,2 (new) but not deleted 3.
	wantSet(pkCounts(searchSegsLive(segs, deletes, []string{"x"}, 10, nil)), map[int64]int{1: 1, 2: 1})
	// "y" only tokenized into doc 1.
	wantSet(pkCounts(searchSegsLive(segs, deletes, []string{"y"}, 10, nil)), map[int64]int{1: 1})
}
