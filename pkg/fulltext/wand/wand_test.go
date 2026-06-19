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
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const testPkType = int32(types.T_int64)

// corpus mirrors what was fed to the Builder, for the brute-force reference.
type corpus struct {
	docTf  map[string]map[int64]int // term -> pk -> total tf (uncapped)
	docLen map[int64]int            // pk -> document length (token count)
	pks    map[int64]bool
}

func cappedTf(raw int) float64 {
	if raw > MaxCappedTf {
		raw = MaxCappedTf
	}
	return float64(raw)
}

func (c *corpus) avgDocLen() float64 {
	if len(c.pks) == 0 {
		return 0
	}
	var sum int
	for _, dl := range c.docLen {
		sum += dl
	}
	return float64(sum) / float64(len(c.pks))
}

// bruteForce computes the exact BM25 top-K: score(d) = Σ w_t·idf(t)²·factor,
// idf = log10(N/df), factor = bm25Factor(cappedTf, dl, avgdl) — MatrixOne's
// default BM25, matching the WAND walk.
func bruteForce(c *corpus, terms []string, limit int, allowPk map[int64]bool) []SearchResult {
	n := len(c.pks)
	avgdl := c.avgDocLen()
	weights := map[string]float64{}
	for _, t := range terms {
		weights[t]++
	}
	scores := map[int64]float64{}
	for t, w := range weights {
		dm, ok := c.docTf[t]
		if !ok {
			continue
		}
		df := len(dm)
		idf := math.Log10(float64(n) / float64(df))
		idfSq := idf * idf
		for d, raw := range dm {
			if allowPk != nil && !allowPk[d] {
				continue
			}
			scores[d] += w * idfSq * bm25Factor(cappedTf(raw), int32(c.docLen[d]), avgdl)
		}
	}
	out := make([]SearchResult, 0, len(scores))
	for d, s := range scores {
		out = append(out, SearchResult{DocID: d, Score: s})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].DocID.(int64) < out[j].DocID.(int64)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func recompute(c *corpus, terms []string, pk int64) float64 {
	n := len(c.pks)
	avgdl := c.avgDocLen()
	weights := map[string]float64{}
	for _, t := range terms {
		weights[t]++
	}
	s := 0.0
	for t, w := range weights {
		dm, ok := c.docTf[t]
		if !ok {
			continue
		}
		if raw, ok := dm[pk]; ok {
			df := len(dm)
			idf := math.Log10(float64(n) / float64(df))
			s += w * idf * idf * bm25Factor(cappedTf(raw), int32(c.docLen[pk]), avgdl)
		}
	}
	return s
}

// buildModelAndCorpus generates a random corpus, feeds it to the Builder one
// occurrence at a time (Add per token), and returns the model + reference.
func buildModelAndCorpus(t *testing.T, rng *rand.Rand, nDocs, nTerms, maxPostings int) (*WandModel, *corpus) {
	t.Helper()
	terms := make([]string, nTerms)
	for i := range terms {
		terms[i] = fmt.Sprintf("term%04d", i) // out-of-dict → exercises overflow ids
	}
	c := &corpus{docTf: map[string]map[int64]int{}, docLen: map[int64]int{}, pks: map[int64]bool{}}
	b := NewBuilder("test", testPkType)
	for _, term := range terms {
		k := 1 + rng.Intn(maxPostings)
		for j := 0; j < k; j++ {
			d := int64(rng.Intn(nDocs))
			tf := 1 + rng.Intn(6)
			for o := 0; o < tf; o++ {
				if err := b.Add(term, d); err != nil {
					t.Fatalf("Add: %v", err)
				}
			}
			if c.docTf[term] == nil {
				c.docTf[term] = map[int64]int{}
			}
			c.docTf[term][d] += tf
			c.docLen[d] += tf // every token contributes to doc length
			c.pks[d] = true
		}
	}
	return b.Finish(), c
}

func assertTopKEqual(t *testing.T, label string, got, want []SearchResult, c *corpus, terms []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: result count got %d want %d", label, len(got), len(want))
	}
	for i := range got {
		if math.Abs(got[i].Score-want[i].Score) > 1e-9 {
			t.Fatalf("%s: rank %d score got %.12g want %.12g", label, i, got[i].Score, want[i].Score)
		}
	}
	seen := map[int64]bool{}
	for _, r := range got {
		pk := r.DocID.(int64)
		if seen[pk] {
			t.Fatalf("%s: duplicate doc-id %d", label, pk)
		}
		seen[pk] = true
		if exp := recompute(c, terms, pk); math.Abs(exp-r.Score) > 1e-9 {
			t.Fatalf("%s: doc %d score got %.12g recomputed %.12g", label, pk, r.Score, exp)
		}
	}
}

func TestWandDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(20260619))
	allTerms := make([]string, 40)
	for i := range allTerms {
		allTerms[i] = fmt.Sprintf("term%04d", i)
	}
	for iter := 0; iter < 40; iter++ {
		m, c := buildModelAndCorpus(t, rng, 300+rng.Intn(400), 40, 100)
		nq := 1 + rng.Intn(6)
		q := make([]string, nq)
		for i := range q {
			if rng.Intn(10) == 0 {
				q[i] = "absent_term"
			} else {
				q[i] = allTerms[rng.Intn(len(allTerms))]
			}
		}
		limit := 1 + rng.Intn(20)
		got := m.Search(q, limit, nil)
		want := bruteForce(c, q, limit, nil)
		assertTopKEqual(t, fmt.Sprintf("iter=%d q=%v lim=%d", iter, q, limit), got, want, c, q)
	}
}

// TestWandSegments verifies that splitting a corpus into capacity-bounded
// segments and merging with corpus-global stats yields the SAME top-K as the
// single-index brute force.
func TestWandSegments(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	allTerms := make([]string, 30)
	for i := range allTerms {
		allTerms[i] = fmt.Sprintf("term%04d", i)
	}
	for iter := 0; iter < 20; iter++ {
		nDocs := 300 + rng.Intn(400)
		c := &corpus{docTf: map[string]map[int64]int{}, docLen: map[int64]int{}, pks: map[int64]bool{}}
		b := NewBuilder("seg", testPkType)
		for _, term := range allTerms {
			k := 1 + rng.Intn(80)
			for j := 0; j < k; j++ {
				d := int64(rng.Intn(nDocs))
				tf := 1 + rng.Intn(6)
				for o := 0; o < tf; o++ {
					if err := b.Add(term, d); err != nil {
						t.Fatal(err)
					}
				}
				if c.docTf[term] == nil {
					c.docTf[term] = map[int64]int{}
				}
				c.docTf[term][d] += tf
				c.docLen[d] += tf
				c.pks[d] = true
			}
		}
		capacity := int64(50 + rng.Intn(200))
		segs := b.FinishSegments(capacity)

		nq := 1 + rng.Intn(5)
		q := make([]string, nq)
		for i := range q {
			q[i] = allTerms[rng.Intn(len(allTerms))]
		}
		limit := 1 + rng.Intn(15)
		got := SearchSegments(segs, q, limit, nil)
		want := bruteForce(c, q, limit, nil)
		assertTopKEqual(t, fmt.Sprintf("seg iter=%d nseg=%d cap=%d", iter, len(segs), capacity), got, want, c, q)
	}
}

// TestWandMerge verifies Merge of two independently-built, disjoint-doc indexes
// (with conflicting overflow word-ids) equals the brute force over the union.
func TestWandMerge(t *testing.T) {
	rng := rand.New(rand.NewSource(123))
	terms := make([]string, 30)
	for i := range terms {
		terms[i] = fmt.Sprintf("term%04d", i)
	}
	for iter := 0; iter < 20; iter++ {
		c := &corpus{docTf: map[string]map[int64]int{}, docLen: map[int64]int{}, pks: map[int64]bool{}}
		// order drives overflow-id assignment; reverse it for B so the two
		// builds assign different ids to the same words → exercises reconcile.
		fill := func(b *Builder, order []string, pkBase, n int) {
			for _, term := range order {
				k := 1 + rng.Intn(40)
				for j := 0; j < k; j++ {
					d := int64(pkBase + rng.Intn(n))
					tf := 1 + rng.Intn(5)
					for o := 0; o < tf; o++ {
						if err := b.Add(term, d); err != nil {
							t.Fatal(err)
						}
					}
					if c.docTf[term] == nil {
						c.docTf[term] = map[int64]int{}
					}
					c.docTf[term][d] += tf
					c.docLen[d] += tf
					c.pks[d] = true
				}
			}
		}
		nA, nB := 100+rng.Intn(200), 100+rng.Intn(200)
		rev := make([]string, len(terms))
		for i := range terms {
			rev[len(terms)-1-i] = terms[i]
		}
		ba, bb := NewBuilder("a", testPkType), NewBuilder("b", testPkType)
		fill(ba, terms, 0, nA)
		fill(bb, rev, nA, nB)
		merged := Merge("m", ba.Finish(), bb.Finish())

		nq := 1 + rng.Intn(5)
		q := make([]string, nq)
		for i := range q {
			q[i] = terms[rng.Intn(len(terms))]
		}
		limit := 1 + rng.Intn(15)
		got := merged.Search(q, limit, nil)
		want := bruteForce(c, q, limit, nil)
		assertTopKEqual(t, fmt.Sprintf("merge iter=%d N=%d", iter, merged.N), got, want, c, q)
	}
}

// buildSeg builds a one-flush delta segment with the given LSN (its batch-LSN
// identity). docs maps pk -> the terms occurring in that doc (one tf each).
func buildSeg(t *testing.T, lsn int64, docs map[int64][]string) *WandModel {
	b := NewBuilder(fmt.Sprintf("seg%d", lsn), testPkType)
	for pk, terms := range docs {
		for _, term := range terms {
			if err := b.Add(term, pk); err != nil {
				t.Fatal(err)
			}
		}
	}
	m := b.Finish()
	m.LSN = lsn
	return m
}

// pkCounts returns pk -> number of times it appears in the results (so a value
// > 1 flags a cross-segment duplicate).
func pkCounts(res []SearchResult) map[int64]int {
	out := map[int64]int{}
	for _, r := range res {
		out[r.DocID.(int64)]++
	}
	return out
}

// TestWandLiveness exercises the LSN-as-identity rule (ComputeLiveness +
// SearchSegmentsLive) that makes CDC delete-then-reinsert / UPDATE correct over
// immutable segments. Assertions are on the LIVE pk SET (dedup / delete /
// reinsert), not exact scores: global N/df/avgdl intentionally still include
// superseded+deleted docs until compaction (accepted stat drift), so scores
// drift but membership must be exact.
func TestWandLiveness(t *testing.T) {
	q := []string{"x"}

	// 1. UPDATE = same pk in two segments → newest-LSN wins, exactly one row.
	t.Run("dedup_update", func(t *testing.T) {
		segs := []*WandModel{
			buildSeg(t, 1, map[int64][]string{5: {"x"}, 6: {"x"}}),
			buildSeg(t, 2, map[int64][]string{5: {"x"}}), // pk 5 updated
		}
		live := ComputeLiveness(segs, nil)
		got := pkCounts(SearchSegmentsLive(segs, q, 10, nil, live))
		if got[5] != 1 || got[6] != 1 || len(got) != 2 {
			t.Fatalf("dedup: want {5:1,6:1}, got %v", got)
		}
		// Without liveness the stale copy leaks → pk 5 appears twice (this is
		// exactly why the search adapter must use ComputeLiveness).
		dup := pkCounts(SearchSegments(segs, q, 10, nil))
		if dup[5] != 2 {
			t.Fatalf("expected the no-liveness path to duplicate pk 5, got %v", dup)
		}
	})

	// 2. DELETE then reINSERT at a higher LSN → live again.
	t.Run("delete_then_reinsert", func(t *testing.T) {
		segs := []*WandModel{
			buildSeg(t, 1, map[int64][]string{5: {"x"}}),
			buildSeg(t, 3, map[int64][]string{5: {"x"}}), // reinsert at LSN 3
		}
		deletes := map[any]int64{normalizeKey(int64(5)): 2} // delete at LSN 2 < 3
		got := pkCounts(SearchSegmentsLive(segs, q, 10, nil, ComputeLiveness(segs, deletes)))
		if got[5] != 1 || len(got) != 1 {
			t.Fatalf("delete-then-reinsert: want {5:1}, got %v", got)
		}
	})

	// 3. DELETE after the latest insert → gone.
	t.Run("delete_after_insert", func(t *testing.T) {
		segs := []*WandModel{
			buildSeg(t, 1, map[int64][]string{5: {"x"}}),
			buildSeg(t, 3, map[int64][]string{5: {"x"}}),
		}
		deletes := map[any]int64{normalizeKey(int64(5)): 4} // delete at LSN 4 > 3
		got := pkCounts(SearchSegmentsLive(segs, q, 10, nil, ComputeLiveness(segs, deletes)))
		if len(got) != 0 {
			t.Fatalf("delete-after-insert: want empty, got %v", got)
		}
	})

	// 4. Pure DELETE of one pk among several.
	t.Run("pure_delete", func(t *testing.T) {
		segs := []*WandModel{buildSeg(t, 1, map[int64][]string{5: {"x"}, 6: {"x"}, 7: {"x"}})}
		deletes := map[any]int64{normalizeKey(int64(6)): 2}
		got := pkCounts(SearchSegmentsLive(segs, q, 10, nil, ComputeLiveness(segs, deletes)))
		if got[5] != 1 || got[7] != 1 || got[6] != 0 || len(got) != 2 {
			t.Fatalf("pure-delete: want {5,7}, got %v", got)
		}
	})

	// 5. Mixed: insert base, update one + insert one in a delta, delete one.
	t.Run("mixed", func(t *testing.T) {
		segs := []*WandModel{
			buildSeg(t, 1, map[int64][]string{1: {"x"}, 2: {"x"}, 3: {"x"}}),
			buildSeg(t, 2, map[int64][]string{2: {"x"}, 4: {"x"}}), // 2 updated, 4 new
		}
		deletes := map[any]int64{normalizeKey(int64(3)): 2} // delete 3
		got := pkCounts(SearchSegmentsLive(segs, q, 10, nil, ComputeLiveness(segs, deletes)))
		want := map[int64]int{1: 1, 2: 1, 4: 1}
		if len(got) != len(want) {
			t.Fatalf("mixed: want %v, got %v", want, got)
		}
		for pk, n := range want {
			if got[pk] != n {
				t.Fatalf("mixed: pk %d want %d, got %d (full %v)", pk, n, got[pk], got)
			}
		}
	})
}

// TestWandToInsertSqlsTag checks the tag column threads through: tag=0 for the
// compacted main index, tag=1 for a CDC delta segment.
func TestWandToInsertSqlsTag(t *testing.T) {
	b := NewBuilder("seg-1", testPkType)
	for _, term := range []string{"a", "b", "营养"} {
		if err := b.Add(term, int64(1)); err != nil {
			t.Fatal(err)
		}
	}
	m := b.Finish()
	cfg := TableConfig{DbName: "db", IndexTable: "ft_index", MetadataTable: "ft_meta"}

	for _, tag := range []int{0, 1} {
		sqls, err := m.ToInsertSqls(cfg, 123, tag)
		if err != nil {
			t.Fatal(err)
		}
		// the chunk INSERT(s) must carry the requested tag, and the wrong tag
		// must not appear.
		want := fmt.Sprintf(", %d)", tag)
		bad := fmt.Sprintf(", %d)", 1-tag)
		found := false
		for _, s := range sqls {
			if bytes.Contains([]byte(s), []byte("ft_index")) {
				if !bytes.Contains([]byte(s), []byte(want)) {
					t.Fatalf("tag=%d: chunk insert missing %q: %s", tag, want, s)
				}
				if bytes.Contains([]byte(s), []byte(bad)) {
					t.Fatalf("tag=%d: chunk insert has wrong tag %q: %s", tag, bad, s)
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("tag=%d: no ft_index chunk insert generated", tag)
		}
	}
}

// TestWandDeleteLogRoundTrip checks the tag=1 delete-log codec round-trips for
// int64 and varchar PKs, validates CRC, and folds to the max-LSN map.
func TestWandDeleteLogRoundTrip(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		recs := []DeleteRecord{{Pk: int64(5), LSN: 2}, {Pk: int64(9), LSN: 7}, {Pk: int64(5), LSN: 4}}
		buf, err := EncodeDeleteLog(int32(types.T_int64), recs)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeDeleteLog(buf)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(recs) {
			t.Fatalf("want %d recs, got %d", len(recs), len(got))
		}
		for i := range recs {
			if got[i].Pk.(int64) != recs[i].Pk.(int64) || got[i].LSN != recs[i].LSN {
				t.Fatalf("rec %d mismatch: want %v got %v", i, recs[i], got[i])
			}
		}
		// max-LSN fold: pk 5 deleted at 2 then 4 → 4 wins.
		m := DeleteMap(got)
		if m[normalizeKey(int64(5))] != 4 || m[normalizeKey(int64(9))] != 7 {
			t.Fatalf("DeleteMap fold wrong: %v", m)
		}
	})

	t.Run("varchar", func(t *testing.T) {
		recs := []DeleteRecord{{Pk: []byte("doc-a"), LSN: 1}, {Pk: []byte("doc-b"), LSN: 3}}
		buf, err := EncodeDeleteLog(int32(types.T_varchar), recs)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeDeleteLog(buf)
		if err != nil {
			t.Fatal(err)
		}
		m := DeleteMap(got)
		if m[normalizeKey([]byte("doc-a"))] != 1 || m[normalizeKey([]byte("doc-b"))] != 3 {
			t.Fatalf("varchar DeleteMap wrong: %v", m)
		}
	})

	t.Run("corruption_detected", func(t *testing.T) {
		buf, err := EncodeDeleteLog(int32(types.T_int64), []DeleteRecord{{Pk: int64(1), LSN: 1}})
		if err != nil {
			t.Fatal(err)
		}
		buf[8] ^= 0xff // flip a byte in the body
		if _, err := DecodeDeleteLog(buf); err == nil {
			t.Fatal("expected checksum mismatch error")
		}
	})

	t.Run("empty", func(t *testing.T) {
		buf, err := EncodeDeleteLog(int32(types.T_int64), nil)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeDeleteLog(buf)
		if err != nil || len(got) != 0 {
			t.Fatalf("empty round-trip: got %v err %v", got, err)
		}
		if DeleteMap(got) != nil {
			t.Fatal("empty DeleteMap should be nil")
		}
	})
}

// ordMembership filters by pk, evaluated on doc ords via the model's pk map.
type ordMembership struct {
	m       *WandModel
	allowPk map[int64]bool
}

func (o ordMembership) Contains(ord int64) bool {
	pk, _ := o.m.PkAt(ord).(int64)
	return o.allowPk[pk]
}

func TestWandPrefilter(t *testing.T) {
	rng := rand.New(rand.NewSource(0xBEEF))
	allTerms := make([]string, 30)
	for i := range allTerms {
		allTerms[i] = fmt.Sprintf("term%04d", i)
	}
	for iter := 0; iter < 25; iter++ {
		nDocs := 300 + rng.Intn(300)
		m, c := buildModelAndCorpus(t, rng, nDocs, 30, 80)
		allowPk := map[int64]bool{}
		for d := range c.pks {
			if rng.Intn(10) < 3 {
				allowPk[d] = true
			}
		}
		nq := 1 + rng.Intn(5)
		q := make([]string, nq)
		for i := range q {
			q[i] = allTerms[rng.Intn(len(allTerms))]
		}
		limit := 1 + rng.Intn(15)
		got := m.Search(q, limit, ordMembership{m, allowPk})
		want := bruteForce(c, q, limit, allowPk)
		for _, r := range got {
			if !allowPk[r.DocID.(int64)] {
				t.Fatalf("iter=%d prefilter returned disallowed doc %d", iter, r.DocID.(int64))
			}
		}
		assertTopKEqual(t, fmt.Sprintf("prefilter iter=%d", iter), got, want, c, q)
	}
}

func TestWandSerializeRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	m, c := buildModelAndCorpus(t, rng, 1000, 50, 400)

	buf, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	sum := Checksum(buf)

	m2, err := Deserialize("test", bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	if m2.N != m.N || m2.NumTerms() != m.NumTerms() || m2.PkType != m.PkType {
		t.Fatalf("mismatch N %d/%d terms %d/%d pkType %d/%d", m2.N, m.N, m2.NumTerms(), m.NumTerms(), m2.PkType, m.PkType)
	}
	if buf2, _ := m2.Serialize(); Checksum(buf2) != sum {
		t.Fatalf("checksum not stable across round-trip")
	}

	q := []string{"term0001", "term0002", "term0010", "term0025"}
	r1 := m.Search(q, 20, nil)
	r2 := m2.Search(q, 20, nil)
	assertTopKEqual(t, "reloaded", r2, bruteForce(c, q, 20, nil), c, q)
	if len(r1) != len(r2) {
		t.Fatalf("reloaded count differs %d/%d", len(r1), len(r2))
	}
}

func TestWandEdgeCases(t *testing.T) {
	b := NewBuilder("e", testPkType)
	// term "a": docs 1,3,5 ; term "b": docs 2,3 ; plus an in-dict Chinese term.
	add := func(w string, d int64, n int) {
		for i := 0; i < n; i++ {
			if err := b.Add(w, d); err != nil {
				t.Fatal(err)
			}
		}
	}
	add("a", 1, 2)
	add("a", 3, 1)
	add("a", 5, 9)
	add("b", 2, 1)
	add("b", 3, 4)
	add("营养", 5, 1) // dictionary word → global word-id
	m := b.Finish()

	if r := m.Search(nil, 5, nil); r != nil {
		t.Fatalf("empty query should be nil")
	}
	if r := m.Search([]string{"a"}, 0, nil); r != nil {
		t.Fatalf("limit 0 should be nil")
	}
	if r := m.Search([]string{"absent"}, 5, nil); len(r) != 0 {
		t.Fatalf("absent query should be empty")
	}
	if r := m.Search([]string{"营养"}, 5, nil); len(r) != 1 || r[0].DocID.(int64) != 5 {
		t.Fatalf("in-dict term search failed: %v", r)
	}

	// round-trip with a mix of overflow + dictionary terms must preserve results.
	buf, err := m.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	m2, err := Deserialize("e", bytes.NewReader(buf))
	if err != nil {
		t.Fatal(err)
	}
	r1 := m.Search([]string{"a", "b", "营养"}, 10, nil)
	r2 := m2.Search([]string{"a", "b", "营养"}, 10, nil)
	if len(r1) != len(r2) || len(r1) != 4 { // docs 1,2,3,5
		t.Fatalf("expected 4 docs both, got %d/%d", len(r1), len(r2))
	}

	// tf is capped at 255: doc 1 gets "z" 300 times → scored as tf 255.
	cb := NewBuilder("cap", testPkType)
	for i := 0; i < 300; i++ {
		_ = cb.Add("z", int64(1))
	}
	_ = cb.Add("z", int64(2))      // df(z)=2
	_ = cb.Add("filler", int64(3)) // N=3 > df → idf > 0
	cm := cb.Finish()
	avg := (300.0 + 1 + 1) / 3
	idf := math.Log10(3.0 / 2.0)
	wantTop := idf * idf * bm25Factor(255, 300, avg) // capped tf, dl=300
	res := cm.Search([]string{"z"}, 1, nil)
	if len(res) != 1 || res[0].DocID.(int64) != 1 || math.Abs(res[0].Score-wantTop) > 1e-9 {
		t.Fatalf("tf cap: got %v, want top doc 1 score %.12g", res, wantTop)
	}
}
