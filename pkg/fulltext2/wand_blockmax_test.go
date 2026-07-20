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

package fulltext2

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// blockMaxCorpus builds a MULTI-BLOCK segment (many BlockSize=128 blocks per
// frequent term) with a Zipfian-ish vocabulary and varied doc lengths, so the
// Block-Max skip path actually fires. Deterministic (fixed seed) — no test flake.
func blockMaxCorpus(t *testing.T, nDocs int) []TokenizedDoc {
	t.Helper()
	rng := rand.New(rand.NewSource(0x5eed))
	// 40 words; earlier words are far more frequent (Zipf), giving long postings
	// that span many blocks with skewed per-block max-tf — the shape Block-Max
	// prunes best.
	vocab := make([]string, 40)
	for i := range vocab {
		vocab[i] = fmt.Sprintf("w%02d", i)
	}
	pick := func() string {
		// Zipf-like: index ~ floor(40 * u^3) biases hard toward small indices.
		u := rng.Float64()
		idx := int(float64(len(vocab)) * u * u * u)
		if idx >= len(vocab) {
			idx = len(vocab) - 1
		}
		return vocab[idx]
	}
	docs := make([]TokenizedDoc, nDocs)
	for i := 0; i < nDocs; i++ {
		n := 3 + rng.Intn(18) // doc length 3..20
		terms := make([]string, n)
		for j := 0; j < n; j++ {
			terms[j] = pick()
		}
		docs[i] = TokenizedDoc{Pk: int64(i), Terms: terms}
	}
	return docs
}

// requireSameTopK asserts the Block-Max walk returns the SAME top-k as the full
// scan: identical count, identical score multiset (score-desc, within float tol),
// and — for every doc scoring strictly ABOVE the k-th (boundary) score — an
// identical pk set. The ORDER of exactly-tied docs is NOT asserted: WAND sums a
// doc's term contributions in cursor order and the full scan in clause-map order,
// so tied scores can differ in the last ULP and sort differently, and at the k-th
// boundary a tie is broken arbitrarily (as in bm25). Those are the only permitted
// differences; a dropped/extra above-boundary doc or a wrong score fails here.
func requireSameTopK(t *testing.T, got, want []Result, ctx string) {
	t.Helper()
	require.Equalf(t, len(want), len(got), "%s: result count", ctx)
	if len(want) == 0 {
		return
	}
	// Score multiset: both are score-desc, so compare pairwise within tolerance.
	for i := range want {
		require.InDeltaf(t, want[i].Score, got[i].Score, 1e-5, "%s: score at rank %d", ctx, i)
	}
	// Set of docs strictly above the boundary score must match exactly (only the
	// boundary tie may differ in membership).
	const tol = 1e-5
	boundary := want[len(want)-1].Score
	above := func(rs []Result) map[any]struct{} {
		m := make(map[any]struct{})
		for _, r := range rs {
			if r.Score > boundary+tol {
				m[r.Pk] = struct{}{}
			}
		}
		return m
	}
	require.Equalf(t, above(want), above(got), "%s: above-boundary pk set", ctx)
}

// TestBlockMaxWANDParity is the correctness gate for the Block-Max WAND walk: over
// a large multi-block corpus it asserts searchWAND (block-max top-k) returns the
// IDENTICAL ranking as searchBooleanFull (the exhaustive scan) for many k values,
// both scorers, both build-side and serialized→loaded segments, and both bare and
// boosted (> / <) disjunctions. A block-skip that ever drops a real top-k doc
// would diverge here.
func TestBlockMaxWANDParity(t *testing.T) {
	const nDocs = 3000
	docs := blockMaxCorpus(t, nDocs)

	build, err := BuildSegmentFromTokenized("bm", int32(types.T_int64), docs)
	require.NoError(t, err)
	require.Greater(t, int(build.N), BlockSize*4, "corpus must span many blocks")

	// The loaded variant exercises deriveTermStats on the Deserialize path.
	blob, err := build.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("bm", bytes.NewReader(blob))
	require.NoError(t, err)

	// Sanity: a frequent term's postings really do span several blocks (else the
	// skip path is untested). w00 is the most frequent under the Zipf pick.
	if pl, ok := build.lookup("w00"); ok {
		require.Greater(t, len(pl.blockLastDoc), 3, "frequent term should have >3 blocks")
		require.Equal(t, len(pl.blockMaxTf), len(pl.blockLastDoc))
	} else {
		t.Fatal("expected w00 present")
	}

	// Disjunctive queries: mixes of frequent + rare terms, 2..5 terms, some boosted.
	mk := func(weights ...struct {
		w string
		x float64
	}) BoolQuery {
		var q BoolQuery
		for _, e := range weights {
			q.should = append(q.should, clause{kind: clauseTerm, terms: []string{e.w}, weight: float32(e.x)})
		}
		return q
	}
	type wq = struct {
		w string
		x float64
	}
	queries := []BoolQuery{
		mk(wq{"w00", 1}, wq{"w01", 1}),
		mk(wq{"w00", 1}, wq{"w05", 1}, wq{"w20", 1}),
		mk(wq{"w02", 1}, wq{"w03", 1}, wq{"w30", 1}, wq{"w39", 1}),
		mk(wq{"w00", 1.1}, wq{"w10", 0.9}, wq{"w15", 1}), // boosted / demoted
		mk(wq{"w01", 1}, wq{"w02", 1}, wq{"w04", 1}, wq{"w08", 1}, wq{"w16", 1}),
		mk(wq{"w38", 1}, wq{"w39", 1}), // both rare
	}
	ks := []int{1, 5, 20, 100, 500, nDocs}
	algos := []ScoreAlgo{BM25, TfIdf}

	for _, seg := range []*Segment{build, loaded} {
		tag := "build"
		if seg == loaded {
			tag = "loaded"
		}
		for _, algo := range algos {
			for qi, q := range queries {
				terms, ok := disjunctiveTerms(q)
				require.True(t, ok, "query %d must be WAND-eligible", qi)
				for _, k := range ks {
					want, err := seg.searchBooleanFull(q, algo, k, nil, nil)
					require.NoError(t, err)
					got := seg.searchWAND(terms, algo, k, nil, nil)
					requireSameTopK(t, got, want,
						fmt.Sprintf("%s algo=%v q=%d k=%d", tag, algo, qi, k))
				}
			}
		}
	}
}
