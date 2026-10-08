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
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var prefixPostingBenchSink int

func prefixPostingRepresentations(t *testing.T, parser string) []*Segment {
	t.Helper()
	built, err := BuildSegmentFromDocsParser("prefix-parity", int32(types.T_int64), []Doc{
		{int64(1), []byte("alpha 中国人民 alpha 中国群众")},
		{int64(2), []byte("alpha 中国香蕉")},
		{int64(3), []byte("beta 中国人民")},
		{int64(4), []byte("alpha 中华人民")},
	}, parser)
	require.NoError(t, err)
	t.Cleanup(built.Free)
	blob, err := built.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("prefix-heap", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(loaded.Free)
	f, err := os.Create(filepath.Join(t.TempDir(), "segment.ft2"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	_, err = f.Write(blob)
	require.NoError(t, err)
	data, err := mmapReadOnly(f)
	require.NoError(t, err)
	mapped := &Segment{Id: "prefix-mmap", mmapData: data, mmapPath: f.Name()}
	t.Cleanup(mapped.Free)
	require.NoError(t, mapped.decodeSegment(data))
	require.NoError(t, f.Close())
	return []*Segment{built, loaded, mapped}
}

// The independent legacy expansion checks MAX, weighting and unscored NOT
// presence without using the new iterator to construct expected score bits.
func TestPrefixPostingScoreBitsAndRepresentations(t *testing.T) {
	for _, parser := range []string{ParserNgram, ParserGojieba} {
		t.Run(parser, func(t *testing.T) {
			for _, seg := range prefixPostingRepresentations(t, parser) {
				idx := NewIndex([]*Segment{seg}, nil)
				for _, prefix := range []string{"中", "中国", "alp", "absent"} {
					terms, err := seg.prefixTerms(prefix)
					require.NoError(t, err)
					for _, algo := range []ScoreAlgo{BM25, TfIdf} {
						for _, scored := range []bool{false, true} {
							for _, weight := range []float32{1, 0.5, 2} {
								gs := idx.newGlobalStats()
								want := make(map[int64]float32)
								for _, term := range terms {
									pl, ok := seg.lookup(term)
									if !ok {
										continue
									}
									docs := pl.materializeDocIDs()
									tfs := pl.materializeTfs()
									for i, ord := range docs {
										sc := float32(0)
										if scored {
											sc = seg.scoreTerm(algo, float64(tfs[i]), gs.idfFor(seg, term, pl), ord, gs.avgdl(seg))
										}
										if cur, seen := want[ord]; !seen || sc > cur {
											want[ord] = sc
										}
									}
								}
								for ord := range want {
									want[ord] *= weight
								}
								got, err := seg.evalClause(clause{kind: clausePrefix, terms: []string{prefix}, weight: weight}, algo, gs.avgdl(seg), idx.newGlobalStats(), scored)
								require.NoError(t, err)
								require.Equal(t, len(want), len(got))
								for ord, sc := range want {
									require.Contains(t, got, ord)
									require.Equal(t, math.Float32bits(sc), math.Float32bits(got[ord]), "parser=%s segment=%s prefix=%s", parser, seg.Id, prefix)
								}
							}
						}
					}
				}
				for _, query := range []string{"alpha 中国", "中国 alpha", "alpha absent"} {
					slots, err := phraseSlots(query, parser)
					require.NoError(t, err)
					require.Equal(t, seg.matchPhraseFallback(slots), seg.matchPhrase(slots))
				}
			}
		})
	}
}

func TestPrefixPostingConcurrentReaders(t *testing.T) {
	segments := prefixPostingRepresentations(t, ParserNgram)
	for _, seg := range segments {
		idx := NewIndex([]*Segment{seg}, nil)
		want, err := idx.SearchQuery([]byte("alpha 中国"), false, ParserNgram, BM25, 100, nil)
		require.NoError(t, err)
		var workers sync.WaitGroup
		for worker := 0; worker < 10; worker++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for i := 0; i < 10; i++ {
					got, err := idx.SearchQuery([]byte("alpha 中国"), false, ParserNgram, BM25, 100, nil)
					if !assert.NoError(t, err) {
						return
					}
					assert.Equal(t, resultScoreBits(want), resultScoreBits(got))
				}
			}()
		}
		workers.Wait()
	}
}

// The FST iterator's value is the posting-directory offset. Check its direct
// decode against the old exact lookup on both build and loaded representations,
// including a prefix that expands to several different CJK trigrams.
func TestPrefixPostingIteratorValueMatchesExactLookup(t *testing.T) {
	docs := []Doc{
		{int64(1), []byte("alpha 中国人民")},
		{int64(2), []byte("alpha 中国香蕉")},
		{int64(3), []byte("alpha 中华人民")},
		{int64(4), []byte("beta 中国人民")},
	}
	built, err := BuildSegmentFromDocsParser("prefix", int32(types.T_int64), docs, ParserNgram)
	require.NoError(t, err)
	data, err := built.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("prefix", bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, loaded.dict.Close()) })

	for _, seg := range []*Segment{built, loaded} {
		for _, prefix := range []string{"", "中", "中国", "alpha", "absent"} {
			terms, err := seg.prefixTerms(prefix)
			require.NoError(t, err)
			var visited []string
			err = seg.forEachPrefixPosting(prefix, func(term string, pl *termPostings) {
				visited = append(visited, term)
				want, ok := seg.lookup(term)
				require.True(t, ok)
				require.Equal(t, want.df(), pl.df())
				require.Equal(t, want.materializeDocIDs(), pl.materializeDocIDs())
				require.Equal(t, want.materializePositions(), pl.materializePositions())
			})
			require.NoError(t, err)
			require.True(t, slices.Equal(terms, visited), "prefix %q: got %v, want %v", prefix, visited, terms)
		}
	}

	// This is a non-empty exact+star phrase: the Chinese two-character slot
	// expands by prefix, while the Latin slot and byte offsets constrain it.
	for _, seg := range []*Segment{built, loaded} {
		idx := NewIndex([]*Segment{seg}, nil)
		require.ElementsMatch(t, []any{int64(1), int64(2)}, nlIDs(t, idx, ParserNgram, "alpha 中国"))
		require.ElementsMatch(t, []any{int64(1), int64(2), int64(3), int64(4)}, boolIDs(t, idx, ParserNgram, "中*"))
	}
}

func TestPrefixPostingIteratorRejectsInvalidDirectoryOffset(t *testing.T) {
	data, err := buildTermDictFST([]string{"bad"}, []uint64{math.MaxUint64})
	require.NoError(t, err)
	dict, err := loadTermDict(data)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dict.Close()) })
	seg := &Segment{dict: dict, ranking: []byte{1}}
	visited := false
	require.NoError(t, seg.forEachPrefixPosting("ba", func(_ string, _ *termPostings) {
		visited = true
	}))
	require.False(t, visited)
	_, ok := seg.LookupLoaded("bad")
	require.False(t, ok)
}

func TestPrefixPostingLoadedMixedPhraseRespectsLiveness(t *testing.T) {
	load := func(id string, recency int64, docs []Doc) *Segment {
		built, err := BuildSegmentFromDocsParser(id, int32(types.T_int64), docs, ParserNgram)
		require.NoError(t, err)
		data, err := built.Serialize()
		require.NoError(t, err)
		seg, err := Deserialize(id, bytes.NewReader(data))
		require.NoError(t, err)
		seg.Recency = recency
		t.Cleanup(func() { require.NoError(t, seg.dict.Close()) })
		return seg
	}

	base := load("prefix-base", 0, []Doc{
		{int64(1), []byte("alpha 中国人民")},
		{int64(2), []byte("alpha 中国香蕉")},
		{int64(3), []byte("beta 中国人民")},
	})
	tail := load("prefix-tail", 10, []Doc{
		{int64(1), []byte("beta 中国人民")}, // Updated: old phrase is dead.
		{int64(4), []byte("alpha 中国人民")},
	})
	deleted := foldDeleteFrame(nil, []DeleteRecord{{Pk: int64(2)}}, 20)
	idx := NewIndex([]*Segment{base, tail}, deleted)
	require.ElementsMatch(t, []any{int64(4)}, nlIDs(t, idx, ParserNgram, "alpha 中国"))
}

func TestPrefixPostingMixedPhraseTFMatchesLiteralStarts(t *testing.T) {
	docs := []Doc{
		{int64(1), []byte("alpha 中国人民 alpha 中国群众")},
		{int64(2), []byte("alpha 中国人民")},
		{int64(3), []byte("alpha 世界人民")},
		{int64(4), []byte("甲乙丙 alpha 中国人民")},
	}
	built, err := BuildSegmentFromDocsParser("prefix-tf", int32(types.T_int64), docs, ParserNgram)
	require.NoError(t, err)
	data, err := built.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("prefix-tf", bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, loaded.dict.Close()) })

	for _, seg := range []*Segment{built, loaded} {
		for _, query := range []string{"alpha 中国", "alpha 世界", "alpha 甲乙"} {
			slots, err := phraseSlots(query, ParserNgram)
			require.NoError(t, err)
			got := make(map[int64]int)
			for _, hit := range seg.matchPhrase(slots) {
				got[hit.ord] = hit.tf
			}
			want := make(map[int64]int)
			for ord, doc := range docs {
				for start := range doc.Text {
					if bytes.HasPrefix(doc.Text[start:], []byte(query)) {
						want[int64(ord)]++
					}
				}
				if want[int64(ord)] == 0 {
					delete(want, int64(ord))
				}
			}
			require.Equal(t, want, got, "query %q", query)
		}
	}
}

// BenchmarkPrefixPostingIteration isolates the removed second FST lookup. It
// does not measure whole-query QPS or replace the real-corpus pack comparison.
func BenchmarkPrefixPostingIteration(b *testing.B) {
	docs := make([]Doc, 2000)
	for i := range docs {
		docs[i] = Doc{Pk: int64(i), Text: []byte("中国" + string(rune(0x4e00+i%200)))}
	}
	built, err := BuildSegmentFromDocsParser("prefix-bench", int32(types.T_int64), docs, ParserNgram)
	if err != nil {
		b.Fatal(err)
	}
	data, err := built.Serialize()
	if err != nil {
		b.Fatal(err)
	}
	seg, err := Deserialize("prefix-bench", bytes.NewReader(data))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = seg.dict.Close() })

	b.Run("baseline", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			terms, err := seg.prefixTerms("中国")
			if err != nil {
				b.Fatal(err)
			}
			n := 0
			for _, term := range terms {
				if pl, ok := seg.lookup(term); ok {
					n += pl.df()
				}
			}
			prefixPostingBenchSink = n
		}
	})
	b.Run("candidate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n := 0
			err := seg.forEachPrefixPosting("中国", func(_ string, pl *termPostings) {
				n += pl.df()
			})
			if err != nil {
				b.Fatal(err)
			}
			prefixPostingBenchSink = n
		}
	})
}
