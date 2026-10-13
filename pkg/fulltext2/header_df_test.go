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
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func loadedHeaderOnlySegment(t *testing.T, df int) *Segment {
	t.Helper()
	fstBytes, err := buildTermDictFST([]string{"term"}, []uint64{0})
	require.NoError(t, err)
	dict, err := loadTermDict(fstBytes)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dict.Close() })

	// The header-only path needs only the first df varint. Keep the block bytes
	// large enough to pass the defensive df bound; the directory is intentionally
	// absent so a test failure that calls decodeTermEntry cannot be mistaken for a
	// successful full decode.
	ranking := binary.AppendUvarint(nil, uint64(df))
	return &Segment{dict: dict, ranking: ranking, blocks: make([]byte, df)}
}

func TestLookupLoadedDFReadsOnlyHeader(t *testing.T) {
	seg := loadedHeaderOnlySegment(t, 7)

	// Deliberately absent directory proves that clean DF does not invoke the full
	// decoder. Corrupt-directory score equivalence is not part of this fast path.
	_, ok := seg.LookupLoaded("term")
	require.False(t, ok)
	df, ok := seg.lookupLoadedDF("term")
	require.True(t, ok)
	require.Equal(t, 7, df)

	idx := &Index{segments: []*Segment{seg}, liveOrd: [][]bool{nil}}
	gs := &globalStats{idx: idx, dfCache: make(map[string]int)}
	require.Equal(t, 7, gs.df("term"))

	_, ok = seg.lookupLoadedDF("missing")
	require.False(t, ok)

	// A real serialized segment follows the same header path. This also guards
	// the FST offset convention used by Serialize/Deserialize.
	orig := buildSegment(int32(types.T_int64), []any{int64(1), int64(2)}, []int32{1, 1}, map[string]*termPostings{
		"term": {docIDs: []int64{0, 1}, tfs: []uint8{1, 1}, positions: [][]int32{{0}, {0}}},
	})
	loaded := roundtrip(t, orig)
	idx = NewIndex([]*Segment{loaded}, nil)
	gs = idx.newGlobalStats()
	require.Equal(t, 2, gs.df("term"))
}

func corruptSerializedTermDirectory(t *testing.T, data []byte, term string) []byte {
	t.Helper()
	corrupt := append([]byte(nil), data...)
	_, fst, ranking, blocks, _, err := sliceMembers(corrupt)
	require.NoError(t, err)
	dict, err := loadTermDict(fst)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dict.Close() })
	off, ok, err := dict.get(term)
	require.NoError(t, err)
	require.True(t, ok)
	require.Less(t, off, uint64(len(ranking)))
	_, n := binary.Uvarint(ranking[int(off):])
	require.Positive(t, n)
	require.Less(t, len(blocks)+1, 1<<7, "fixture requires a one-byte impossible nblk")
	ranking[int(off)+n] = byte(len(blocks) + 1)
	return corrupt
}

func TestCorruptLoadedDirectoryWithReadableHeader(t *testing.T) {
	badBuild := buildSegment(int32(types.T_int64),
		[]any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7)},
		[]int32{1, 1, 1, 1, 1, 1, 1},
		map[string]*termPostings{
			"term": {
				docIDs:    []int64{0, 1, 2, 3, 4, 5, 6},
				tfs:       []uint8{1, 1, 1, 1, 1, 1, 1},
				positions: [][]int32{{0}, {0}, {0}, {0}, {0}, {0}, {0}},
			},
		})
	badData, err := badBuild.Serialize()
	require.NoError(t, err)
	badData = corruptSerializedTermDirectory(t, badData, "term")
	bad, err := Deserialize("bad", bytes.NewReader(badData))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bad.dict.Close() })

	off, ok, err := bad.dict.get("term")
	require.NoError(t, err)
	require.True(t, ok)
	df, ok := bad.termDFAt(off)
	require.True(t, ok, "the counterexample keeps a readable DF header")
	require.Equal(t, 7, df)
	_, ok = bad.LookupLoaded("term")
	require.False(t, ok, "the reference decoder rejects the damaged directory")
	df, ok = bad.lookupLoadedDF("term")
	require.True(t, ok)
	require.Equal(t, 7, df)
	healthy := roundtrip(t, buildSegment(int32(types.T_int64), []any{int64(100)}, []int32{1}, map[string]*termPostings{
		"term": {docIDs: []int64{0}, tfs: []uint8{1}, positions: [][]int32{{0}}},
	}))

	gotIdx := NewIndex([]*Segment{bad, healthy}, nil)
	// A damaged term contributes its header DF but never supplies a posting. We
	// do not assert score/ranking equivalence with the old full-decoder DF path.
	require.Equal(t, 8, gotIdx.newGlobalStats().df("term"))
	for _, algo := range []ScoreAlgo{BM25, TfIdf} {
		got, err := gotIdx.SearchQuery([]byte("+term"), true, ParserDefault, algo, 10, nil)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, int64(100), got[0].Pk)
		require.False(t, math.IsNaN(float64(got[0].Score)))
		require.False(t, math.IsInf(float64(got[0].Score), 0))
	}
}

func TestCorruptEntryDoesNotDisableHealthyTerms(t *testing.T) {
	build := buildSegment(int32(types.T_int64), []any{int64(1), int64(2)}, []int32{1, 1}, map[string]*termPostings{
		"bad":  {docIDs: []int64{0}, tfs: []uint8{1}, positions: [][]int32{{0}}},
		"good": {docIDs: []int64{1}, tfs: []uint8{1}, positions: [][]int32{{0}}},
	})
	data, err := build.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("mixed", bytes.NewReader(corruptSerializedTermDirectory(t, data, "bad")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	// An unrelated damaged directory does not disable the healthy term's header.
	df, ok := loaded.lookupLoadedDF("good")
	require.True(t, ok)
	require.Equal(t, 1, df)
	idx := NewIndex([]*Segment{loaded}, nil)
	require.Equal(t, 1, idx.newGlobalStats().df("good"))
	got, err := idx.SearchQuery([]byte("+good"), true, ParserDefault, BM25, 10, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, int64(2), got[0].Pk)
}

func TestLookupLoadedDFGuardsMalformedHeader(t *testing.T) {
	cases := []struct {
		name    string
		ranking []byte
		blocks  []byte
		value   uint64
		wantOK  bool
	}{
		{name: "offset-out-of-range", ranking: []byte{1}, blocks: []byte{1}, value: 1},
		{name: "offset-max-uint64", ranking: []byte{1}, blocks: []byte{1}, value: math.MaxUint64},
		{name: "truncated-varint", ranking: []byte{0x80}, blocks: []byte{1}},
		{name: "varint-overflow", ranking: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, blocks: make([]byte, 8), value: 0},
		{name: "df-larger-than-blocks", ranking: []byte{9}, blocks: make([]byte, 8), value: 0},
		{name: "zero-df", ranking: []byte{0}, blocks: []byte{1}, value: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fstBytes, err := buildTermDictFST([]string{"term"}, []uint64{tc.value})
			require.NoError(t, err)
			dict, err := loadTermDict(fstBytes)
			require.NoError(t, err)
			t.Cleanup(func() { _ = dict.Close() })
			seg := &Segment{dict: dict, ranking: tc.ranking, blocks: tc.blocks}
			_, ok := seg.lookupLoadedDF("term")
			require.Equal(t, tc.wantOK, ok)
			require.NotPanics(t, func() {
				_, ok := seg.LookupLoaded("term")
				require.False(t, ok)
			})
		})
	}
}

func TestGlobalStatsDFLoadedDirtyStillUsesLivePostings(t *testing.T) {
	orig := buildSegment(int32(types.T_int64), []any{int64(1), int64(2), int64(3)}, []int32{1, 1, 1}, map[string]*termPostings{
		"term": {docIDs: []int64{0, 1, 2}, tfs: []uint8{1, 1, 1}, positions: [][]int32{{0}, {0}, {0}}},
	})
	loaded := roundtrip(t, orig)
	idx := &Index{segments: []*Segment{loaded}, liveOrd: [][]bool{{true, false, true}}}
	gs := &globalStats{idx: idx, dfCache: make(map[string]int)}
	require.Equal(t, 2, gs.df("term"))
}

func TestDecodeTermEntryUnsignedBounds(t *testing.T) {
	for _, tc := range []struct {
		name         string
		baseB, baseP uint64
		blockLens    []uint64
		posLens      []uint64
		wantOK       bool
	}{
		{name: "huge-block-base", baseB: 1 << 63, blockLens: []uint64{1}, posLens: []uint64{0}},
		{name: "huge-position-base", baseP: 1 << 63, blockLens: []uint64{1}, posLens: []uint64{1}},
		{name: "max-block-base", baseB: math.MaxUint64, blockLens: []uint64{1}, posLens: []uint64{0}},
		{name: "max-position-base", baseP: math.MaxUint64, blockLens: []uint64{1}, posLens: []uint64{1}},
		{name: "cumulative-block-length", blockLens: []uint64{2, 1}, posLens: []uint64{0, 0}},
		{name: "cumulative-position-length", blockLens: []uint64{1, 1}, posLens: []uint64{2, 1}},
		{name: "overflow-block-length", blockLens: []uint64{1, math.MaxUint64}, posLens: []uint64{0, 0}},
		{name: "overflow-position-length", blockLens: []uint64{1, 1}, posLens: []uint64{1, math.MaxUint64}},
		{name: "base-plus-block-length", baseB: 2, blockLens: []uint64{1}, posLens: []uint64{0}},
		{name: "base-plus-position-length", baseP: 2, blockLens: []uint64{1}, posLens: []uint64{1}},
		{name: "empty-at-end", baseB: 2, baseP: 2, blockLens: []uint64{0}, posLens: []uint64{0}, wantOK: true},
		{name: "last-byte", baseB: 1, baseP: 1, blockLens: []uint64{1}, posLens: []uint64{1}, wantOK: true},
		{name: "position-free", blockLens: []uint64{2}, posLens: []uint64{0}, wantOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A minimal directory with tiny backing sections isolates arithmetic
			// boundaries without allocating memory based on the malformed values.
			r := binary.AppendUvarint(nil, uint64(len(tc.blockLens)))
			r = binary.AppendUvarint(r, uint64(len(tc.blockLens)))
			r = binary.AppendUvarint(r, tc.baseB)
			r = binary.AppendUvarint(r, tc.baseP)
			r = append(r, 1, 1) // max TF, min doc length
			for i, n := range tc.blockLens {
				r = append(r, 1, 1, 1) // last doc gap, max TF, min doc length
				r = binary.AppendUvarint(r, n)
				r = binary.AppendUvarint(r, tc.posLens[i])
			}
			seg := &Segment{ranking: r, blocks: []byte{0, 1}, positions: []byte{0, 1}}
			if tc.name == "position-free" {
				seg.positions = nil
			}
			require.NotPanics(t, func() {
				p, ok := seg.decodeTermEntry(0)
				require.Equal(t, tc.wantOK, ok)
				if !ok {
					require.Nil(t, p)
					return
				}
				var nb, np uint64
				for i, n := range tc.blockLens {
					nb += n
					np += tc.posLens[i]
				}
				require.Equal(t, seg.blocks[tc.baseB:tc.baseB+nb], p.blockData)
				require.Equal(t, seg.positions[tc.baseP:tc.baseP+np], p.posRaw)
			})
		})
	}
}

func TestHeaderDFValidIndexScoreParity(t *testing.T) {
	for _, count := range []int{1, 4, 10} {
		for _, dirty := range []bool{false, true} {
			t.Run(fmt.Sprintf("segments=%d/dirty=%t", count, dirty), func(t *testing.T) {
				var builds, loaded []*Segment
				for si := 0; si < count; si++ {
					b := NewBuilder(fmt.Sprintf("seg-%d", si), int32(types.T_int64))
					for i := 0; i < BlockSize+3; i++ {
						terms := []string{"alpha", "beta"}
						if i%3 == 0 {
							terms = append(terms, "beta", "gamma")
						}
						if i%5 == 0 {
							terms = []string{"alpine", "delta"}
						}
						pk := int64(si*1000 + i)
						if dirty && si > 0 && i == 1 {
							pk = 1 // supersede the older copy, independently of deletes
							terms = []string{"gamma"}
						}
						feed(t, b, pk, terms...)
					}
					s, err := b.Finish()
					require.NoError(t, err)
					s.Recency = int64(si)
					builds = append(builds, s)
					l := roundtrip(t, s)
					l.Recency = s.Recency
					loaded = append(loaded, l)
				}
				deletes := map[any]int64{}
				if dirty {
					deletes[int64(2)] = int64(count)
				}
				wantIdx, gotIdx := NewIndex(builds, deletes), NewIndex(loaded, deletes)
				gs := gotIdx.newGlobalStats()
				for _, term := range []string{"alpha", "alpine", "beta", "gamma", "delta", "missing"} {
					want := 0
					for si, s := range loaded {
						if p, ok := s.LookupLoaded(term); ok {
							want += materializedLiveDF(p, gotIdx.liveOrd[si])
						}
					}
					require.Equal(t, want, gs.df(term), term)
				}
				for _, q := range []struct {
					text    string
					boolean bool
				}{
					{"+alpha +beta", true}, {"+alpha +alpha", true}, {"+alpha +missing", true},
					{"alpha gamma", true}, {"+alpha -gamma", true}, {"+(alpha delta) beta", true},
					{"al*", true}, {"+\"alpha beta\"", true}, {"alpha beta", false},
				} {
					for _, algo := range []ScoreAlgo{BM25, TfIdf} {
						for _, limit := range []int{0, 1, 1000, int(wantIdx.globalN) + 1} {
							want, err := wantIdx.SearchQuery([]byte(q.text), q.boolean, ParserDefault, algo, limit, nil)
							require.NoError(t, err)
							got, err := gotIdx.SearchQuery([]byte(q.text), q.boolean, ParserDefault, algo, limit, nil)
							require.NoError(t, err)
							if limit == 0 || len(want) < limit {
								require.Equal(t, resultScoreBits(want), resultScoreBits(got), "query=%s algo=%d limit=%d", q.text, algo, limit)
								continue
							}
							// Equal-score segment merging may choose different PKs at a
							// tied cutoff. Check every returned PK against the unbounded
							// oracle, and require the same complete Top-K score sequence.
							full, err := wantIdx.SearchQuery([]byte(q.text), q.boolean, ParserDefault, algo, int(wantIdx.globalN)+1, nil)
							require.NoError(t, err)
							bits := resultScoreBits(full)
							require.Len(t, got, len(want))
							for _, r := range got {
								v, exists := bits[r.Pk]
								require.True(t, exists)
								require.Equal(t, v, math.Float32bits(r.Score))
							}
							sort.Slice(full, func(i, j int) bool { return full[i].Score > full[j].Score })
							sort.Slice(got, func(i, j int) bool { return got[i].Score > got[j].Score })
							for i := range got {
								require.Equal(t, math.Float32bits(full[i].Score), math.Float32bits(got[i].Score))
							}
						}
					}
				}
			})
		}
	}
}

func benchmarkLoadedTermDFSegment(b *testing.B, df int) (*Segment, uint64) {
	b.Helper()
	nblk := (df + BlockSize - 1) / BlockSize
	ranking := make([]byte, 0, 16+nblk*8)
	ranking = append(ranking, postingsFormatV1)
	var nterms [8]byte
	binary.LittleEndian.PutUint64(nterms[:], 1)
	ranking = append(ranking, nterms[:]...)
	entryOff := uint64(len(ranking))
	ranking = binary.AppendUvarint(ranking, uint64(df))
	ranking = binary.AppendUvarint(ranking, uint64(nblk))
	ranking = binary.AppendUvarint(ranking, 0) // blockDataBase
	ranking = binary.AppendUvarint(ranking, 0) // posRawBase
	ranking = append(ranking, 1)               // termMaxTf
	ranking = binary.AppendUvarint(ranking, 1) // minDocLen
	blocks := make([]byte, 0, 2*df)
	for block := 0; block < nblk; block++ {
		blen := df - block*BlockSize
		if blen > BlockSize {
			blen = BlockSize
		}
		// One-byte doc gaps plus one byte of tf per posting keep the synthetic
		// block valid for the full decoder while making setup deterministic.
		blkLen := 2 * blen
		ranking = binary.AppendUvarint(ranking, uint64(blen)) // lastDocGap
		ranking = append(ranking, 1)                          // blockMaxTf
		ranking = binary.AppendUvarint(ranking, 1)            // blockMinDocLen
		ranking = binary.AppendUvarint(ranking, uint64(blkLen))
		ranking = binary.AppendUvarint(ranking, 0) // no positions in this benchmark
		blocks = append(blocks, make([]byte, blkLen)...)
	}
	fstBytes, err := buildTermDictFST([]string{"term"}, []uint64{entryOff})
	if err != nil {
		b.Fatal(err)
	}
	dict, err := loadTermDict(fstBytes)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = dict.Close() })
	seg := &Segment{dict: dict, ranking: ranking, blocks: blocks}
	got, ok := seg.termDFAt(entryOff)
	if !ok || got != df {
		b.Fatalf("header at offset %d: got (%d, %t), want %d", entryOff, got, ok, df)
	}
	tp, ok := seg.LookupLoaded("term")
	if !ok || tp.df() != df {
		b.Fatal("benchmark directory does not match header DF")
	}
	return seg, entryOff
}

func BenchmarkLoadedTermDFHeader(b *testing.B) {
	for _, df := range []int{256, 10_000, 50_000, 200_000} {
		for _, segments := range []int{1, 4, 10} {
			name := fmt.Sprintf("df=%d/segments=%d", df, segments)
			b.Run(name, func(b *testing.B) {
				segs := make([]*Segment, segments)
				for i := range segs {
					segs[i], _ = benchmarkLoadedTermDFSegment(b, df)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, seg := range segs {
						benchmarkDFSink, _ = seg.lookupLoadedDF("term")
					}
				}
			})
		}
	}
}

func BenchmarkLoadedTermDFHeaderAtOffset(b *testing.B) {
	for _, df := range []int{256, 10_000, 50_000, 200_000} {
		name := fmt.Sprintf("df=%d", df)
		b.Run(name, func(b *testing.B) {
			seg, entryOff := benchmarkLoadedTermDFSegment(b, df)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkDFSink, _ = seg.termDFAt(entryOff)
			}
		})
	}
}

func BenchmarkLoadedTermDFDecode(b *testing.B) {
	for _, df := range []int{256, 10_000, 50_000, 200_000} {
		for _, segments := range []int{1, 4, 10} {
			name := fmt.Sprintf("df=%d/segments=%d", df, segments)
			b.Run(name, func(b *testing.B) {
				segs := make([]*Segment, segments)
				for i := range segs {
					segs[i], _ = benchmarkLoadedTermDFSegment(b, df)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, seg := range segs {
						if p, ok := seg.LookupLoaded("term"); ok {
							benchmarkDFSink = p.df()
						}
					}
				}
			})
		}
	}
}

var benchmarkDFSink int
