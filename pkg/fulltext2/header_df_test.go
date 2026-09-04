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

func TestLookupLoadedDFRequiresValidatedDirectory(t *testing.T) {
	seg := loadedHeaderOnlySegment(t, 7)

	// A readable DF header is insufficient: the normal decoder rejects this
	// intentionally incomplete directory, so the fast path must reject it too.
	_, ok := seg.LookupLoaded("term")
	require.False(t, ok)
	_, ok = seg.lookupLoadedDF("term")
	require.False(t, ok)

	idx := &Index{segments: []*Segment{seg}, liveOrd: [][]bool{nil}}
	gs := &globalStats{idx: idx, dfCache: make(map[string]int)}
	require.Zero(t, gs.df("term"))

	_, ok = seg.lookupLoadedDF("missing")
	require.False(t, ok)

	// A real serialized segment follows the same header path. This also guards
	// the FST offset convention used by Serialize/Deserialize.
	orig := buildSegment(int32(types.T_int64), []any{int64(1), int64(2)}, []int32{1, 1}, map[string]*termPostings{
		"term": {docIDs: []int64{0, 1}, tfs: []uint8{1, 1}, positions: [][]int32{{0}, {0}}},
	})
	loaded := roundtrip(t, orig)
	require.True(t, loaded.headerDFSafe)
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

func TestCorruptLoadedDirectoryDoesNotContributeGlobalDF(t *testing.T) {
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
	require.False(t, bad.headerDFSafe)

	off, ok, err := bad.dict.get("term")
	require.NoError(t, err)
	require.True(t, ok)
	df, ok := bad.termDFAt(off)
	require.True(t, ok, "the counterexample keeps a readable DF header")
	require.Equal(t, 7, df)
	_, ok = bad.LookupLoaded("term")
	require.False(t, ok, "the reference decoder rejects the damaged directory")
	_, ok = bad.lookupLoadedDF("term")
	require.False(t, ok, "an unvalidated segment must not use the header fast path")

	emptyBuild := buildSegment(int32(types.T_int64),
		[]any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7)},
		[]int32{1, 1, 1, 1, 1, 1, 1}, nil)
	empty := roundtrip(t, emptyBuild)
	healthy := roundtrip(t, buildSegment(int32(types.T_int64), []any{int64(100)}, []int32{1}, map[string]*termPostings{
		"term": {docIDs: []int64{0}, tfs: []uint8{1}, positions: [][]int32{{0}}},
	}))

	gotIdx := NewIndex([]*Segment{bad, healthy}, nil)
	wantIdx := NewIndex([]*Segment{empty, healthy}, nil)
	for _, algo := range []ScoreAlgo{BM25, TfIdf} {
		got, err := gotIdx.SearchQuery([]byte("+term"), true, ParserDefault, algo, 10, nil)
		require.NoError(t, err)
		want, err := wantIdx.SearchQuery([]byte("+term"), true, ParserDefault, algo, 10, nil)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Len(t, want, 1)
		require.Equal(t, want[0].Pk, got[0].Pk)
		require.Equal(t, math.Float32bits(want[0].Score), math.Float32bits(got[0].Score))
	}
}

func TestCorruptEntryFallsBackForHealthyTermsInSameSegment(t *testing.T) {
	build := buildSegment(int32(types.T_int64), []any{int64(1), int64(2)}, []int32{1, 1}, map[string]*termPostings{
		"bad":  {docIDs: []int64{0}, tfs: []uint8{1}, positions: [][]int32{{0}}},
		"good": {docIDs: []int64{1}, tfs: []uint8{1}, positions: [][]int32{{0}}},
	})
	data, err := build.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("mixed", bytes.NewReader(corruptSerializedTermDirectory(t, data, "bad")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })
	require.False(t, loaded.headerDFSafe)

	// Segment-wide fallback is conservative: the damaged term remains a miss,
	// while unrelated healthy terms still use the reference decoder and search.
	_, ok := loaded.lookupLoadedDF("good")
	require.False(t, ok)
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
			seg := &Segment{dict: dict, ranking: tc.ranking, blocks: tc.blocks, headerDFSafe: true}
			_, ok := seg.lookupLoadedDF("term")
			require.Equal(t, tc.wantOK, ok)
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

func benchmarkLoadedTermDFSegment(b *testing.B, df int) *Segment {
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
	return &Segment{dict: dict, ranking: ranking, blocks: blocks, headerDFSafe: true}
}

func BenchmarkValidateLoadedPostingsDirectory(b *testing.B) {
	for _, df := range []int{256, 10_000, 50_000, 200_000} {
		b.Run(fmt.Sprintf("df=%d", df), func(b *testing.B) {
			seg := benchmarkLoadedTermDFSegment(b, df)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !seg.validateLoadedPostingsDirectory() {
					b.Fatal("valid posting directory rejected")
				}
			}
		})
	}
}

func BenchmarkLoadedTermDFHeader(b *testing.B) {
	for _, df := range []int{256, 10_000, 50_000, 200_000} {
		for _, segments := range []int{1, 4, 10} {
			name := fmt.Sprintf("df=%d/segments=%d", df, segments)
			b.Run(name, func(b *testing.B) {
				segs := make([]*Segment, segments)
				for i := range segs {
					segs[i] = benchmarkLoadedTermDFSegment(b, df)
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

func benchmarkLoadedVocabularySegment(b *testing.B, n int) *Segment {
	b.Helper()
	terms := make([]string, n)
	values := make([]uint64, n)
	ranking := make([]byte, 1+8, 1+8+n*12)
	ranking[0] = postingsFormatV1
	binary.LittleEndian.PutUint64(ranking[1:], uint64(n))
	blocks := make([]byte, n*2)
	for i := 0; i < n; i++ {
		terms[i] = fmt.Sprintf("term-%06d", i)
		values[i] = uint64(len(ranking))
		ranking = binary.AppendUvarint(ranking, 1)           // df
		ranking = binary.AppendUvarint(ranking, 1)           // nblk
		ranking = binary.AppendUvarint(ranking, uint64(2*i)) // blockDataBase
		ranking = binary.AppendUvarint(ranking, 0)           // posRawBase
		ranking = append(ranking, 1)                         // termMaxTf
		ranking = binary.AppendUvarint(ranking, 1)           // minDocLen
		ranking = binary.AppendUvarint(ranking, 0)           // lastDocGap
		ranking = append(ranking, 1)                         // blockMaxTf
		ranking = binary.AppendUvarint(ranking, 1)           // blockMinDocLen
		ranking = binary.AppendUvarint(ranking, 2)           // block bytes
		ranking = binary.AppendUvarint(ranking, 0)           // no positions
	}
	fstBytes, err := buildTermDictFST(terms, values)
	if err != nil {
		b.Fatal(err)
	}
	dict, err := loadTermDict(fstBytes)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = dict.Close() })
	return &Segment{dict: dict, ranking: ranking, blocks: blocks}
}

func BenchmarkValidateLoadedPostingsDirectoryVocabulary(b *testing.B) {
	for _, terms := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("terms=%d", terms), func(b *testing.B) {
			seg := benchmarkLoadedVocabularySegment(b, terms)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !seg.validateLoadedPostingsDirectory() {
					b.Fatal("valid posting directory rejected")
				}
			}
		})
	}
}

func BenchmarkLoadedTermDFHeaderAtOffset(b *testing.B) {
	for _, df := range []int{256, 10_000, 50_000, 200_000} {
		name := fmt.Sprintf("df=%d", df)
		b.Run(name, func(b *testing.B) {
			seg := benchmarkLoadedTermDFSegment(b, df)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkDFSink, _ = seg.termDFAt(0)
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
					segs[i] = benchmarkLoadedTermDFSegment(b, df)
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
