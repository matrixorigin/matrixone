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
	"encoding/binary"
	"fmt"
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

	df, ok := seg.lookupLoadedDF("term")
	require.True(t, ok)
	require.Equal(t, 7, df)

	// The synthetic ranking contains no nblk/base-offset/max-tf fields, so the
	// normal full decoder must fail. This proves the clean global-df path did not
	// silently allocate and parse a complete term directory.
	_, ok = seg.LookupLoaded("term")
	require.False(t, ok)

	idx := &Index{segments: []*Segment{seg}, liveOrd: [][]bool{nil}}
	gs := &globalStats{idx: idx, dfCache: make(map[string]int)}
	require.Equal(t, 7, gs.df("term"))
	require.Equal(t, 7, gs.df("term"), "query-local df cache should retain the header result")

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
			seg := &Segment{dict: dict, ranking: tc.ranking, blocks: tc.blocks}
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
	fstBytes, err := buildTermDictFST([]string{"term"}, []uint64{0})
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
