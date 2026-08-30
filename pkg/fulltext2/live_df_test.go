// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/stretchr/testify/require"
)

func makeLiveDFPostings(n int, loaded bool) *termPostings {
	docs := make([]int64, n)
	tfs := make([]uint8, n)
	docLen := make([]int32, n)
	for i := range docs {
		docs[i] = int64(i)
		tfs[i] = 1
		docLen[i] = 1
	}
	build := &termPostings{docIDs: docs, tfs: tfs}
	deriveTermStats(build, docLen)
	if !loaded {
		return build
	}

	p := &termPostings{
		ndoc:          n,
		maxTf:         build.maxTf,
		minDocLen:     build.minDocLen,
		blockLastDoc:  append([]int64(nil), build.blockLastDoc...),
		blockMaxTf:    append([]uint8(nil), build.blockMaxTf...),
		blockMinDocLn: append([]int32(nil), build.blockMinDocLn...),
		blockOff:      make([]int64, 0, build.nblk()+1),
	}
	p.blockOff = append(p.blockOff, 0)
	var data []byte
	var prev int64
	for b := 0; b < build.nblk(); b++ {
		lo := b * BlockSize
		hi := lo + build.blockLen(b)
		for _, doc := range docs[lo:hi] {
			data = binary.AppendUvarint(data, uint64(doc-prev))
			prev = doc
		}
		data = append(data, tfs[lo:hi]...)
		p.blockOff = append(p.blockOff, int64(len(data)))
	}
	p.blockData = data
	return p
}

func materializedLiveDF(p *termPostings, live []bool) int {
	df := 0
	for _, ord := range p.materializeDocIDs() {
		if live == nil || live[ord] {
			df++
		}
	}
	return df
}

func TestLiveTermDFParity(t *testing.T) {
	for _, loaded := range []bool{false, true} {
		name := "build"
		if loaded {
			name = "loaded"
		}
		t.Run(name, func(t *testing.T) {
			p := makeLiveDFPostings(2*BlockSize+17, loaded)
			for _, tc := range []struct {
				name string
				live []bool
			}{
				{name: "clean"},
				{name: "dirty", live: func() []bool {
					live := make([]bool, p.df())
					for i := range live {
						live[i] = i%3 != 0
					}
					return live
				}()},
				{name: "all-dead", live: make([]bool, p.df())},
			} {
				t.Run(tc.name, func(t *testing.T) {
					idx := &Index{liveOrd: [][]bool{tc.live}}
					require.Equal(t, materializedLiveDF(p, tc.live), idx.liveTermDF(0, p))
				})
			}
		})
	}
}

func TestLiveTermDFFullyLiveDoesNotDecode(t *testing.T) {
	// No block directory or bytes: the only valid operation is the fully-live raw-df path.
	p := &termPostings{ndoc: 2*BlockSize + 3}
	idx := &Index{liveOrd: [][]bool{nil}}
	require.Equal(t, p.ndoc, idx.liveTermDF(0, p))
}

func TestLiveTermDFTruncatedBlockDoesNotPanic(t *testing.T) {
	p := &termPostings{
		ndoc:         3,
		blockData:    []byte{1}, // one valid doc gap; the remaining two are truncated
		blockOff:     []int64{0, 1},
		blockLastDoc: []int64{3},
	}
	idx := &Index{liveOrd: [][]bool{{false, true, true}}}
	require.Equal(t, 1, idx.liveTermDF(0, p))

	var docs [BlockSize]int64
	var tfs [BlockSize]uint8
	require.Equal(t, 1, p.fillBlock(0, docs[:], tfs[:]), "partial doc decode must preserve fillBlock's safe fallback")
}

var liveDFSink int

func TestLiveTermDFLoadedDirtyDoesNotAllocateByDF(t *testing.T) {
	p := makeLiveDFPostings(32*BlockSize+1, true)
	live := make([]bool, p.df())
	for i := range live {
		live[i] = i%5 != 0
	}
	idx := &Index{liveOrd: [][]bool{live}}
	want := materializedLiveDF(p, live)
	require.Equal(t, want, idx.liveTermDF(0, p))

	allocs := testing.AllocsPerRun(100, func() {
		liveDFSink = idx.liveTermDF(0, p)
	})
	require.Zero(t, allocs)
	require.Equal(t, want, liveDFSink)
}

func BenchmarkLiveTermDF(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 50_000, 200_000} {
		for _, loaded := range []bool{false, true} {
			kind := "build"
			if loaded {
				kind = "loaded"
			}
			p := makeLiveDFPostings(n, loaded)
			for _, dirty := range []bool{false, true} {
				state := "clean"
				var live []bool
				if dirty {
					state = "dirty"
					live = make([]bool, n)
					for i := range live {
						live[i] = i%7 != 0
					}
				}
				idx := &Index{liveOrd: [][]bool{live}}
				for _, terms := range []int{1, 2, 4, 8} {
					prefix := fmt.Sprintf("df=%d/%s/%s/terms=%d", n, kind, state, terms)
					b.Run(prefix+"/stream", func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							for term := 0; term < terms; term++ {
								liveDFSink = idx.liveTermDF(0, p)
							}
						}
					})
					b.Run(prefix+"/materialized-oracle", func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							for term := 0; term < terms; term++ {
								liveDFSink = materializedLiveDF(p, live)
							}
						}
					})
				}
			}
		}
	}
}
