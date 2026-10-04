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
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const cjkBenchTarget = "中华人民共和国"

func buildCJKPhraseBenchIndex(b *testing.B) *Index {
	b.Helper()
	rng := rand.New(rand.NewSource(42))
	alphabet := []rune("天地玄黄宇宙洪荒日月盈昃辰宿列张寒来暑往秋收冬藏")
	target := []rune(cjkBenchTarget)
	docs := make([]Doc, 5000)
	for i := range docs {
		text := make([]rune, 24)
		for j := range text {
			text[j] = alphabet[rng.Intn(len(alphabet))]
		}
		if i%10 == 0 {
			copy(text[8:8+len(target)], target)
		}
		docs[i] = Doc{Pk: int64(i), Text: []byte(string(text))}
	}

	seg, err := BuildSegmentFromDocsParser("cjk-phrase-bench", int32(types.T_int64), docs, ParserNgram)
	if err != nil {
		b.Fatal(err)
	}
	blob, err := seg.Serialize()
	if err != nil {
		b.Fatal(err)
	}
	loaded, err := Deserialize(seg.Id, bytes.NewReader(blob))
	if err != nil {
		b.Fatal(err)
	}
	idx := NewIndex([]*Segment{loaded}, nil)
	b.Cleanup(idx.Free)
	return idx
}

func BenchmarkConjunctiveCJKPhraseBoundary(b *testing.B) {
	idx := buildCJKPhraseBenchIndex(b)
	cases := []struct {
		name    string
		pattern string
	}{
		{name: "7-char", pattern: "中华人民共和国"},
		{name: "5-char", pattern: "中华人民共"},
		{name: "3-char", pattern: "共和国"},
	}
	modes := []struct {
		name    string
		boolean bool
		prefix  string
	}{
		{name: "natural"},
		{name: "boolean-must", boolean: true, prefix: "+"},
	}

	for _, tc := range cases {
		for _, mode := range modes {
			b.Run(tc.name+"/"+mode.name, func(b *testing.B) {
				pattern := []byte(mode.prefix + tc.pattern)
				results, err := idx.SearchQuery(pattern, mode.boolean, ParserNgram, BM25, 10, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(results) == 0 {
					b.Fatalf("setup query %q unexpectedly returned no results", pattern)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := idx.SearchQuery(pattern, mode.boolean, ParserNgram, BM25, 10, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
