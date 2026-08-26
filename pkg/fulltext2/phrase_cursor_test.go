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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestPhraseCursorMatchesFallback proves the block-cursor phrase intersection
// (matchPhraseCursor) returns EXACTLY what the materialize fallback returns, across many
// blocks (> BlockSize docs, so the cursor crosses block boundaries, skips non-overlapping
// blocks, and decodes positions per block) — on both a build-side and a serialized/loaded
// segment. The fallback is the trusted oracle; any divergence is a cursor bug.
func TestPhraseCursorMatchesFallback(t *testing.T) {
	phrases := []string{
		"中文學習", "學習教材", "遠東兒童", "兒童中文", "教學指引",
		"中文學習教材", "生字卡片", "初學者適合", "短篇小說", "樂趣無窮",
	}
	// 500 docs (~4 blocks). Each doc concatenates a deterministic rotating subset of the
	// phrases (some repeated → tf>1), so phrases appear in scattered, block-spanning docs.
	docs := make([]Doc, 0, 500)
	for i := 0; i < 500; i++ {
		var b bytes.Buffer
		for k := 0; k < 3; k++ {
			b.WriteString(phrases[(i+k*3)%len(phrases)])
			b.WriteByte(' ')
		}
		if i%5 == 0 { // repeat one phrase → occurrence count > 1
			b.WriteString(phrases[i%len(phrases)])
		}
		docs = append(docs, Doc{int64(i + 1), b.Bytes()})
	}

	seg, err := BuildSegmentFromDocsParser("cur", int32(types.T_int64), docs, ParserNgram)
	require.NoError(t, err)
	blob, err := seg.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("cur", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	// Multi-trigram queries (each decomposes to >= 2 exact slots → the cursor path).
	queries := []string{
		"中文學習", "學習教材", "中文學習教材", "遠東兒童中文", "教學指引",
		"初學者適合", "短篇小說樂趣", "生字卡片", "兒童中文學習", "不存在的詞語",
	}
	key := func(h []docTf) []string {
		out := make([]string, len(h))
		for i, d := range h {
			out[i] = fmt.Sprintf("%d:%d", d.ord, d.tf)
		}
		return out
	}

	for _, name := range []string{"build", "loaded"} {
		sg := seg
		if name == "loaded" {
			sg = loaded
		}
		t.Run(name, func(t *testing.T) {
			for _, q := range queries {
				slots, err := phraseSlots(q, ParserNgram)
				require.NoError(t, err)
				if len(slots) < 2 {
					continue // cursor path is multi-slot only
				}
				cur := sg.matchPhraseCursor(slots)
				fb := sg.matchPhraseFallback(slots)
				require.ElementsMatch(t, key(fb), key(cur), "query %q (ord:tf must match the oracle)", q)
			}
		})
	}
}
