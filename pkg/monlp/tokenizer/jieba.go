// Copyright 2024 Matrix Origin
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

package tokenizer

import (
	"iter"
	"strings"
	"sync"

	"github.com/yanyiwu/gojieba"
)

type JiebaTokenizer struct {
	jieba  *gojieba.Jieba
	useHmm bool
	shared bool
}

var (
	sharedJiebaHmmOnce   sync.Once
	sharedJiebaHmm       *JiebaTokenizer
	sharedJiebaNoHmmOnce sync.Once
	sharedJiebaNoHmm     *JiebaTokenizer
)

// SharedJiebaTokenizer returns a process-wide JiebaTokenizer. Two singletons
// are maintained — one with HMM enabled and one without — and each is loaded
// lazily on first use (~1s for dictionary loading). The returned tokenizer is
// safe for concurrent Tokenize calls and must not be Free'd.
//
// Choose useHmm by intent:
//   - false at index build time: dictionary-only segmentation gives stable,
//     reproducible tokens that don't drift across deployments.
//   - true at query time: HMM new-word discovery broadens recall for terms
//     not in the dictionary.
func SharedJiebaTokenizer(useHmm bool) *JiebaTokenizer {
	if useHmm {
		sharedJiebaHmmOnce.Do(func() {
			sharedJiebaHmm = &JiebaTokenizer{
				jieba:  gojieba.NewJieba(),
				useHmm: true,
				shared: true,
			}
		})
		return sharedJiebaHmm
	}
	sharedJiebaNoHmmOnce.Do(func() {
		sharedJiebaNoHmm = &JiebaTokenizer{
			jieba:  gojieba.NewJieba(),
			useHmm: false,
			shared: true,
		}
	})
	return sharedJiebaNoHmm
}

// NewJiebaTokenizer constructs a JiebaTokenizer backed by gojieba.
// When useHmm is true the HMM model is used to discover unknown words during
// segmentation; when false only dictionary-based segmentation is performed.
//
// The returned tokenizer holds C resources that are released either when
// Free is called explicitly or when the value is garbage collected.
func NewJiebaTokenizer(useHmm bool) *JiebaTokenizer {
	return &JiebaTokenizer{
		jieba:  gojieba.NewJieba(),
		useHmm: useHmm,
	}
}

// Free releases the underlying gojieba resources. Subsequent calls are no-ops.
// Free is a no-op for the shared tokenizer returned by SharedJiebaTokenizer.
func (t *JiebaTokenizer) Free() {
	if t.shared {
		return
	}
	if t.jieba != nil {
		t.jieba.Free()
		t.jieba = nil
	}
}

func isAllBreaker(s string) bool {
	if len(s) == 0 {
		return true
	}
	for _, r := range s {
		if !isBreakerRune(r) {
			return false
		}
	}
	return true
}

// truncateUTF8 returns a prefix of bs of at most maxLen bytes that ends on a
// valid UTF-8 boundary.
func truncateUTF8(bs []byte, maxLen int) []byte {
	if len(bs) <= maxLen {
		return bs
	}
	n := maxLen
	for n > 0 && bs[n]&0xC0 == 0x80 {
		n--
	}
	return bs[:n]
}

func (t *JiebaTokenizer) Tokenize(input []byte) iter.Seq[Token] {
	return func(yield func(Token) bool) {
		if t.jieba == nil || len(input) == 0 {
			return
		}

		words := t.jieba.Tokenize(string(input), gojieba.DefaultMode, t.useHmm)
		var tokenPos int32
		for _, w := range words {
			if isAllBreaker(w.Str) {
				continue
			}

			lowered := strings.ToLower(w.Str)
			bs := truncateUTF8([]byte(lowered), MAX_TOKEN_SIZE)

			var token Token
			token.TokenBytes[0] = byte(len(bs))
			copy(token.TokenBytes[1:], bs)
			token.TokenPos = tokenPos
			token.BytePos = int32(w.Start)
			if !yield(token) {
				return
			}
			tokenPos++
		}
	}
}
