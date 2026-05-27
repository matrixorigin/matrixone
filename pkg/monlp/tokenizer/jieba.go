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
	"fmt"
	"iter"
	"os"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	sharedJiebaHmmErr    error
	sharedJiebaNoHmmOnce sync.Once
	sharedJiebaNoHmm     *JiebaTokenizer
	sharedJiebaNoHmmErr  error
)

// newJiebaChecked validates that every dictionary file exists before calling
// gojieba.NewJieba, which otherwise panics inside cgo if a path is missing.
// The check turns the dominant failure mode into a returned error; other
// gojieba initialization failures (e.g. malformed dictionaries) still panic.
func newJiebaChecked(paths [5]string) (*gojieba.Jieba, error) {
	for _, p := range paths {
		if _, statErr := os.Stat(p); statErr != nil {
			return nil, moerr.NewInternalErrorNoCtx(
				fmt.Sprintf("jieba dictionary not available: %s: %v", p, statErr))
		}
	}
	return gojieba.NewJieba(paths[:]...), nil
}

// SharedJiebaTokenizer returns a process-wide JiebaTokenizer. Two singletons
// are maintained — one with HMM enabled and one without — and each is loaded
// lazily on first use (~1s for dictionary loading). The returned tokenizer is
// safe for concurrent Tokenize calls and must not be Free'd.
//
// If dictionary files are missing or gojieba initialization otherwise fails,
// the error is cached and returned on every subsequent call for the same
// useHmm value — initialization is not retried.
//
// Choose useHmm by intent:
//   - false at index build time: dictionary-only segmentation gives stable,
//     reproducible tokens that don't drift across deployments.
//   - true at query time: HMM new-word discovery broadens recall for terms
//     not in the dictionary.
func SharedJiebaTokenizer(useHmm bool) (*JiebaTokenizer, error) {
	paths := jiebaDictPaths()
	if useHmm {
		sharedJiebaHmmOnce.Do(func() {
			j, err := newJiebaChecked(paths)
			if err != nil {
				sharedJiebaHmmErr = err
				return
			}
			sharedJiebaHmm = &JiebaTokenizer{
				jieba:  j,
				useHmm: true,
				shared: true,
			}
		})
		return sharedJiebaHmm, sharedJiebaHmmErr
	}
	sharedJiebaNoHmmOnce.Do(func() {
		j, err := newJiebaChecked(paths)
		if err != nil {
			sharedJiebaNoHmmErr = err
			return
		}
		sharedJiebaNoHmm = &JiebaTokenizer{
			jieba:  j,
			useHmm: false,
			shared: true,
		}
	})
	return sharedJiebaNoHmm, sharedJiebaNoHmmErr
}

// NewJiebaTokenizer constructs a JiebaTokenizer backed by gojieba.
// When useHmm is true the HMM model is used to discover unknown words during
// segmentation; when false only dictionary-based segmentation is performed.
//
// Returns an error if dictionary files are missing or gojieba initialization
// fails.
//
// The returned tokenizer holds C resources that are released either when
// Free is called explicitly or when the value is garbage collected.
func NewJiebaTokenizer(useHmm bool) (*JiebaTokenizer, error) {
	paths := jiebaDictPaths()
	j, err := newJiebaChecked(paths)
	if err != nil {
		return nil, err
	}
	return &JiebaTokenizer{
		jieba:  j,
		useHmm: useHmm,
	}, nil
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

func (t *JiebaTokenizer) Tokenize(input []byte) iter.Seq2[Token, error] {
	return func(yield func(Token, error) bool) {
		if t.jieba == nil || len(input) == 0 {
			return
		}

		var tokenPos int32
		emit := func(word string, bytePos int32) bool {
			lowered := strings.ToLower(word)
			bs := truncateUTF8([]byte(lowered), MAX_TOKEN_SIZE)
			var tk Token
			tk.TokenBytes[0] = byte(len(bs))
			copy(tk.TokenBytes[1:], bs)
			tk.TokenPos = tokenPos
			tk.BytePos = bytePos
			tokenPos++
			return yield(tk, nil)
		}

		// gojieba is a Chinese-first segmenter: with HMM=false an English
		// word like "color" has no dictionary entry and falls through to
		// per-character tokens. Pre-split the input into pure-ASCII spans
		// (handled by SimpleTokenizer's Latin path) and the rest (handed
		// to gojieba). This boundary always lands on a UTF-8 char edge
		// because every multi-byte UTF-8 byte is >= 0x80.
		var sub SimpleTokenizer
		i := 0
		for i < len(input) {
			ascii := input[i] < 0x80
			j := i + 1
			for j < len(input) && (input[j] < 0x80) == ascii {
				j++
			}
			chunk := input[i:j]
			chunkOff := int32(i)
			i = j

			if ascii {
				for tk, err := range sub.Tokenize(chunk) {
					if err != nil {
						yield(Token{}, err)
						return
					}
					slen := tk.TokenBytes[0]
					word := string(tk.TokenBytes[1 : slen+1])
					if !emit(word, chunkOff+tk.BytePos) {
						return
					}
				}
				continue
			}

			words := t.jieba.Tokenize(string(chunk), gojieba.DefaultMode, t.useHmm)
			for _, w := range words {
				if isAllBreaker(w.Str) {
					continue
				}
				if !emit(w.Str, chunkOff+int32(w.Start)) {
					return
				}
			}
		}
	}
}
