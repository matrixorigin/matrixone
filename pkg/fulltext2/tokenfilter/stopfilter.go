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

package tokenfilter

import (
	"iter"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
)

// StopwordFile is the builtin stopword list, resolved in the jieba dict
// directory. It is gojieba's stop_words.utf8 (618 English words + Chinese +
// punctuation, one entry per line), reused as the unified English+Chinese stop
// list. gojieba's own tokenizer never consults this file (only its keyword/POS
// APIs do), so StopFilter loads and applies it here.
const StopwordFile = "stop_words.utf8"

// StopFilter drops tokens whose word is in the stopword set, leaving every
// surviving token's TokenPos/BytePos untouched so a positional phrase still
// matches by the original byte gaps. The words in the set must be in the
// tokenizer's folded form (lower-cased Latin), which is how both the builtin
// list and the tokenizer represent them: the gojieba tokenizer routes Latin
// runs through SimpleTokenizer (which lower-cases), and CJK segments match
// their file entries verbatim.
type StopFilter struct {
	set map[string]struct{}
}

// NewStopFilter returns a StopFilter over set.
func NewStopFilter(set map[string]struct{}) *StopFilter {
	return &StopFilter{set: set}
}

func (f *StopFilter) Filter(in iter.Seq2[tokenizer.Token, error]) iter.Seq2[tokenizer.Token, error] {
	return func(yield func(tokenizer.Token, error) bool) {
		for tk, err := range in {
			if err != nil {
				yield(tokenizer.Token{}, err)
				return
			}
			slen := tk.TokenBytes[0]
			if _, stop := f.set[string(tk.TokenBytes[1:slen+1])]; stop {
				continue
			}
			if !yield(tk, nil) {
				return
			}
		}
	}
}

var (
	builtinStopMu  sync.Mutex
	builtinStopSet map[string]struct{}
)

// BuiltinStopwords returns the builtin stopword set loaded from the jieba dict
// directory's stop_words.utf8 (resolved the same way the tokenizer locates its
// dictionaries). Only a successful load is cached; callers must not mutate it.
func BuiltinStopwords() (map[string]struct{}, error) {
	builtinStopMu.Lock()
	defer builtinStopMu.Unlock()
	if builtinStopSet != nil {
		return builtinStopSet, nil
	}
	path := filepath.Join(tokenizer.JiebaDictDir(), StopwordFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("fulltext stopword file %s: %v", path, err)
	}
	builtinStopSet = ParseStopwords(data)
	return builtinStopSet, nil
}

// ParseStopwords builds a stopword set from word-per-line bytes, splitting on
// "\n" (so "\r\n" is handled by trimming the surrounding whitespace, "\r"
// included) and trimming each line; blank lines are skipped.
func ParseStopwords(data []byte) map[string]struct{} {
	set := make(map[string]struct{})
	for _, line := range strings.Split(string(data), "\n") {
		w := strings.TrimSpace(line)
		if w == "" {
			continue
		}
		set[w] = struct{}{}
	}
	return set
}
