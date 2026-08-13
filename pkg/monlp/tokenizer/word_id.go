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

package tokenizer

import (
	"bufio"
	"os"
	"strings"
	"sync"
)

// Word-id support for the WAND retrieval index.
//
// jieba.dict.utf8 is a fixed list of ~349k dictionary words ("word freq POS"
// per line). Each word's line index is a stable, process-wide word id, shared
// by every index — so dictionary words need no per-index term storage. Tokens
// NOT in the dictionary (English words, numbers, user-dict / HMM-discovered
// tokens) return ok=false; the caller assigns them per-index overflow ids in a
// range above DictWordIDLimit.

// DictWordIDLimit is the exclusive upper bound of global (dictionary) word ids.
// Out-of-dictionary overflow ids must be assigned at or above this value so the
// two id spaces never collide, regardless of the dictionary's exact size.
const DictWordIDLimit = int32(1) << 24 // 16,777,216 (dict has ~349k entries)

var (
	wordIDOnce sync.Once
	wordIDMap  map[string]int32
	wordIDErr  error
)

func loadWordIDMap() {
	path := jiebaDictPaths()[0] // jieba.dict.utf8
	f, err := os.Open(path)
	if err != nil {
		wordIDErr = err
		return
	}
	defer f.Close()

	m := make(map[string]int32, 400000)
	var id int32
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			id++
			continue
		}
		// "word freq POS" — the word is the first space-separated field.
		word := line
		if sp := strings.IndexByte(line, ' '); sp >= 0 {
			word = line[:sp]
		}
		// First occurrence wins (the line index is the stable id); the dict has
		// a few case variants but no exact-duplicate words.
		if _, exists := m[word]; !exists {
			m[word] = id
		}
		id++
	}
	if err := sc.Err(); err != nil {
		wordIDErr = err
		return
	}
	if id >= DictWordIDLimit {
		// Defensive: the dict grew past the reserved global id space.
		wordIDErr = errDictTooLarge
		return
	}
	wordIDMap = m
}

var errDictTooLarge = &dictError{"jieba dictionary exceeds DictWordIDLimit"}

type dictError struct{ msg string }

func (e *dictError) Error() string { return e.msg }

// WordID returns the global word id of a jieba-dictionary word (its line index
// in jieba.dict.utf8). ok is false for out-of-dictionary tokens, which the
// caller maps to per-index overflow ids (>= DictWordIDLimit). The dictionary is
// loaded once on first call and shared process-wide.
func WordID(word string) (id int32, ok bool, err error) {
	wordIDOnce.Do(loadWordIDMap)
	if wordIDErr != nil {
		return 0, false, wordIDErr
	}
	id, ok = wordIDMap[word]
	return id, ok, nil
}
