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

package bytejson

import (
	"iter"
	"strconv"

	"github.com/matrixorigin/monlp/tokenizer"
)

// TokenizeValue tokenizes the values of the ByteJson object
// note that we do not break word with space, do not normalize
// case, 3-gram, etc etc, only truncate the string to 23 bytes.
func (bj ByteJson) TokenizeValue(includeKey bool) iter.Seq[tokenizer.Token] {
	return func(yield func(tokenizer.Token) bool) {
		tokenizeOne(bj, 1, includeKey, yield)
	}
}

func fillToken(t *tokenizer.Token, s []byte, pos int32) {
	copy(t.TokenBytes[1:], s)
	if len(s) > tokenizer.MAX_TOKEN_SIZE {
		t.TokenBytes[0] = tokenizer.MAX_TOKEN_SIZE
	} else {
		t.TokenBytes[0] = byte(len(s))
	}
	t.TokenPos = pos
}

func tokenizeOne(bj ByteJson, pos int32, includeKey bool, yield func(tokenizer.Token) bool) int32 {
	var t tokenizer.Token

	switch bj.Type {
	case TpCodeObject:
		// object: recursively tokenize each value
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			if includeKey {
				tag := bj.getObjectKey(i)
				fillToken(&t, tag, pos)
				if !yield(t) {
					// early stop
					return 0
				}
				pos = pos + 1
			}

			nextbj := bj.getObjectVal(i)
			pos = tokenizeOne(nextbj, pos, includeKey, yield)
			if pos == 0 {
				return 0
			}
		}
		return pos
	case TpCodeArray:
		// array: recursively tokenize each element
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			nextbj := bj.getArrayElem(i)
			pos = tokenizeOne(nextbj, pos, includeKey, yield)
			if pos == 0 {
				return 0
			}
		}
		return pos

	case TpCodeInt64:

		sbuf := t.TokenBytes[1:1]
		sbuf = strconv.AppendInt(sbuf, bj.GetInt64(), 10)
		t.TokenBytes[0] = byte(len(sbuf))
		t.TokenPos = pos
	case TpCodeUint64:
		sbuf := t.TokenBytes[1:1]
		sbuf = strconv.AppendUint(sbuf, bj.GetUint64(), 10)
		t.TokenBytes[0] = byte(len(sbuf))
		t.TokenPos = pos
	case TpCodeFloat64:
		sbuf := t.TokenBytes[1:1]
		sbuf, _ = bj.toFloat64(sbuf)
		t.TokenBytes[0] = byte(len(sbuf))
		t.TokenPos = pos
	case TpCodeString:
		s := bj.GetString()
		fillToken(&t, s, pos)

	default:
		// ignore other types, esp literal (true, false, null), are not tokenized.
		return pos
	}

	if !yield(t) {
		// early stop
		return 0
	}
	return pos + 1
}
