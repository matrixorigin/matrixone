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
	"strconv"

	"github.com/matrixorigin/monlp/tokenizer"
)

// TokenizeValue tokenizes the values of the ByteJson object
// note that we do not break word with space, do not normalize
// case, 3-gram, etc etc, only truncate the string to 23 bytes.
func (bj ByteJson) TokenizeValue(buf []tokenizer.Token) []tokenizer.Token {
	switch bj.Type {
	case TpCodeObject:
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			buf = bj.getObjectVal(i).TokenizeValue(buf)
		}
	case TpCodeArray:
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			buf = bj.getArrayElem(i).TokenizeValue(buf)
		}
	case TpCodeInt64:
		var t tokenizer.Token
		sbuf := t.TokenBytes[1:1]
		sbuf = strconv.AppendInt(sbuf, bj.GetInt64(), 10)
		t.TokenBytes[0] = byte(len(sbuf))
		t.TokenPos = int32(len(buf) + 1)
		buf = append(buf, t)
	case TpCodeUint64:
		var t tokenizer.Token
		sbuf := t.TokenBytes[1:1]
		sbuf = strconv.AppendUint(sbuf, bj.GetUint64(), 10)
		t.TokenBytes[0] = byte(len(sbuf))
		t.TokenPos = int32(len(buf) + 1)
		buf = append(buf, t)
	case TpCodeFloat64:
		var t tokenizer.Token
		sbuf := t.TokenBytes[1:1]
		sbuf, _ = bj.toFloat64(sbuf)
		t.TokenBytes[0] = byte(len(sbuf))
		t.TokenPos = int32(len(buf) + 1)
		buf = append(buf, t)
	case TpCodeString:
		var t tokenizer.Token
		s := bj.GetString()
		copy(t.TokenBytes[1:], s)
		if len(s) > tokenizer.MAX_TOKEN_SIZE {
			t.TokenBytes[0] = 23
		} else {
			t.TokenBytes[0] = byte(len(s))
		}
		t.TokenPos = int32(len(buf) + 1)
		buf = append(buf, t)
	default:
	}
	return buf
}
