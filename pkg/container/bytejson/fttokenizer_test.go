// Copyright 2022 Matrix Origin
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
	"encoding/json"
	"testing"

	"github.com/matrixorigin/monlp/tokenizer"
)

type tokenTestCase struct {
	input         string
	tokens        []string
	tokensWithKey []string
}

func checkTokens(t *testing.T, tokens []tokenizer.Token, expected []string) {
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}

	for i := range tokens {
		var tk tokenizer.Token
		if len(expected[i]) > tokenizer.MAX_TOKEN_SIZE {
			tk.TokenBytes[0] = byte(tokenizer.MAX_TOKEN_SIZE)
		} else {
			tk.TokenBytes[0] = byte(len(expected[i]))
		}
		copy(tk.TokenBytes[1:], expected[i])

		if tokens[i].TokenPos != int32(i+1) || tokens[i].TokenBytes != tk.TokenBytes {
			t.Errorf("expected token %s, got %s", expected[i], tokens[i].TokenBytes)
		}
	}
}

func TestByteJson(t *testing.T) {
	tcs := []tokenTestCase{
		{
			input:         `{"a": 1, "b": 2}`,
			tokens:        []string{"1", "2"},
			tokensWithKey: []string{"a", "1", "b", "2"},
		},
		{
			input:         `{"a": [1, 2], "b": [3, true, "hello"], "c": "hello again"}`,
			tokens:        []string{"1", "2", "3", "hello", "hello again"},
			tokensWithKey: []string{"a", "1", "2", "b", "3", "hello", "c", "hello again"},
		},
		{
			input:         `{"a": [1.2, 2.0], "b": [3, true, "hello"], "c": "abcdefghijklmnopqrstuvwxyz"}`,
			tokens:        []string{"1.2", "2", "3", "hello", "abcdefghijklmnopqrstuvw"},
			tokensWithKey: []string{"a", "1.2", "2", "b", "3", "hello", "c", "abcdefghijklmnopqrstuvw"},
		},
		{
			input:         `{"a": "相见时难别亦难", "b": "I come, I see, I 征服", "c": "相见时难别亦难，东风无力百花残。 春蚕到死丝方尽，蜡炬成灰泪始干。"}`,
			tokens:        []string{"相见时难别亦难", "I come, I see, I 征服", "相见时难别亦难，东风无力百花残。 春蚕到死丝方尽，蜡炬成灰泪始干。"},
			tokensWithKey: []string{"a", "相见时难别亦难", "b", "I come, I see, I 征服", "c", "相见时难别亦难，东风无力百花残。 春蚕到死丝方尽，蜡炬成灰泪始干。"},
		},
		{
			input:         `{"a bcdefghijklmnopqrstuvwxyz": 1, "学而时习之，不亦说乎": "说什么说， 就你话多"}`,
			tokens:        []string{"1", "说什么说， 就你话多"},
			tokensWithKey: []string{"a bcdefghijklmnopqrstuv", "1", "学而时习之，不亦说乎", "说什么说， 就你话多"},
		},
	}

	for _, tc := range tcs {
		var bj ByteJson
		if err := json.Unmarshal([]byte(tc.input), &bj); err != nil {
			t.Fatal(err)
		}

		var tokens []tokenizer.Token
		for tk := range bj.TokenizeValue(false) {
			tokens = append(tokens, tk)
		}
		checkTokens(t, tokens, tc.tokens)

		var tokensWithKey []tokenizer.Token
		for tk := range bj.TokenizeValue(true) {
			tokensWithKey = append(tokensWithKey, tk)
		}
		checkTokens(t, tokensWithKey, tc.tokensWithKey)
	}
}
