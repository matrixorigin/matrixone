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
	input  string
	tokens []string
}

func TestByteJson(t *testing.T) {
	tcs := []tokenTestCase{
		{
			input:  `{"a": 1, "b": 2}`,
			tokens: []string{"1", "2"},
		},
		{
			input:  `{"a": [1, 2], "b": [3, true, "hello"], "c": "hello again"}`,
			tokens: []string{"1", "2", "3", "hello", "hello again"},
		},
		{
			input:  `{"a": [1.2, 2.0], "b": [3, true, "hello"], "c": "abcdefghijklmnopqrstuvwxyz"}`,
			tokens: []string{"1.2", "2", "3", "hello", "abcdefghijklmnopqrstuvw"},
		},
		{
			input:  `{"a": "相见时难别亦难", "b": "I come, I see, I 征服", "c": "相见时难别亦难，东风无力百花残。 春蚕到死丝方尽，蜡炬成灰泪始干。"}`,
			tokens: []string{"相见时难别亦难", "I come, I see, I 征服", "相见时难别亦难，东风无力百花残。 春蚕到死丝方尽，蜡炬成灰泪始干。"},
		},
	}

	for _, tc := range tcs {
		var bj ByteJson
		if err := json.Unmarshal([]byte(tc.input), &bj); err != nil {
			t.Fatal(err)
		}

		var tokens []tokenizer.Token
		tokens = bj.TokenizeValue(tokens)
		if len(tokens) != len(tc.tokens) {
			t.Fatalf("expected %d tokens, got %d", len(tc.tokens), len(tokens))
		}

		for i := range tokens {
			var tk tokenizer.Token
			if len(tc.tokens[i]) > tokenizer.MAX_TOKEN_SIZE {
				tk.TokenBytes[0] = byte(tokenizer.MAX_TOKEN_SIZE)
			} else {
				tk.TokenBytes[0] = byte(len(tc.tokens[i]))
			}
			copy(tk.TokenBytes[1:], tc.tokens[i])

			if tokens[i].TokenPos != int32(i+1) || tokens[i].TokenBytes != tk.TokenBytes {
				t.Errorf("expected token %s, got %s", tc.tokens[i], tokens[i].TokenBytes)
			}
		}
	}
}
