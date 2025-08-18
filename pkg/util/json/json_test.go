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

package json

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type v struct {
	X int `json:"x"`
}

func TestMustMarshal(t *testing.T) {
	assert.Equal(t, `{"x":1}`, string(MustMarshal(&v{X: 1})))
}

var prettyV = `{
  "x": 1
}
`

func TestMustPretty(t *testing.T) {
	assert.Equal(t, prettyV, string(Pretty(&v{X: 1})))
}

func TestUnmarshal(t *testing.T) {
	var v any
	var va []any

	s := `["a", "b", "c"]`
	MustUnmarshal([]byte(s), &v)
	assert.Equal(t, []any{"a", "b", "c"}, v)

	MustUnmarshal([]byte(s), &va)
	assert.Equal(t, []any{"a", "b", "c"}, va)

	s2 := `"a"`
	MustUnmarshal([]byte(s2), &v)
	assert.Equal(t, "a", v)

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("panic: %v", r)
			}
		}()
		MustUnmarshal([]byte(s2), &va)
	}()
}

func TestRunJQOnString(t *testing.T) {
	iv, err := RunJQInt(`.x`, `{"x":1}`)
	assert.NoError(t, err)
	assert.Equal(t, 1, iv)

	fv, err := RunJQFloat(`.x`, `{"x":1}`)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, fv)
}
