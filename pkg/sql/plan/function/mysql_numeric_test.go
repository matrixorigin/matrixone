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

package function

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMySQLIntegerPrefix(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  int64
	}{
		{name: "plain prefix", value: "256tail", want: 256},
		{name: "leading ascii whitespace and sign", value: "\t +224tail", want: 224},
		{name: "negative", value: "-256tail", want: -256},
		{name: "decimal stops at point", value: "224.9tail", want: 224},
		{name: "no prefix", value: "abc", want: 0},
		{name: "empty", value: "", want: 0},
		{name: "whitespace only", value: " \n\r", want: 0},
		{name: "negative zero", value: "-0tail", want: 0},
		{name: "positive overflow clamps", value: "9223372036854775808tail", want: math.MaxInt64},
		{name: "negative overflow clamps", value: "-9223372036854775809tail", want: math.MinInt64},
		{name: "binary bytes are not unicode whitespace", value: string([]byte{0xc2, 0xa0, '2'}), want: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, parseMySQLIntegerPrefix([]byte(test.value)))
		})
	}
}
