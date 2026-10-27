// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParseFirstArrayTypeParam(t *testing.T) {
	for _, tt := range []struct {
		input string
		width int
		valid bool
	}{
		{input: " 38,18", width: 38, valid: true},
		{input: "-1,ignored", width: -1, valid: true},
		{input: "0", width: 0, valid: true},
		{input: ",10", width: -1},
		{input: "invalid,10", width: -1},
		{input: "999999999999999999999999999", width: -1},
	} {
		t.Run(tt.input, func(t *testing.T) {
			width, valid := parseFirstArrayTypeParam(tt.input)
			require.Equal(t, tt.width, width)
			require.Equal(t, tt.valid, valid)
		})
	}
}
