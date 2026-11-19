// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bytejson

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

func TestMySQLOpaqueVisibleLineBoundaries(t *testing.T) {
	for _, tc := range []struct {
		n       int
		encoded string
	}{
		{0, ""}, {1, "YQ=="}, {57, strings.Repeat("YWFh", 19)},
		{58, strings.Repeat("YWFh", 19) + "\nYQ=="},
		{114, strings.Repeat("YWFh", 19) + "\n" + strings.Repeat("YWFh", 19)},
		{115, strings.Repeat("YWFh", 19) + "\n" + strings.Repeat("YWFh", 19) + "\nYQ=="},
	} {
		t.Run(fmt.Sprint(tc.n), func(t *testing.T) {
			value, err := NewMySQLOpaque(defines.MORPCVersion52, 252, []byte(strings.Repeat("a", tc.n)))
			require.NoError(t, err)
			stored, err := value.Marshal()
			require.NoError(t, err)
			expected := "base64:type252:" + tc.encoded
			got, err := value.MarshalJSON()
			require.NoError(t, err)
			var decoded string
			require.NoError(t, json.Unmarshal(got, &decoded))
			require.Equal(t, expected, decoded)
			unquoted, err := value.Unquote()
			require.NoError(t, err)
			require.Equal(t, expected, unquoted)
			after, err := value.Marshal()
			require.NoError(t, err)
			require.Equal(t, stored, after)
			require.NotContains(t, string(value.GetString()), "\n", "storage remains compact")
			nested, err := CreateByteJSON(map[string]any{"v": []any{value}})
			require.NoError(t, err)
			text, err := nested.MarshalJSON()
			require.NoError(t, err)
			var object map[string][]string
			require.NoError(t, json.Unmarshal(text, &object))
			require.Equal(t, []string{expected}, object["v"])
		})
	}
}
