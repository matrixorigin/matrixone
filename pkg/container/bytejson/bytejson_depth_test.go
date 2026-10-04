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

package bytejson

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func independentJSONDepth(value any) int {
	maxDepth := 1
	switch value := value.(type) {
	case []any:
		for _, child := range value {
			if childDepth := 1 + independentJSONDepth(child); childDepth > maxDepth {
				maxDepth = childDepth
			}
		}
	case map[string]any:
		for _, child := range value {
			if childDepth := 1 + independentJSONDepth(child); childDepth > maxDepth {
				maxDepth = childDepth
			}
		}
	}
	return maxDepth
}

func TestByteJsonDepthMatchesIndependentOracle(t *testing.T) {
	for _, raw := range []string{
		`null`, `true`, `0`, `"叶"`, `[]`, `{}`,
		`[null, 1, {"值": [false, "文字"]}]`,
		`{"left": 1, "right": {"deep": [{"x": 0}]}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var tree any
			require.NoError(t, json.Unmarshal([]byte(raw), &tree))
			document, err := ParseFromString(raw)
			require.NoError(t, err)
			before := append([]byte(nil), document.Data...)
			require.Equal(t, independentJSONDepth(tree), document.Depth())
			require.Equal(t, before, document.Data)
		})
	}
}

func TestByteJsonDepthTracksContainerLimitBoundary(t *testing.T) {
	for _, containers := range []int{
		JSONDocumentMaxNestingDepth - 1,
		JSONDocumentMaxNestingDepth,
		JSONDocumentMaxNestingDepth + 1,
	} {
		raw := strings.Repeat(`{"a":`, containers) + `1` + strings.Repeat(`}`, containers)
		document, err := ParseFromString(raw)
		require.NoError(t, err)
		require.Equal(t, containers+1, document.Depth())
	}

	atLimit := strings.Repeat(`{"a":`, JSONDocumentMaxNestingDepth) + `1` +
		strings.Repeat(`}`, JSONDocumentMaxNestingDepth)
	_, err := ParseFromByteSliceWithDepthLimit([]byte(atLimit), JSONDocumentMaxNestingDepth)
	require.NoError(t, err)

	tooDeep := strings.Repeat(`{"a":`, JSONDocumentMaxNestingDepth+1) + `1` +
		strings.Repeat(`}`, JSONDocumentMaxNestingDepth+1)
	_, err = ParseFromByteSliceWithDepthLimit([]byte(tooDeep), JSONDocumentMaxNestingDepth)
	require.ErrorContains(t, err, "json document nesting depth exceeds 100")
}

func TestByteJsonDepthCancellationHook(t *testing.T) {
	document, err := ParseFromString(`[1,2,3,4]`)
	require.NoError(t, err)
	calls := 0
	_, err = document.DepthWithCheck(func() error {
		calls++
		if calls == 2 {
			return context.Canceled
		}
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, calls)
}
