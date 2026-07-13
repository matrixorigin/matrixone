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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// TestBuildFullTextParams_Version covers the VERSION index-option plumbing into
// algo_params: VERSION=2 selects the WAND fulltext v2 engine; unset/1 leaves the
// classic index untouched (no version param recorded).
func TestBuildFullTextParams_Version(t *testing.T) {
	// VERSION=2 is recorded.
	js, err := buildFullTextParams(&tree.FullTextIndex{
		IndexOption: &tree.IndexOption{ParserName: "gojieba", Version: 2},
	})
	require.NoError(t, err)
	m, err := catalog.IndexParamsStringToMap(js)
	require.NoError(t, err)
	require.Equal(t, "2", m[catalog.IndexAlgoParamVersion])
	require.Equal(t, "gojieba", m["parser"])

	// Unset (0) and explicit V1 carry NO version param — classic index is unchanged.
	for _, v := range []int64{0, 1} {
		js, err := buildFullTextParams(&tree.FullTextIndex{
			IndexOption: &tree.IndexOption{ParserName: "gojieba", Version: v},
		})
		require.NoError(t, err)
		m, err := catalog.IndexParamsStringToMap(js)
		require.NoError(t, err)
		require.NotContains(t, m, catalog.IndexAlgoParamVersion, "version=%d must not be recorded", v)
	}
}
