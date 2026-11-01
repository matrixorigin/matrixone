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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func ftIndexOpt(opt *tree.IndexOption) *tree.FullTextIndex {
	return &tree.FullTextIndex{IndexOption: opt}
}

// buildFullText2Params stores token_filter only for a gojieba POSITION_FREE index, and
// rejects every unsupported combination — the CPU-side coverage for the gojieba-only,
// bag-of-words-only contract (BVT for it is GPU-gated/skipped in non-GPU CI).
func TestBuildFullText2Params_TokenFilter(t *testing.T) {
	// valid: gojieba + POSITION_FREE + stop → params carry token_filter=stop.
	params, err := buildFullText2Params(ftIndexOpt(&tree.IndexOption{
		ParserName:   "gojieba",
		PositionFree: true,
		TokenFilters: []string{"stop"},
	}))
	require.NoError(t, err)
	require.Contains(t, params, catalog.IndexAlgoParamTokenFilter)
	require.Contains(t, params, "stop")

	// case/space are normalized to the canonical stored name.
	params, err = buildFullText2Params(ftIndexOpt(&tree.IndexOption{
		ParserName:   "gojieba",
		PositionFree: true,
		TokenFilters: []string{" STOP "},
	}))
	require.NoError(t, err)
	require.Contains(t, params, `"`+catalog.IndexAlgoParamTokenFilter+`":"stop"`)

	// token_filter without POSITION_FREE is rejected.
	_, err = buildFullText2Params(ftIndexOpt(&tree.IndexOption{
		ParserName:   "gojieba",
		TokenFilters: []string{"stop"},
	}))
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "POSITION_FREE"))

	// POSITION_FREE requires gojieba, so ngram is rejected before token_filter is reached.
	_, err = buildFullText2Params(ftIndexOpt(&tree.IndexOption{
		ParserName:   "ngram",
		PositionFree: true,
		TokenFilters: []string{"stop"},
	}))
	require.Error(t, err)

	// an unknown filter name is rejected.
	_, err = buildFullText2Params(ftIndexOpt(&tree.IndexOption{
		ParserName:   "gojieba",
		PositionFree: true,
		TokenFilters: []string{"bogus"},
	}))
	require.Error(t, err)

	// no token_filter → params never mention it.
	params, err = buildFullText2Params(ftIndexOpt(&tree.IndexOption{ParserName: "gojieba"}))
	require.NoError(t, err)
	require.NotContains(t, params, catalog.IndexAlgoParamTokenFilter)
}
