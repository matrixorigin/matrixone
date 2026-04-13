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

package catalog

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestFullTextIndexParamsAllowDefaultParser(t *testing.T) {
	params, err := IndexParamsToJsonString(tree.NewFullTextIndex(nil, "ftidx", false, &tree.IndexOption{}))
	require.NoError(t, err)
	require.Empty(t, params)
}

func TestFullTextIndexParamsNormalizeExplicitParser(t *testing.T) {
	params, err := IndexParamsToJsonString(tree.NewFullTextIndex(nil, "ftidx", false, &tree.IndexOption{
		ParserName: "NGRAM",
	}))
	require.NoError(t, err)
	require.Equal(t, `{"parser":"ngram"}`, params)
}

func TestFullTextIndexParamsRejectInvalidParser(t *testing.T) {
	_, err := IndexParamsToJsonString(tree.NewFullTextIndex(nil, "ftidx", false, &tree.IndexOption{
		ParserName: "bad_parser",
	}))
	require.Error(t, err)
}
