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

package plan

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSONFunctionalIndexMetadataSurvivesUnusedExpressions(t *testing.T) {
	index := &IndexDef{IndexName: "j", IndexAlgoParams: `{"functional_json_semantics":"1"}`}
	// Deliberately no columns/expressions: this is the metadata retained by a
	// pruned SELECT1/LIMIT0 source, not proof of an operator execution.
	query := &Query{Nodes: []*Node{{TableDef: &TableDef{Indexes: []*IndexDef{index}}}}}
	features, err := RequiredRemoteExpressionFeatures(query)
	require.NoError(t, err)
	require.True(t, features.JSONFunctionalIndexSemantics)
	require.True(t, features.Any())
	bytes, err := query.Marshal()
	require.NoError(t, err)
	var decoded Query
	require.NoError(t, decoded.Unmarshal(bytes))
	features, err = RequiredRemoteExpressionFeatures(&decoded)
	require.NoError(t, err)
	require.True(t, features.JSONFunctionalIndexSemantics)
}

func TestJSONFunctionalIndexUnknownRevisionFailsClosed(t *testing.T) {
	for _, params := range []string{`{"functional_json_semantics":"0"}`, `{"functional_json_semantics":"2"}`, `{"functional_json_semantics":1}`, `{"functional_json_semantics":`} {
		_, err := RequiredRemoteExpressionFeatures(&TableDef{Indexes: []*IndexDef{{IndexAlgoParams: params}}})
		require.Error(t, err)
	}
	for _, params := range []string{"", `{}`, `{"comment":"functional_json_semantics"}`} {
		features, err := RequiredRemoteExpressionFeatures(&IndexDef{IndexAlgoParams: params})
		require.NoError(t, err)
		require.False(t, features.JSONFunctionalIndexSemantics)
	}
}
