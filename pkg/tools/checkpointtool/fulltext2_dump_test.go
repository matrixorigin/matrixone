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

package checkpointtool

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/stretchr/testify/require"
)

// TestRenderCreateIndexStatementFulltext2 pins the checkpoint DDL round trip for a
// fulltext2 index: it must emit FULLTEXT2 (not classic FULLTEXT, which would drop the
// positional / position-free engine on restore) and carry the full persisted option
// set (parser + position_free + capacities). A classic fulltext index still renders
// FULLTEXT.
func TestRenderCreateIndexStatementFulltext2(t *testing.T) {
	params, err := catalog.IndexParamsMapToJsonString(map[string]string{
		"parser":                                  "ngram",
		catalog.IndexAlgoParamPositionFree:        "true",
		catalog.IndexAlgoParamMaxIndexCapacity:    "1000",
		catalog.IndexAlgoParamMaxPostingsCapacity: "8000",
	})
	require.NoError(t, err)

	info := &indexDDLInfo{
		name:       "ft",
		indexType:  "FULLTEXT",
		algo:       catalog.MoIndexFullText2Algo.ToString(),
		algoParams: params,
		columns:    map[string]indexDDLColumn{"body": {name: "body", ordinal: 0}},
	}

	ddl, err := renderCreateIndexStatement("t", info)
	require.NoError(t, err)
	require.Contains(t, ddl, "FULLTEXT2 ", "engine preserved on restore")
	require.NotContains(t, ddl, "FULLTEXT ", "not downgraded to classic fulltext")
	require.Contains(t, ddl, "WITH PARSER ngram")
	require.Contains(t, ddl, catalog.IndexAlgoParamPositionFree+" = true")
	require.Contains(t, ddl, catalog.IndexAlgoParamMaxIndexCapacity+" = 1000")
	require.Contains(t, ddl, catalog.IndexAlgoParamMaxPostingsCapacity+" = 8000")

	// classic fulltext keeps the FULLTEXT keyword (regression guard).
	info.algo = catalog.MOIndexFullTextAlgo.ToString()
	ddl2, err := renderCreateIndexStatement("t", info)
	require.NoError(t, err)
	require.Contains(t, ddl2, "FULLTEXT ")
	require.NotContains(t, ddl2, "FULLTEXT2")
}
