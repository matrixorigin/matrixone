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

package plan_test

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	ft2plan "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestMoIndexStatsFulltext2Configuration(t *testing.T) {
	source := &pbplan.TableDef{Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "id"}}
	for _, tc := range []struct{ params, parser string }{
		{"", "ngram"}, {`{"parser":"default"}`, "ngram"}, {`{"parser":"JSON","async":"false"}`, "json"},
	} {
		defs := []*pbplan.IndexDef{
			{IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: "meta", Parts: []string{"body", "title"}, IndexAlgoParams: tc.params},
			{IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: "segments", Parts: []string{"body", "title"}, IndexAlgoParams: tc.params},
			{IndexAlgoTableType: catalog.IndexIgnored_TblType, IndexTableName: "rejected"},
		}
		var cfg indexstats.Config
		require.NoError(t, (ft2plan.Hooks{}).ConfigureIndexStats(t.Context(), source, defs, &cfg))
		require.Equal(t, int64(2), cfg.FullTextVersion)
		require.True(t, cfg.Async)
		require.Equal(t, tc.parser, cfg.Parser)
		require.Equal(t, []string{"body", "title"}, cfg.Columns)
		require.Equal(t, "meta", cfg.Metadata)
		require.Equal(t, "segments", cfg.Entries)
		require.Len(t, cfg.Tables, 3)
		for _, incomplete := range [][]*pbplan.IndexDef{defs[:1], defs[1:]} {
			cfg = indexstats.Config{}
			require.ErrorContains(t, (ft2plan.Hooks{}).ConfigureIndexStats(t.Context(), source, incomplete, &cfg), "incomplete")
		}
		cfg = indexstats.Config{}
		require.ErrorContains(t, (ft2plan.Hooks{}).ConfigureIndexStats(t.Context(), nil, defs, &cfg), "incomplete")
		defs[0].IndexAlgoParams = "{"
		require.Error(t, (ft2plan.Hooks{}).ConfigureIndexStats(t.Context(), source, defs, &cfg))
	}
}
