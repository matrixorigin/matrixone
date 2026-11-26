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
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	ivfplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"github.com/stretchr/testify/require"
)

func TestMoIndexStatsIVFConfiguration(t *testing.T) {
	source := &pbplan.TableDef{Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "id"}}
	defs := []*pbplan.IndexDef{
		{IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Metadata, IndexTableName: "meta", Parts: []string{"embedding"}},
		{IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Centroids, IndexTableName: "centers"},
		{IndexAlgoTableType: catalog.SystemSI_IVFFLAT_TblType_Entries, IndexTableName: "entries"},
		{IndexAlgoTableType: catalog.IndexIgnored_TblType, IndexTableName: "rejected"},
	}
	for _, tc := range []struct {
		params, op, quant string
		lists             int64
		async             bool
	}{
		{`{}`, "vector_l2_ops", "none", 1, false},
		{`{"async":"bad"}`, "vector_l2_ops", "none", 1, false},
		{`{"lists":"17","op_type":"vector_cosine_ops","quantization":"int8","async":"true"}`, "vector_cosine_ops", "int8", 17, true},
	} {
		defs[0].IndexAlgoParams = tc.params
		var cfg indexstats.Config
		require.NoError(t, (ivfplan.Hooks{}).ConfigureIndexStats(t.Context(), source, defs, &cfg))
		require.Equal(t, tc.lists, cfg.Lists)
		require.Equal(t, tc.op, cfg.OpType)
		require.Equal(t, tc.quant, cfg.Quantization)
		require.Equal(t, tc.async, cfg.Async)
		require.Equal(t, "embedding", cfg.Column)
		require.Equal(t, "meta", cfg.Metadata)
		require.Equal(t, "centers", cfg.Centroids)
		require.Equal(t, "entries", cfg.Entries)
		require.Len(t, cfg.Tables, 4)
	}
	for _, params := range []string{`{`, `{"lists":"0"}`, `{"lists":"-1"}`, `{"lists":"bad"}`, `{"async":true}`} {
		defs[0].IndexAlgoParams = params
		var cfg indexstats.Config
		require.Error(t, (ivfplan.Hooks{}).ConfigureIndexStats(t.Context(), source, defs, &cfg), params)
	}
	defs[0].IndexAlgoParams = `{}`
	var cfg indexstats.Config
	require.ErrorContains(t, (ivfplan.Hooks{}).ConfigureIndexStats(t.Context(), source, defs[:2], &cfg), "incomplete")
	cfg = indexstats.Config{}
	require.ErrorContains(t, (ivfplan.Hooks{}).ConfigureIndexStats(t.Context(), nil, defs, &cfg), "incomplete")
	defs[0].Parts = []string{"a", "b"}
	require.ErrorContains(t, (ivfplan.Hooks{}).ConfigureIndexStats(t.Context(), source, defs, &cfg), "column")
}
