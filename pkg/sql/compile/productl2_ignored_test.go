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

package compile

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func l2Node(t *testing.T, cfg *vectorindex.IndexTableConfig, params string, cols ...string) *plan.Node {
	opt := tree.CentroidxOption{OpType: "vector_l2_ops"}
	if cfg != nil {
		b, err := json.Marshal(cfg)
		require.NoError(t, err)
		opt.Params, opt.Config = params, string(b)
	}
	node := &plan.Node{ExtraOptions: opt.String()}
	for i, name := range cols {
		node.ProjectList = append(node.ProjectList, &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: int32(i), Name: name}}})
	}
	node.ProjectList = append(node.ProjectList, &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0, Name: "c.id"}}})
	return node
}

func TestProductL2Ignored(t *testing.T) {
	c := &Compile{}

	ig, err := c.productL2Ignored(l2Node(t, nil, ""))
	require.NoError(t, err)
	require.Nil(t, ig, "a metric alone names no index")

	cfg := &vectorindex.IndexTableConfig{DbName: "db", IgnoredTable: "ign", MaxBadRecords: 4, PKey: "id",
		PKeyType: int32(types.T_int64), KeyPartType: int32(types.T_array_float32)}
	ig, err = c.productL2Ignored(l2Node(t, cfg, `{"quantization":"float16"}`, "src.v", "src.id"))
	require.NoError(t, err)
	require.Equal(t, int32(1), ig.Pk, "the build names the key column after the source column")
	require.Equal(t, "ign", ig.Table)
	require.Equal(t, int64(4), ig.Limit)
	require.False(t, ig.Serial)
	require.Equal(t, types.T_array_float16, ig.Storage.Entry)

	ig, err = c.productL2Ignored(l2Node(t, cfg, `{}`, catalog.SystemSI_IVFFLAT_TblCol_Entries_pk, "src1"))
	require.NoError(t, err)
	require.Equal(t, int32(0), ig.Pk, "the sync names it after the entries column")

	// a quoted key name may hold dots
	cfg.PKey = "a.id"
	for _, tc := range []struct {
		names []string
		pk    int32
	}{
		{[]string{"v", "a.id"}, 1},
		{[]string{"src.v", "src.b.a.id", "src.a.id"}, 2},
		{[]string{"b.a.id", "a.id"}, 1},
		{[]string{"src.b.a.id"}, -1},
	} {
		ig, err = c.productL2Ignored(l2Node(t, cfg, `{}`, tc.names...))
		require.NoError(t, err)
		require.Equal(t, tc.pk, ig.Pk, "%v", tc.names)
	}
	cfg.PKey = "id"

	cfg.PKey, cfg.IgnoredTable = catalog.CPrimaryKeyColName, ""
	ig, err = c.productL2Ignored(l2Node(t, cfg, `{}`, "src."+catalog.CPrimaryKeyColName))
	require.NoError(t, err)
	require.True(t, ig.Serial)
	require.Empty(t, ig.Table, "an index without an ignored table fails on such a row")

	cfg.PKey, cfg.PKeyType, cfg.PKeyEnum = "e", int32(types.T_enum), "a,b"
	ig, err = c.productL2Ignored(l2Node(t, cfg, `{}`, "src.e"))
	require.NoError(t, err)
	require.Equal(t, int32(0), ig.Pk, "an ENUM key is recorded")
	require.True(t, ig.Enum)
	require.Equal(t, "a,b", ig.EnumValues)

	_, err = c.productL2Ignored(&plan.Node{ExtraOptions: "{"})
	require.Error(t, err)
	_, err = c.productL2Ignored(&plan.Node{ExtraOptions: tree.CentroidxOption{OpType: "x", Params: "{", Config: "{}"}.String()})
	require.Error(t, err)
	_, err = c.productL2Ignored(&plan.Node{ExtraOptions: tree.CentroidxOption{OpType: "x", Params: "{}", Config: "{"}.String()})
	require.Error(t, err)
}
