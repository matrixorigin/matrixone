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

package explain

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planpkg "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func sqlJSONWireIndexNode(t *testing.T, node *plan.Node) sqlJSONNode {
	t.Helper()
	query := &plan.Query{StmtType: plan.Query_SELECT, Steps: []int32{0}, Nodes: []*plan.Node{node}}
	wire, err := query.Marshal()
	require.NoError(t, err)
	var remote plan.Query
	require.NoError(t, remote.Unmarshal(wire))
	raw, err := BuildSQLJSONPlan(t.Context(), &remote)
	require.NoError(t, err)
	var decoded sqlJSONPlan
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Len(t, decoded.MatrixOne.Nodes, 1)
	return decoded.MatrixOne.Nodes[0]
}

func TestSQLJSONIndexSearchWirePayload(t *testing.T) {
	opts := []byte(`{"initial_probe_count":7,"bucket_expand_step":3,"threads_search":2}`)
	node := sqlJSONWireIndexNode(t, &plan.Node{
		NodeId: 0, NodeType: plan.Node_INDEX_SEARCH_SCAN,
		IndexSearchScan: &plan.IndexSearchScan{
			SourceTable:      &plan.ObjectRef{SchemaName: "db", ObjName: "items"},
			Index:            &plan.IndexDef{IndexName: "idx_embedding", IndexAlgo: "ivfflat"},
			DistanceFunction: "l2_distance", Direction: plan.OrderBySpec_DESC,
			QueryPayload:        &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[0.1,0.2]"}}}},
			CandidateLimit:      planpkg.MakePlan2Uint64ConstExprWithType(12),
			PostFilterOverFetch: true, AlgoOptions: opts,
			AlgoExprNames:   []string{"first_round_limit"},
			AlgoExprs:       []*plan.Expr{planpkg.MakePlan2Uint64ConstExprWithType(4)},
			IncludedColumns: []string{"id", "embedding"},
		},
	})
	require.Equal(t, "Vector Index Scan", node.Operator)
	require.Equal(t, "db.items", node.TableName)
	for _, want := range []string{"index=idx_embedding", "metric=l2_distance", "direction=DESC",
		"post_filter_over_fetch=true", "algo_setting[0]=NProbe: 7", "query_payload=[0.1,0.2]",
		"candidate_limit=12", `algo_expr["first_round_limit"]=4`, "included_column[1]=embedding"} {
		require.Contains(t, node.Expressions, want)
	}
	for _, value := range node.Expressions {
		if encoded, ok := strings.CutPrefix(value, "algo_options_base64="); ok {
			decoded, err := base64.StdEncoding.DecodeString(encoded)
			require.NoError(t, err)
			require.Equal(t, opts, decoded)
			return
		}
	}
	t.Fatal("missing algorithm options")
}

func TestSQLJSONFulltextIndexSearchWireMetadata(t *testing.T) {
	node := fulltextSearchScanNode()
	node.IndexSearchScan.SourceTable = &plan.ObjectRef{SchemaName: "db", ObjName: "documents"}
	node.IndexSearchScan.QueryPayload = planpkg.MakePlan2StringConstExprWithType("needle")
	decoded := sqlJSONWireIndexNode(t, node)
	require.Equal(t, "Fulltext Index Scan", decoded.Operator)
	require.Equal(t, "db.documents", decoded.TableName)
	require.Contains(t, decoded.Expressions, "index=ft_body")
	require.Contains(t, strings.Join(decoded.Expressions, "\n"), "needle")
	require.NotContains(t, strings.Join(decoded.Expressions, "\n"), "metric=")
}

func TestSQLJSONIndexSearchOpaqueOptionsAndNamedExpressions(t *testing.T) {
	opts := []byte{0, 255, 'x'}
	decoded := sqlJSONWireIndexNode(t, &plan.Node{NodeType: plan.Node_INDEX_SEARCH_SCAN,
		IndexSearchScan: &plan.IndexSearchScan{
			Index:       &plan.IndexDef{IndexName: "future_index", IndexAlgo: "future_plugin"},
			AlgoOptions: opts, AlgoExprNames: []string{"a]\n=2"}, AlgoExprs: []*plan.Expr{sqlJSONTestInt32(1)},
		}})
	require.Contains(t, decoded.Expressions, "algo_options_base64="+base64.StdEncoding.EncodeToString(opts))
	require.Contains(t, decoded.Expressions, `algo_expr["a]\n=2"]=1`)
}

func TestSQLJSONIndexSearchRejectsMalformedAlgorithmMetadata(t *testing.T) {
	for _, tc := range []struct {
		name  string
		names []string
		exprs []*plan.Expr
	}{
		{"missing name", nil, []*plan.Expr{sqlJSONTestInt32(1)}},
		{"missing expression", []string{"limit"}, nil},
		{"empty name", []string{""}, []*plan.Expr{sqlJSONTestInt32(1)}},
		{"nil expression", []string{"limit"}, []*plan.Expr{nil}},
		{"duplicate name", []string{"limit", "limit"}, []*plan.Expr{sqlJSONTestInt32(1), sqlJSONTestInt32(2)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlJSONIndexSearchExpressions(context.Background(), &plan.IndexSearchScan{
				Index: &plan.IndexDef{IndexName: "idx"}, AlgoExprNames: tc.names, AlgoExprs: tc.exprs,
			}, &ExplainOptions{Format: EXPLAIN_FORMAT_TEXT})
			require.Error(t, err)
		})
	}
	_, err := sqlJSONIndexSearchExpressions(t.Context(), &plan.IndexSearchScan{
		Index: &plan.IndexDef{IndexName: "idx", IndexAlgo: "ivfflat"}, AlgoOptions: []byte("{"),
	}, &ExplainOptions{Format: EXPLAIN_FORMAT_TEXT})
	require.Error(t, err)
}
