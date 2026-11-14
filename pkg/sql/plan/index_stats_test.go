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

package plan

import (
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func indexStatsTestContext(t *testing.T) *MockCompilerContext {
	t.Helper()
	mock := NewMockCompilerContext(false, newPlanTestProcess(t))
	mock.objects["stats_source"] = &pbplan.ObjectRef{Db: 10, Obj: 20, SchemaName: "db", ObjName: "stats_source"}
	source := &pbplan.TableDef{Name: "stats_source", DbName: "db", TblId: 20, DbId: 10, Version: 7,
		TableType: catalog.SystemOrdinaryRel, Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "id"}}
	for _, kind := range []string{"metadata", "centroids", "entries"} {
		source.Indexes = append(source.Indexes, &pbplan.IndexDef{IndexName: "idx", IndexAlgo: "ivfflat",
			IndexAlgoTableType: kind, IndexTableName: "hidden_" + kind, Parts: []string{"embedding"},
			IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops"}`})
	}
	mock.tables["stats_source"] = source
	return mock
}

func TestMoIndexStatsPlannerDispatchAndSourceDependency(t *testing.T) {
	mock := indexStatsTestContext(t)
	stmt, err := parsers.ParseOne(mock.GetContext(), dialect.MYSQL, "SELECT * FROM mo_index_stats('db','stats_source','idx') AS s", 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(mock, stmt, false, nil)
	require.NoError(t, err)
	var scan *pbplan.Node
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType == pbplan.Node_FUNCTION_SCAN {
			scan = node
		}
	}
	require.NotNil(t, scan)
	require.Equal(t, "stats_source", scan.ObjRef.ObjName)
	require.Len(t, scan.TableDef.Cols, 4)
	for i, name := range []string{"index_type", "row_type", "object_id", "metrics"} {
		require.Equal(t, name, scan.TableDef.Cols[i].Name)
	}
	require.Equal(t, int32(types.T_json), scan.TableDef.Cols[3].Typ.Id)
	var cfg indexstats.Config
	require.NoError(t, json.Unmarshal(scan.TableDef.TblFunc.Param, &cfg))
	require.Equal(t, "hidden_metadata", cfg.Metadata)
	require.Equal(t, "embedding", cfg.Column)
	require.Equal(t, "vector_l2_ops", cfg.OpType)
	require.Equal(t, int64(2), cfg.Lists)
	require.Equal(t, uint64(20), cfg.TableID)
	require.Equal(t, "none", cfg.Quantization)
	require.Len(t, cfg.Tables, 3)
	require.Equal(t, "2", cfg.Options["lists"])

	builder := NewQueryBuilder(pbplan.Query_SELECT, mock, true, true)
	_, err = builder.buildIndexStats(nil, NewBindContext(builder, nil), []*pbplan.Expr{
		makePlan2StringConstExprWithType("db"), makePlan2StringConstExprWithType("stats_source"), makePlan2StringConstExprWithType("idx"),
	})
	require.NoError(t, err)
	require.Len(t, builder.qry.CatalogDependencies, 1)
	require.Equal(t, int64(7), builder.qry.CatalogDependencies[0].Server)
}

func TestMoIndexStatsPlannerRejectsInvalidOrUnsupportedTargets(t *testing.T) {
	for _, tc := range []struct{ name, index, algo, want string }{
		{"missing", "missing", "ivfflat", "does not exist"},
		{"hnsw", "idx", "hnsw", "not supported"},
		{"spatial", "idx", "rtree", "not supported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := indexStatsTestContext(t)
			for _, idx := range mock.tables["stats_source"].Indexes {
				idx.IndexAlgo = tc.algo
			}
			builder := NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
			_, err := builder.buildIndexStats(nil, NewBindContext(builder, nil), []*pbplan.Expr{
				makePlan2StringConstExprWithType("db"), makePlan2StringConstExprWithType("stats_source"), makePlan2StringConstExprWithType(tc.index),
			})
			require.ErrorContains(t, err, tc.want)
		})
	}
	mock := indexStatsTestContext(t)
	builder := NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
	_, err := builder.buildIndexStats(nil, NewBindContext(builder, nil), nil)
	require.ErrorContains(t, err, "requires 3 arguments")
	_, err = builder.buildIndexStats(nil, NewBindContext(builder, nil), []*pbplan.Expr{nil, nil, nil})
	require.ErrorContains(t, err, "non-empty string literals")
}

func TestMoIndexStatsFulltextPlanner(t *testing.T) {
	for _, tc := range []struct {
		name, algo, params, parser string
		version                    int64
		async                      bool
	}{
		{"classic_defaults", "fulltext", "", "ngram", 1, false},
		{"classic_async", "fulltext", `{"parser":"gojieba","async":"true"}`, "gojieba", 1, true},
		{"v2", "fulltext2", `{"parser":"ngram"}`, "ngram", 2, true},
		{"legacy_v2", "fulltext", `{"version":"2","parser":"json"}`, "json", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := indexStatsTestContext(t)
			kinds := []string{""}
			if tc.version == 2 {
				kinds = []string{catalog.FullText2Index_TblType_Metadata, catalog.FullText2Index_TblType_Storage}
			}
			mock.tables["stats_source"].Indexes = nil
			for _, kind := range kinds {
				mock.tables["stats_source"].Indexes = append(mock.tables["stats_source"].Indexes, &pbplan.IndexDef{
					IndexName: "idx", IndexAlgo: tc.algo, IndexAlgoTableType: kind, IndexTableName: "hidden_" + kind,
					Parts: []string{"body", "title"}, IndexAlgoParams: tc.params,
				})
			}
			builder := NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
			_, err := builder.buildIndexStats(nil, NewBindContext(builder, nil), []*pbplan.Expr{
				makePlan2StringConstExprWithType("db"), makePlan2StringConstExprWithType("stats_source"), makePlan2StringConstExprWithType("idx"),
			})
			require.NoError(t, err)
			var cfg indexstats.Config
			for _, node := range builder.qry.Nodes {
				if node.NodeType == pbplan.Node_FUNCTION_SCAN {
					require.NoError(t, json.Unmarshal(node.TableDef.TblFunc.Param, &cfg))
				}
			}
			require.Equal(t, tc.version, cfg.FullTextVersion)
			require.Equal(t, tc.async, cfg.Async)
			require.Equal(t, tc.parser, cfg.Parser)
			require.Equal(t, []string{"body", "title"}, cfg.Columns)
			require.NotEmpty(t, cfg.Entries)
		})
	}
}
