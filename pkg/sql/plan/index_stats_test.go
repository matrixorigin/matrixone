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
	"context"
	"encoding/json"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type indexStatsTestPlugin struct {
	indexplugin.AlgoPlugin
	name  string
	hooks planplugin.Hooks
}

func (p indexStatsTestPlugin) Algo() string           { return p.name }
func (p indexStatsTestPlugin) Plan() planplugin.Hooks { return p.hooks }

type indexStatsTestHooks struct {
	planplugin.Hooks
	configure func(context.Context, *pbplan.TableDef, []*pbplan.IndexDef, *indexstats.Config) error
}

func (h *indexStatsTestHooks) ReadIndexStats(*sqlexec.SqlProcess, indexstats.Config) (indexstats.Rows, error) {
	return indexstats.SummaryRow("custom", map[string]any{"plugin": true}), nil
}

func (h *indexStatsTestHooks) ConfigureIndexStats(ctx context.Context, source *pbplan.TableDef, defs []*pbplan.IndexDef, cfg *indexstats.Config) error {
	return h.configure(ctx, source, defs, cfg)
}

var indexStatsTestPluginID atomic.Uint64

func TestMoIndexStatsPlannerUsesRegisteredCapability(t *testing.T) {
	base, ok := indexplugin.Get("ivfflat")
	require.True(t, ok)
	owner, ok := base.Plan().(planplugin.IndexStatsHooks)
	require.True(t, ok)
	mock := indexStatsTestContext(t)
	source := mock.tables["stats_source"]
	name := "test_index_stats_" + strconv.FormatUint(indexStatsTestPluginID.Add(1), 10)
	for _, idx := range source.Indexes {
		idx.IndexAlgo = name
	}
	calls := 0
	hooks := &indexStatsTestHooks{Hooks: base.Plan()}
	hooks.configure = func(ctx context.Context, gotSource *pbplan.TableDef, defs []*pbplan.IndexDef, cfg *indexstats.Config) error {
		calls++
		require.Equal(t, source.TblId, gotSource.TblId)
		require.Equal(t, source.Name, gotSource.Name)
		require.Equal(t, gotSource.Indexes, defs)
		return owner.ConfigureIndexStats(ctx, gotSource, defs, cfg)
	}
	indexplugin.Register(indexStatsTestPlugin{AlgoPlugin: base, name: name, hooks: hooks})
	build := func() (*QueryBuilder, int32, error) {
		builder := NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
		node, err := builder.buildIndexStats(nil, NewBindContext(builder, nil), []*pbplan.Expr{
			makePlan2StringConstExprWithType("db"), makePlan2StringConstExprWithType("stats_source"), makePlan2StringConstExprWithType("idx"),
		})
		return builder, node, err
	}
	builder, node, err := build()
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	var cfg indexstats.Config
	require.NoError(t, json.Unmarshal(builder.qry.Nodes[node].TableDef.TblFunc.Param, &cfg))
	require.Equal(t, name, cfg.Algorithm)
	execOwner, err := indexplugin.IndexStats(builder.GetContext(), cfg.Algorithm)
	require.NoError(t, err)
	rows, err := execOwner.ReadIndexStats(sqlexec.NewSqlProcess(mock.GetProcess()), cfg)
	require.NoError(t, err)
	require.Equal(t, "custom", rows.IndexType)
	require.True(t, rows.At(0).Metrics["plugin"].(bool))
	_, err = indexplugin.IndexStats(builder.GetContext(), "unsupported_diagnostic")
	require.ErrorContains(t, err, "not supported")
	require.Equal(t, "hidden_entries", cfg.Entries)
	require.Equal(t, "id", cfg.PrimaryKey)

	wantErr := moerr.NewInvalidInput(mock.GetContext(), "owner rejected diagnostic configuration")
	hooks.configure = func(context.Context, *pbplan.TableDef, []*pbplan.IndexDef, *indexstats.Config) error { return wantErr }
	_, _, err = build()
	require.ErrorIs(t, err, wantErr)
}

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
	require.ErrorContains(t, err, "requires 3 or 4 arguments")
	_, err = builder.buildIndexStats(nil, NewBindContext(builder, nil), []*pbplan.Expr{nil, nil, nil})
	require.ErrorContains(t, err, "non-empty string literals")
}

func TestMoIndexStatsPlannerDeepOptions(t *testing.T) {
	for _, tc := range []struct {
		options string
		want    *indexstats.AnalyzeOptions
	}{
		{"", nil},
		{"{}", &indexstats.AnalyzeOptions{SampleCount: 100, RecallTarget: .95}},
		{`{"sample_count":32,"recall_target":0.9,"retrain":true}`, &indexstats.AnalyzeOptions{SampleCount: 32, RecallTarget: .9, Retrain: true}},
	} {
		t.Run(tc.options, func(t *testing.T) {
			mock := indexStatsTestContext(t)
			builder := NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
			args := []*pbplan.Expr{makePlan2StringConstExprWithType("db"), makePlan2StringConstExprWithType("stats_source"), makePlan2StringConstExprWithType("idx")}
			if tc.options != "" {
				args = append(args, makePlan2StringConstExprWithType(tc.options))
			}
			node, err := builder.buildIndexStats(nil, NewBindContext(builder, nil), args)
			require.NoError(t, err)
			tf := builder.qry.Nodes[node].TableDef.TblFunc
			require.Equal(t, indexstats.FunctionName, tf.Name)
			var cfg indexstats.Config
			require.NoError(t, json.Unmarshal(tf.Param, &cfg))
			require.Equal(t, tc.want, cfg.Analyze)
		})
	}
	mock := indexStatsTestContext(t)
	builder := NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
	args := []*pbplan.Expr{makePlan2StringConstExprWithType("db"), makePlan2StringConstExprWithType("stats_source"), makePlan2StringConstExprWithType("idx"), nil}
	_, err := builder.buildIndexStats(nil, NewBindContext(builder, nil), args)
	require.ErrorContains(t, err, "JSON string literal")
	args[3] = makePlan2StringConstExprWithType("")
	_, err = builder.buildIndexStats(nil, NewBindContext(builder, nil), args)
	require.ErrorContains(t, err, "JSON object")
	_, err = builder.buildIndexStats(nil, NewBindContext(builder, nil), append(args, makePlan2StringConstExprWithType("{}")))
	require.ErrorContains(t, err, "requires 3 or 4 arguments")
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

func TestMoIndexStatsPlannerSourceBoundary(t *testing.T) {
	for _, kind := range []string{"temporary", "partitioned", "resolved"} {
		t.Run(kind, func(t *testing.T) {
			mock := indexStatsTestContext(t)
			source := mock.tables["stats_source"]
			switch kind {
			case "temporary":
				source.IsTemporary = true
			case "partitioned":
				source.Partition = &pbplan.Partition{}
			case "resolved":
				source.Name = "resolved_source"
				mock.objects["stats_source"].SchemaName = "resolved_db"
			}
			builder := NewQueryBuilder(pbplan.Query_SELECT, mock, false, true)
			node, err := builder.buildIndexStats(nil, NewBindContext(builder, nil), []*pbplan.Expr{makePlan2StringConstExprWithType("db"), makePlan2StringConstExprWithType("stats_source"), makePlan2StringConstExprWithType("idx")})
			if kind != "resolved" {
				require.ErrorContains(t, err, "non-temporary, non-partitioned")
				return
			}
			require.NoError(t, err)
			var cfg indexstats.Config
			require.NoError(t, json.Unmarshal(builder.qry.Nodes[node].TableDef.TblFunc.Param, &cfg))
			require.Equal(t, "resolved_db", cfg.Database)
			require.Equal(t, "resolved_source", cfg.Table)
		})
	}
}
