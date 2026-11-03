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
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBoundedDeleteIndexRuntimeFilterSQL(t *testing.T) {
	proc := newPlanTestProcess(t)
	mock := NewMockOptimizer(true, proc)
	ddl, err := runOneStmt(mock, t, `create table bounded_delete (
		id varchar(32) not null, expires_at datetime(6) not null,
		token_hash varchar(32) not null, session_id varchar(32),
		user_id varchar(32) not null, browser_hash varchar(32),
		primary key(expires_at,id), unique key uq_id(id),
		unique key uq_hash(token_hash), key idx_session(session_id),
		unique key uq_browser(browser_hash), key idx_user(user_id))`)
	require.NoError(t, err)
	create := ddl.GetDdl().GetCreateTable()
	tables := append([]*planpb.TableDef{create.TableDef}, create.IndexTables...)
	var targetName string
	for _, idx := range create.TableDef.Indexes {
		if idx.IndexName == "idx_user" {
			targetName = idx.IndexTableName
		}
	}
	require.NotEmpty(t, targetName)
	var targetID uint64
	for i, table := range tables {
		table.TblId = uint64(900000 + i)
		if table.Name == targetName {
			targetID = table.TblId
		}
	}
	require.NotZero(t, targetID)

	// Every run gets its own catalog and statistics: binding/remapping may
	// mutate them. The physical PK's NDV is deliberately stale, not a claim
	// that a unique index actually contains only 1,000 distinct keys.
	newContext := func() *updateIndexBuildTestContext {
		mock := NewMockOptimizer(true, proc)
		ctx := &updateIndexBuildTestContext{MockCompilerContext: &mock.ctxt, statsCache: NewStatsCache(), statsByID: make(map[uint64]*statspb.StatsInfo)}
		for i, original := range tables {
			table := proto.Clone(original).(*planpb.TableDef)
			table.Name2ColIndex = make(map[string]int32)
			table.TableType = catalog.SystemOrdinaryRel
			if i > 0 {
				table.TableType = catalog.SystemIndexRel
			}
			for _, idx := range table.Indexes {
				idx.TableExist = true
			}
			for j, col := range table.Cols {
				col.ColId = uint64(j)
				table.Name2ColIndex[col.Name] = int32(j)
			}
			if _, ok := table.Name2ColIndex[catalog.Row_ID]; !ok {
				pos := len(table.Cols)
				table.Name2ColIndex[catalog.Row_ID] = int32(pos)
				table.Cols = append(table.Cols, &planpb.ColDef{ColId: uint64(pos), Name: catalog.Row_ID, OriginName: catalog.Row_ID, Typ: planpb.Type{Id: int32(types.T_Rowid), Width: 16, NotNullable: true}, Hidden: true, Default: &planpb.Default{}})
			}
			ctx.tables[table.Name] = table
			ctx.objects[table.Name] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
			ctx.id2name[table.TblId] = table.Name
			stats := NewStatsInfo()
			stats.TableCnt, stats.BlockNumber = 97000, 12
			for _, col := range table.Cols {
				stats.NdvMap[col.Name], stats.SizeMap[col.Name] = 97000, 97000*32
			}
			if i == 0 {
				stats.NdvMap["user_id"], stats.NdvMap["browser_hash"], stats.NdvMap["expires_at"] = 3, 0, 1
			}
			if table.TblId == targetID {
				stats.NdvMap[table.Pkey.PkeyColName] = 1000
			}
			ctx.statsCache.Set(table.TblId, stats)
			ctx.statsByID[table.TblId] = stats
		}
		return ctx
	}

	parse := func(limit int) *tree.Delete {
		stmt, err := mysql.ParseOne(t.Context(), fmt.Sprintf("delete from bounded_delete limit %d", limit), 1)
		require.NoError(t, err)
		t.Cleanup(stmt.Free)
		return stmt.(*tree.Delete)
	}
	planWithBuilder := func(limit int, baseline bool) (*QueryBuilder, *planpb.Query, int32) {
		ctx := newContext()
		ndv := ctx.statsByID[targetID].NdvMap[ctx.tables[targetName].Pkey.PkeyColName]
		require.Equal(t, float64(1000), ndv)
		require.Less(t, float64(99)/ndv, 0.1)
		require.GreaterOrEqual(t, float64(100)/ndv, 0.1)
		builder := NewQueryBuilder(planpb.Query_DELETE, ctx, false, true)
		root, err := builder.bindDelete(ctx, parse(limit), NewBindContext(builder, nil))
		require.NoError(t, err)
		var targetTag int32
		for tag, registration := range builder.deleteIndexScanTables {
			if registration.tableID == targetID {
				require.Zero(t, targetTag, "idx_user registered more than once")
				targetTag = tag
			}
		}
		require.NotZero(t, targetTag, "binder must register the idx_user scan")
		if baseline {
			builder.deleteIndexScanTables = nil
		}
		builder.qry.Steps = append(builder.qry.Steps, root)
		builder.skipStats = false // The ordinary BuildPlan path enables stats after binding.
		query, err := builder.createQuery()
		require.NoError(t, err)
		return builder, query, targetTag
	}

	publicPlan, err := BuildPlan(newContext(), parse(99), false)
	require.NoError(t, err)
	publicScan, publicJoin, _ := boundedDeleteSQLTarget(t, publicPlan.GetQuery(), targetID)
	t.Run("BuildPlan", func(t *testing.T) {
		assertBoundedDeleteSQLPair(t, publicScan, publicJoin, 99)
	})

	builder, positive, tag := planWithBuilder(99, false)
	scan, join, live := boundedDeleteSQLTarget(t, positive, targetID)
	registration := builder.deleteIndexScanTables[tag]
	require.Same(t, scan, registration.rejectedScan, "ordinary generation must reject this scan")
	require.Same(t, join, registration.rejectedJoin, "rejection must belong to this live join")
	assertBoundedDeleteSQLPremises(t, positive, scan, join, live, 99)
	assertBoundedDeleteSQLPair(t, scan, join, 99)

	_, baseline, _ := planWithBuilder(99, true)
	baselineScan, baselineJoin, baselineLive := boundedDeleteSQLTarget(t, baseline, targetID)
	require.Empty(t, baselineScan.RuntimeFilterProbeList)
	require.Empty(t, baselineJoin.RuntimeFilterBuildList)
	assertBoundedDeleteSQLBaseline(t, baseline, baselineLive, positive, live, scan, join)

	require.Equal(t, scan.RuntimeFilterProbeList, publicScan.RuntimeFilterProbeList)
	require.Equal(t, join.RuntimeFilterBuildList, publicJoin.RuntimeFilterBuildList)

	controlBuilder, control, controlTag := planWithBuilder(100, false)
	controlScan, controlJoin, controlLive := boundedDeleteSQLTarget(t, control, targetID)
	controlRegistration := controlBuilder.deleteIndexScanTables[controlTag]
	require.Same(t, controlScan, controlRegistration.rejectedScan)
	require.Same(t, controlJoin, controlRegistration.rejectedJoin)
	assertBoundedDeleteSQLPremises(t, control, controlScan, controlJoin, controlLive, 100)
	require.Len(t, controlJoin.OnList, 1)
	require.True(t, exprStructuralEqual(join.OnList[0], controlJoin.OnList[0]), "the control must retain the same join key")
	require.Empty(t, controlScan.RuntimeFilterProbeList, "100/1000 reaches the cost boundary")
	require.Empty(t, controlJoin.RuntimeFilterBuildList, "100/1000 reaches the cost boundary")
}

// Traverse only executable nodes: the optimizer may retain discarded joins.
func boundedDeleteSQLTarget(t *testing.T, query *planpb.Query, targetID uint64) (*planpb.Node, *planpb.Node, map[int32]*planpb.Node) {
	t.Helper()
	require.Len(t, query.Steps, 1)
	live := make(map[int32]*planpb.Node)
	var scan, parent *planpb.Node
	var visit func(int32, *planpb.Node)
	visit = func(id int32, p *planpb.Node) {
		require.GreaterOrEqual(t, id, int32(0))
		require.Less(t, int(id), len(query.Nodes))
		if _, ok := live[id]; ok {
			return
		}
		n := query.Nodes[id]
		live[id] = n
		if n.NodeType == planpb.Node_TABLE_SCAN && n.TableDef != nil && n.TableDef.TblId == targetID {
			require.Nil(t, scan, "target scan must be unique")
			scan, parent = n, p
		}
		for _, child := range n.Children {
			visit(child, n)
		}
	}
	visit(query.Steps[0], nil)
	require.NotNil(t, scan, "idx_user scan must be live")
	require.NotNil(t, parent)
	return scan, parent, live
}

func assertBoundedDeleteSQLPremises(t *testing.T, query *planpb.Query, scan, join *planpb.Node, live map[int32]*planpb.Node, limit uint64) {
	t.Helper()
	require.Equal(t, planpb.Node_JOIN, join.NodeType)
	require.Equal(t, planpb.Node_INNER, join.JoinType)
	require.Len(t, join.Children, 2)
	require.Same(t, scan, query.Nodes[join.Children[0]])
	require.NotNil(t, join.Stats.HashmapStats)
	require.False(t, join.Stats.HashmapStats.Shuffle)
	require.False(t, join.Stats.HashmapStats.HashOnPK)
	require.GreaterOrEqual(t, join.Stats.HashmapStats.HashmapSize, float64(100))
	require.Equal(t, float64(97000), scan.Stats.TableCnt)
	require.Empty(t, scan.FilterList)
	require.Empty(t, scan.BlockFilterList)
	require.Nil(t, scan.Limit)
	require.Nil(t, scan.Offset)
	require.Empty(t, join.RuntimeFilterProbeList)
	require.Equal(t, ExecTypeAP_ONECN, GetExecType(query, false, false))
	var sourceBound, ordinaryLocalFilter bool
	for _, node := range live {
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.Name == "bounded_delete" {
			got, ok := getLiteralUint64(node.Limit)
			sourceBound = ok && got == limit
		}
		if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_RIGHT && len(node.Children) == 2 && len(node.RuntimeFilterBuildList) == 1 {
			probe := query.Nodes[node.Children[0]]
			ordinaryLocalFilter = ordinaryLocalFilter || probe.Stats.ForceOneCN && len(probe.RuntimeFilterProbeList) == 1 &&
				probe.RuntimeFilterProbeList[0].Tag == node.RuntimeFilterBuildList[0].Tag
		}
	}
	require.True(t, sourceBound, "the live source must retain the literal bound")
	require.True(t, ordinaryLocalFilter, "an existing RIGHT-join filter must supply local placement")
}

func assertBoundedDeleteSQLPair(t *testing.T, scan, join *planpb.Node, limit int32) {
	t.Helper()
	require.Len(t, scan.RuntimeFilterProbeList, 1, "late pass must add the index probe")
	require.Len(t, join.RuntimeFilterBuildList, 1, "late pass must add the join producer")
	probe, build := scan.RuntimeFilterProbeList[0], join.RuntimeFilterBuildList[0]
	require.NotZero(t, probe.Tag)
	require.Equal(t, probe.Tag, build.Tag)
	require.False(t, probe.NotOnPk)
	require.False(t, build.NotOnPk)
	require.GreaterOrEqual(t, build.UpperLimit, limit)
	require.NotNil(t, probe.Expr)
	col := probe.Expr.GetCol()
	require.NotNil(t, col)
	require.GreaterOrEqual(t, col.ColPos, int32(0))
	require.Less(t, int(col.ColPos), len(scan.TableDef.Cols))
	require.Equal(t, scan.TableDef.Pkey.PkeyColName, scan.TableDef.Cols[col.ColPos].Name)
	require.NotNil(t, build.BuildExpr)
	require.NotNil(t, build.BuildExpr.GetCol())
	require.Equal(t, int32(-1), build.BuildExpr.GetCol().RelPos)
	require.Zero(t, build.BuildExpr.GetCol().ColPos)
	require.Len(t, join.OnList, 1)
	equality := join.OnList[0].GetF()
	require.NotNil(t, equality)
	require.Len(t, equality.Args, 2)
	require.True(t, exprStructuralEqual(probe.Expr, equality.Args[0]), "probe must use the remapped join key")
	require.NotNil(t, equality.Args[1].GetF(), "the producer key is the evaluated index-key expression")
	require.True(t, isSameColumnType(equality.Args[1].Typ, build.BuildExpr.Typ))
	require.NotNil(t, build.ProbeType)
	require.True(t, isSameColumnType(probe.Expr.Typ, *build.ProbeType))
	require.True(t, isSameColumnType(probe.Expr.Typ, build.BuildExpr.Typ))
	require.Equal(t, planpb.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1, build.KeyEncoding)
	if build.Expr != nil {
		require.True(t, exprStructuralEqual(build.Expr, build.BuildExpr), "legacy expression must match the producer")
	}
}

// Compare all existing filters and their links while allowing generated message
// tags to change. The added pair is the only extra filter on the live tree.
func assertBoundedDeleteSQLBaseline(t *testing.T, baseline *planpb.Query, old map[int32]*planpb.Node, actual *planpb.Query, current map[int32]*planpb.Node, targetScan, targetJoin *planpb.Node) {
	t.Helper()
	require.Equal(t, GetExecType(baseline, false, false), GetExecType(actual, false, false))
	require.Len(t, current, len(old))
	tags := make(map[int32]int32)
	reverseTags := make(map[int32]int32)
	filterCount := 0
	compare := func(before, after []*planpb.RuntimeFilterSpec) {
		require.Len(t, after, len(before))
		for i, prior := range before {
			got := after[i]
			if mapped, ok := tags[prior.Tag]; ok {
				require.Equal(t, mapped, got.Tag)
			} else {
				if previous, ok := reverseTags[got.Tag]; ok {
					require.Equal(t, previous, prior.Tag)
				}
				tags[prior.Tag], reverseTags[got.Tag] = got.Tag, prior.Tag
			}
			left, right := proto.Clone(prior).(*planpb.RuntimeFilterSpec), proto.Clone(got).(*planpb.RuntimeFilterSpec)
			left.Tag, right.Tag = 0, 0
			require.Equal(t, left, right)
			filterCount++
		}
	}
	for id, before := range old {
		after, ok := current[id]
		require.True(t, ok, "baseline node %d must remain live", id)
		require.Equal(t, before.NodeType, after.NodeType)
		require.Equal(t, before.Stats, after.Stats, "node %d stats and ForceOneCN must not change", id)
		build, probe := after.RuntimeFilterBuildList, after.RuntimeFilterProbeList
		if after == targetJoin {
			build = nil
		}
		if after == targetScan {
			probe = nil
		}
		compare(before.RuntimeFilterBuildList, build)
		compare(before.RuntimeFilterProbeList, probe)
	}
	require.Greater(t, filterCount, 0, "baseline must retain ordinary filters")
}
