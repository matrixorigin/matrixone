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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	ft2plan "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestFullTextBooleanCompositionPreservesScan(t *testing.T) {
	for _, ft2 := range []bool{false, true} {
		for _, op := range []string{"or", "not"} {
			t.Run(op+map[bool]string{false: "/classic", true: "/fulltext2"}[ft2], func(t *testing.T) {
				builder, scanID, projID := buildWrappedMatchGuardPlan(t, true)
				scan := builder.qry.Nodes[scanID]
				if ft2 {
					idx := scan.TableDef.Indexes[0]
					idx.IndexAlgo = catalog.MoIndexFullText2Algo.ToString()
					idx.IndexAlgoTableType = catalog.FullText2Index_TblType_Storage
					scan.TableDef.Indexes = append(scan.TableDef.Indexes, &planpb.IndexDef{IndexName: idx.IndexName, IndexAlgo: idx.IndexAlgo, IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: "__mo_fts_meta_ft", TableExist: true})
				}
				match := DeepCopyExpr(builder.qry.Nodes[projID].ProjectList[0])
				args := []*planpb.Expr{match}
				if op == "or" {
					args = append(args, scan.FilterList[0])
				}
				pred, err := BindFuncExprImplByPlanExpr(builder.GetContext(), op, args)
				require.NoError(t, err)
				scan.FilterList = []*planpb.Expr{pred}
				builder.prepareSpecialIndexGuards(projID)
				newID, err := builder.applyIndices(projID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
				require.NoError(t, err)
				builder.qry.Steps = []int32{newID}
				require.Zero(t, countReachableFullTextMatches(builder.qry), "all admitted MATCH leaves must resolve")
				require.Len(t, collectFullTextFunctionScans(builder, newID), 1)
				joins, filters := 0, 0
				var visit func(int32)
				visit = func(id int32) {
					n := builder.qry.Nodes[id]
					if n.NodeType == planpb.Node_JOIN {
						joins++
						require.Equal(t, planpb.Node_LEFT, n.JoinType, "scalar/complement rows must survive")
					}
					if n.NodeType == planpb.Node_FILTER {
						filters++
						require.NotEmpty(t, n.FilterList)
					}
					for _, c := range n.Children {
						visit(c)
					}
				}
				visit(newID)
				require.Equal(t, 1, joins)
				require.Equal(t, 1, filters)
				require.Empty(t, scan.FilterList)
			})
		}
	}
}

func TestFullTextBooleanPublicPlanner(t *testing.T) {
	for _, query := range []string{
		"select id from ft where match(title, body) against('hello') or base_id='b1'",
		"select id from ft where not match(title, body) against('hello')",
		"select id, match(title, body) against('hello') from ft where match(title, body) against('hello') or true",
		"select count(*) from ft where match(title, body) against('hello') or base_id='b1'",
		"select id from ft where match(title, body) against('hello') or base_id='b1' limit 2 offset 1",
	} {
		t.Run(query, func(t *testing.T) {
			cc := newFullTextJoinMockCompilerContext(t)
			def := makeFullTextJoinTestTableDef("ft", true)
			cc.tables["ft"] = def
			cc.objects["ft"] = &planpb.ObjectRef{SchemaName: "test", ObjName: "ft"}
			builder := NewQueryBuilder(planpb.Query_SELECT, cc, false, true)
			registerFullTextJoinRegularIndexTable(builder, def.Indexes[0].IndexTableName)
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, query, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(cc, stmt, false)
			require.NoError(t, err)
			require.Zero(t, countReachableFullTextMatches(p.GetQuery()), "public planner must admit preserving composition")
			left := 0
			for _, n := range p.GetQuery().Nodes {
				if n.NodeType == planpb.Node_JOIN && n.JoinType == planpb.Node_LEFT {
					left++
				}
			}
			require.Greater(t, left, 0, "public route must preserve scalar/complement rows")
		})
	}
}

// These fixtures own only typed plans and tiny expression batches. The public
// cases independently exercise bind/optimize/index/remap; reader/BVT tests own
// document visibility, uniqueness and SQL result-set evidence.
func booleanFullTextFixture(t *testing.T, ft2 bool) (*QueryBuilder, int32, int32) {
	t.Helper()
	builder, scanID, projectID := buildWrappedMatchGuardPlan(t, true)
	if ft2 {
		scan := builder.qry.Nodes[scanID]
		secondary := scan.TableDef.Indexes[1]
		convertFullTextJoinTestToFulltext2(builder, scan)
		scan.TableDef.Indexes = append(scan.TableDef.Indexes, secondary)
	}
	return builder, scanID, projectID
}

func booleanFullTextBind(t *testing.T, name string, args ...*planpb.Expr) *planpb.Expr {
	t.Helper()
	expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
	require.NoError(t, err)
	return expr
}

func booleanFullTextRewrite(t *testing.T, builder *QueryBuilder, root int32) int32 {
	t.Helper()
	builder.prepareSpecialIndexGuards(root)
	newRoot, err := builder.applyIndices(root, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	builder.qry.Steps = []int32{newRoot}
	require.Zero(t, countReachableFullTextMatches(builder.qry))
	return newRoot
}

func booleanFullTextNodes(query *planpb.Query, root int32) []*planpb.Node {
	var nodes []*planpb.Node
	seen := map[int32]bool{}
	var visit func(int32)
	visit = func(id int32) {
		if seen[id] {
			return
		}
		seen[id] = true
		n := query.Nodes[id]
		nodes = append(nodes, n)
		for _, child := range n.Children {
			visit(child)
		}
	}
	visit(root)
	return nodes
}

func booleanFullTextScoreProject(t *testing.T, builder *QueryBuilder, root int32) *planpb.Node {
	t.Helper()
	var project *planpb.Node
	for _, n := range booleanFullTextNodes(builder.qry, root) {
		if builder.isFullTextBooleanScoreProject(n) {
			require.Nil(t, project, "one normalization boundary per base scan")
			project = n
		}
	}
	require.NotNil(t, project)
	return project
}

func requireBooleanFullTextCompleteStreams(t *testing.T, query *planpb.Query, root int32, count int) {
	t.Helper()
	streams := 0
	for _, n := range booleanFullTextNodes(query, root) {
		if !isFullTextSearchScan(n) && !isFulltext2SearchScan(n) {
			continue
		}
		streams++
		spec := n.IndexSearchScan
		require.Nil(t, n.Limit)
		require.Nil(t, n.Offset)
		require.Nil(t, spec.CandidateLimit)
		require.Nil(t, spec.DistanceRange)
		require.Empty(t, spec.PreFilters)
		require.Empty(t, spec.IncludedColumns)
		require.Empty(t, spec.AlgoExprs, "no zero-relevance threshold guard")
		require.Empty(t, spec.AlgoExprNames)
		require.False(t, spec.PostFilterOverFetch)
		require.Empty(t, n.FilterList)
		require.Empty(t, n.BlockFilterList)
		require.Empty(t, n.OrderBy, "no implicit relevance Top-K")
		require.Empty(t, n.RuntimeFilterProbeList)
		require.Empty(t, n.RuntimeFilterBuildList)
		require.Len(t, n.TableDef.Cols, 2, "never use the covered INCLUDE path")
		if isFulltext2SearchScan(n) {
			opts, err := ft2plan.DecodeScanOptions(spec.AlgoOptions)
			require.NoError(t, err)
			require.Empty(t, opts.ScoreRange)
			require.Empty(t, opts.IncludePreds)
		}
	}
	require.Equal(t, count, streams)
}

func TestFullTextBooleanDeduplicatesExactLeaves(t *testing.T) {
	for _, ft2 := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			kind string
			want int
		}{
			{"same leaf", "same", 1},
			{"different pattern", "pattern", 2},
			{"different mode", "mode", 2},
			{"different indexed parts", "parts", 2},
			{"different parameter positions", "params", 2},
		} {
			t.Run(tc.name+map[bool]string{false: "/classic", true: "/fulltext2"}[ft2], func(t *testing.T) {
				builder, scanID, projectID := booleanFullTextFixture(t, ft2)
				scan := builder.qry.Nodes[scanID]
				first := DeepCopyExpr(builder.qry.Nodes[projectID].ProjectList[0])
				second := DeepCopyExpr(first)
				switch tc.kind {
				case "pattern":
					second.GetF().Args[0] = makePlan2StringConstExprWithType("world", false)
				case "mode":
					second.GetF().Args[1] = makePlan2Int64ConstExprWithType(int64(tree.FULLTEXT_BOOLEAN))
				case "parts":
					idx := DeepCopyIndexDef(scan.TableDef.Indexes[0])
					idx.IndexName += "_title"
					idx.IndexTableName += "_title"
					idx.Parts = []string{"title"}
					scan.TableDef.Indexes = append(scan.TableDef.Indexes, idx)
					registerFullTextJoinRegularIndexTable(builder, idx.IndexTableName)
					if ft2 {
						meta := DeepCopyIndexDef(idx)
						meta.IndexAlgoTableType = catalog.FullText2Index_TblType_Metadata
						meta.IndexTableName += "_meta"
						scan.TableDef.Indexes = append(scan.TableDef.Indexes, meta)
					}
					second.GetF().Args = second.GetF().Args[:3]
				case "params":
					first.GetF().Args[0] = ftScoreParamExpr(0)
					second.GetF().Args[0] = ftScoreParamExpr(1)
				}
				// One identical projected copy must reuse the WHERE slot, too.
				builder.qry.Nodes[projectID].ProjectList = []*planpb.Expr{DeepCopyExpr(first), DeepCopyExpr(second)}
				scan.FilterList = []*planpb.Expr{booleanFullTextBind(t, "or", first, second)}
				root := booleanFullTextRewrite(t, builder, projectID)
				requireBooleanFullTextCompleteStreams(t, builder.qry, root, tc.want)
				normalized := booleanFullTextScoreProject(t, builder, root)
				require.Len(t, normalized.ProjectList, len(scan.TableDef.Cols)+2*tc.want)
				for _, expr := range builder.qry.Nodes[projectID].ProjectList {
					require.NotNil(t, expr.GetCol())
					require.Equal(t, normalized.BindingTags[0], expr.GetCol().RelPos)
				}
				if tc.want == 1 {
					require.Equal(t, builder.qry.Nodes[projectID].ProjectList[0], builder.qry.Nodes[projectID].ProjectList[1])
				} else {
					require.NotEqual(t, builder.qry.Nodes[projectID].ProjectList[0].GetCol().ColPos, builder.qry.Nodes[projectID].ProjectList[1].GetCol().ColPos)
				}
			})
		}
	}
}

func TestFullTextBooleanRefusesWholeUnsupportedPredicate(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*planpb.Expr, *planpb.Node)
	}{
		{"uncovered column", func(e *planpb.Expr, n *planpb.Node) { e.GetF().Args[2] = ftjColExpr(n.TableDef, n.BindingTags[0], 1) }},
		{"partial index coverage", func(e *planpb.Expr, _ *planpb.Node) { e.GetF().Args = e.GetF().Args[:3] }},
		{"cross binding", func(e *planpb.Expr, _ *planpb.Node) { e.GetF().Args[3].GetCol().RelPos += 100 }},
		{"internal JSON probe", func(e *planpb.Expr, _ *planpb.Node) {
			e.GetF().Args[1] = makePlan2Int64ConstExprWithType(fulltext2.JSONProbeMode)
		}},
		{"dynamic mode", func(e *planpb.Expr, _ *planpb.Node) { e.GetF().Args[1] = ftScoreParamExpr(1) }},
		{"unowned scan pagination", func(_ *planpb.Expr, n *planpb.Node) { n.Limit = makePlan2Uint64ConstExprWithType(1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, scanID, projectID := booleanFullTextFixture(t, false)
			scan := builder.qry.Nodes[scanID]
			first := DeepCopyExpr(builder.qry.Nodes[projectID].ProjectList[0])
			second := DeepCopyExpr(first)
			tc.edit(second, scan)
			scan.FilterList = []*planpb.Expr{booleanFullTextBind(t, "or", first, second)}
			before := DeepCopyNode(scan)
			nodeCount := len(builder.qry.Nodes)
			remap := map[[2]int32]*planpb.Expr{}
			_, err := builder.applyFullTextBooleanComposition(scanID, scan, remap)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			require.Equal(t, before.String(), scan.String(), "validation must precede every scan mutation")
			require.Len(t, builder.qry.Nodes, nodeCount, "valid first leaf must not publish a partial stream")
			require.Empty(t, builder.ftJoinServed)
			require.Empty(t, remap)
		})
	}
}

func TestFullTextBooleanProtectsEarlyIndexPaths(t *testing.T) {
	builder, scanID, projectID := booleanFullTextFixture(t, false)
	scan := builder.qry.Nodes[scanID]
	match := DeepCopyExpr(builder.qry.Nodes[projectID].ProjectList[0])
	// Removing the projected MATCH makes the Boolean filter the ONLY guard.
	builder.qry.Nodes[projectID].ProjectList = []*planpb.Expr{ftjColExpr(scan.TableDef, scan.BindingTags[0], 0)}
	composition := booleanFullTextBind(t, "or", match, scan.FilterList[0])
	scalar := DeepCopyExpr(scan.FilterList[0])
	scan.FilterList = []*planpb.Expr{composition, scalar}
	require.True(t, builder.scanHasMatchedFullTextFilter(scan))
	builder.prepareSpecialIndexGuards(projectID)
	require.True(t, builder.isScanProtected(scanID))
	optimized, err := builder.applyIndicesForFilters(scanID, scan, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
	require.NoError(t, err)
	require.Equal(t, scanID, optimized,
		"a real secondary index must not consume the protected scan")
	for _, algo := range []string{catalog.MoIndexHnswAlgo.ToString(), catalog.MoIndexIvfFlatAlgo.ToString()} {
		require.False(t, builder.vectorIndexSupportsContext(&vectorSortContext{scanNode: scan}, algo), algo)
	}
	root := booleanFullTextRewrite(t, builder, projectID)
	require.Len(t, scan.FilterList, 1)
	require.Equal(t, scalar, scan.FilterList[0], "only the independent scalar conjunct stays on the scan")
	requireBooleanFullTextCompleteStreams(t, builder.qry, root, 1)
	// Nearest legacy control: a positive driver still permits IVFFLAT's existing path.
	control, controlID, _ := booleanFullTextFixture(t, false)
	plain := control.qry.Nodes[controlID]
	plain.FilterList = []*planpb.Expr{makeFullTextMatchExpr("hello", 0, plain.TableDef, plain.BindingTags[0], []int32{2, 3})}
	require.False(t, scanHasFullTextBooleanComposition(plain))
	require.True(t, control.vectorIndexSupportsContext(&vectorSortContext{scanNode: plain}, catalog.MoIndexIvfFlatAlgo.ToString()))
}

func TestFullTextBooleanNumericTruthAndThreeValuedLogic(t *testing.T) {
	for _, predicate := range []string{"bare scan sibling", "bare upper filter", "or", "and", "not", "not and scalar"} {
		t.Run(predicate, func(t *testing.T) {
			builder, scanID, projectID := booleanFullTextFixture(t, false)
			scan := builder.qry.Nodes[scanID]
			flagPos := int32(len(scan.TableDef.Cols))
			scan.TableDef.Cols = append(scan.TableDef.Cols, &planpb.ColDef{Name: "flag", Typ: planpb.Type{Id: int32(types.T_bool)}})
			scan.TableDef.Name2ColIndex["flag"] = flagPos
			match := DeepCopyExpr(builder.qry.Nodes[projectID].ProjectList[0])
			match.Typ = planpb.Type{Id: int32(types.T_float32)}
			flag := ftjColExpr(scan.TableDef, scan.BindingTags[0], flagPos)
			// NOT provides the composition trigger even when the consumer is a
			// separate bare numeric root or an AND-only expression above the scan.
			scan.FilterList = []*planpb.Expr{booleanFullTextBind(t, "not", DeepCopyExpr(match))}
			consumer := DeepCopyExpr(match)
			switch predicate {
			case "or", "and":
				consumer = booleanFullTextBind(t, predicate, consumer, flag)
			case "not":
				consumer = booleanFullTextBind(t, "not", consumer)
			case "not and scalar":
				consumer = booleanFullTextBind(t, "and", booleanFullTextBind(t, "not", consumer), flag)
			}
			consumerID := scanID
			if predicate == "bare scan sibling" {
				scan.FilterList = append(scan.FilterList, consumer)
			} else {
				consumerID = builder.appendNode(&planpb.Node{NodeType: planpb.Node_FILTER, Children: []int32{scanID}, FilterList: []*planpb.Expr{consumer}}, builder.ctxByNode[scanID])
				builder.qry.Nodes[projectID].Children[0] = consumerID
			}
			root := booleanFullTextRewrite(t, builder, projectID)
			normalized := booleanFullTextScoreProject(t, builder, root)
			scorePos := int32(len(scan.TableDef.Cols))
			var rewritten *planpb.Expr
			if predicate == "bare scan sibling" {
				for _, n := range booleanFullTextNodes(builder.qry, root) {
					if n.NodeType == planpb.Node_FILTER && len(n.FilterList) == 2 {
						rewritten = n.FilterList[1]
					}
				}
			} else {
				rewritten = builder.qry.Nodes[consumerID].FilterList[0]
			}
			require.NotNil(t, rewritten)
			require.Equal(t, int32(types.T_bool), rewritten.Typ.Id, "executable predicates must be BOOL")
			proc := builder.compCtx.GetProcess()
			raw := batch.NewWithSize(1)
			raw.Vecs[0] = vector.NewVec(types.T_float32.ToType())
			defer raw.Clean(proc.Mp())
			// Cross the four index states (returned with a positive, negative and zero
			// score, not returned) with all three scalar truth states.
			for sibling := 0; sibling < 3; sibling++ {
				for state, score := range []float32{2, -2, 0, 0} {
					require.NoError(t, vector.AppendFixed(raw.Vecs[0], score, state == 3, proc.Mp()))
				}
			}
			raw.SetRowCount(12)
			normalizeExpr := DeepCopyExpr(normalized.ProjectList[scorePos])
			require.Equal(t, "coalesce", normalizeExpr.GetF().Func.ObjName)
			streamScore := normalizeExpr.GetF().Args[0].GetCol()
			normalizeExpr = replaceColumnsForExpr(normalizeExpr, map[[2]int32]*planpb.Expr{
				{streamScore.RelPos, streamScore.ColPos}: {Typ: normalizeExpr.GetF().Args[0].Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
			})
			normalizer, err := colexec.NewExpressionExecutor(proc, normalizeExpr)
			require.NoError(t, err)
			defer normalizer.Free()
			scores, err := normalizer.Eval(proc, []*batch.Batch{raw}, nil)
			require.NoError(t, err)
			for row := 0; row < 12; row++ {
				require.False(t, scores.IsNull(uint64(row)))
				require.Equal(t, []float32{2, -2, 0, 0}[row%4], vector.GetFixedAtWithTypeCheck[float32](scores, row))
			}
			foundPos := scorePos + 1
			foundCol := normalized.ProjectList[foundPos]
			require.Equal(t, streamScore, foundCol.GetCol(), "the found slot is the raw stream score")
			require.False(t, foundCol.Typ.NotNullable)
			input := batch.NewWithSize(3)
			// These vectors belong to the normalizer and raw, so input cleanup must not free them.
			input.Vecs[0] = scores
			input.Vecs[1] = vector.NewVec(types.T_bool.ToType())
			defer input.Vecs[1].Free(proc.Mp())
			input.Vecs[2] = raw.Vecs[0]
			for row := 0; row < 12; row++ {
				require.NoError(t, vector.AppendFixed(input.Vecs[1], row/4 == 0, row/4 == 2, proc.Mp()))
			}
			input.SetRowCount(12)
			bound := replaceColumnsForExpr(DeepCopyExpr(rewritten), map[[2]int32]*planpb.Expr{
				{normalized.BindingTags[0], scorePos}: {Typ: planpb.Type{Id: int32(types.T_float32)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
				{normalized.BindingTags[0], flagPos}:  {Typ: flag.Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}},
				{normalized.BindingTags[0], foundPos}: {Typ: foundCol.Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 2}}},
			})
			executor, err := colexec.NewExpressionExecutor(proc, bound)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			for row := 0; row < 12; row++ {
				// A row the index returned matches whatever its score, zero included.
				matched, sibling, unknown := row%4 < 3, row/4 == 0, row/4 == 2
				want, wantNull := matched, false
				switch predicate {
				case "or":
					want, wantNull = matched || sibling, !matched && unknown
				case "and":
					want, wantNull = matched && sibling, matched && unknown
				case "not":
					want = !matched
				case "not and scalar":
					want, wantNull = !matched && sibling, !matched && unknown
				}
				require.Equal(t, wantNull, result.IsNull(uint64(row)), "row %d", row)
				if !wantNull {
					require.Equal(t, want, vector.GetFixedAtWithTypeCheck[bool](result, row), "row %d", row)
				}
			}
		})
	}
}

func TestFullTextBooleanPreservesBoundParametersAndSource(t *testing.T) {
	for _, ft2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "classic", true: "fulltext2"}[ft2], func(t *testing.T) {
			builder, scanID, projectID := booleanFullTextFixture(t, ft2)
			scan := builder.qry.Nodes[scanID]
			if ft2 {
				scan.TableDef.Indexes[0].IncludedColumns = []string{"base_id"}
			}
			snapshot := &planpb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1700000000, LogicalTime: 7}}
			scan.ScanSnapshot = snapshot
			scan.ObjRef.Db, scan.ObjRef.Obj, scan.ObjRef.SubscriptionName = 71, 91, "publisher"
			scan.ObjRef.Snapshot = DeepCopySnapshot(snapshot)
			// A binary/composite identity carries its exact representation metadata.
			pkType := planpb.Type{Id: int32(types.T_varbinary), Width: 191, Scale: 3, Charset: uint32(types.CharsetBinary), NotNullable: true}
			scan.TableDef.Cols[0].Typ = pkType
			match := DeepCopyExpr(builder.qry.Nodes[projectID].ProjectList[0])
			pattern := ftScoreParamExpr(3)
			pattern.Typ.Width = 400
			match.GetF().Args[0] = pattern
			match.Typ = planpb.Type{Id: int32(types.T_float32)}
			threshold := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 5}}}
			scan.FilterList = []*planpb.Expr{booleanFullTextBind(t, "or", booleanFullTextBind(t, ">", DeepCopyExpr(match), threshold), scan.FilterList[0])}
			builder.qry.Nodes[projectID].ProjectList[0] = DeepCopyExpr(match)
			root := booleanFullTextRewrite(t, builder, projectID)
			requireBooleanFullTextCompleteStreams(t, builder.qry, root, 1)
			stream := collectFullTextFunctionScans(builder, root)[0]
			spec := stream.IndexSearchScan
			if ft2 {
				opts, err := ft2plan.DecodeScanOptions(spec.AlgoOptions)
				require.NoError(t, err)
				var config fulltext2.TableConfig
				require.NoError(t, json.Unmarshal([]byte(opts.Config), &config))
				require.Equal(t, []string{"base_id"}, config.IncludeColumns, "catalog INCLUDE names may be carried as configuration")
				require.True(t, planHasReachableNodeType(builder.qry, root, planpb.Node_TABLE_SCAN), "INCLUDE metadata must not remove the preserving base scan")
			}
			require.Equal(t, pattern, spec.QueryPayload)
			require.NotSame(t, pattern, spec.QueryPayload)
			require.Equal(t, int32(3), spec.QueryPayload.GetP().Pos)
			require.Equal(t, pattern.Typ, spec.QueryPayload.Typ)
			require.Equal(t, scan.ObjRef, spec.SourceTable)
			require.NotSame(t, scan.ObjRef, spec.SourceTable)
			require.Equal(t, scan.TableDef.Name, spec.SourceTableDef.Name)
			require.Equal(t, scan.TableDef.Cols, spec.SourceTableDef.Cols)
			require.Equal(t, scan.TableDef.Pkey, spec.SourceTableDef.Pkey)
			require.NotSame(t, scan.TableDef, spec.SourceTableDef)
			require.Equal(t, scan.TableDef.Indexes[0], spec.Index)
			require.Equal(t, pkType, stream.TableDef.Cols[0].Typ)
			for _, got := range []*planpb.Snapshot{stream.ScanSnapshot, spec.ScanSnapshot} {
				require.Equal(t, snapshot, got)
				require.NotSame(t, snapshot, got)
				require.NotSame(t, snapshot.TS, got.TS)
			}
			params := []*planpb.Expr{}
			var visit func(*planpb.Expr)
			visit = func(e *planpb.Expr) {
				if e == nil {
					return
				}
				if e.GetP() != nil {
					params = append(params, e)
				}
				if f := e.GetF(); f != nil {
					for _, arg := range f.Args {
						visit(arg)
					}
				}
			}
			for _, n := range booleanFullTextNodes(builder.qry, root) {
				for _, e := range n.FilterList {
					visit(e)
				}
			}
			require.Len(t, params, 1)
			require.Equal(t, threshold, params[0], "execution threshold stays in the residual Boolean tree")
			require.NotEmpty(t, spec.HiddenTables)
			require.Equal(t, scan.TableDef.Indexes[0].IndexTableName, spec.HiddenTables[0].Object.ObjName)
			// Mutating the template's source objects must not mutate the stream.
			snapshot.TS.LogicalTime = 9
			scan.ObjRef.Obj = 99
			pattern.GetP().Pos = 8
			require.Equal(t, int32(3), spec.QueryPayload.GetP().Pos)
			require.Equal(t, int64(91), spec.SourceTable.Obj)
			require.Equal(t, uint32(7), spec.ScanSnapshot.TS.LogicalTime)
		})
	}
}

func TestFullTextBooleanConsumerBindingsAndOuterJoinBoundary(t *testing.T) {
	for _, consumer := range []string{"aggregate", "window partition", "outer join"} {
		t.Run(consumer, func(t *testing.T) {
			builder, scanID, projectID := booleanFullTextFixture(t, false)
			scan := builder.qry.Nodes[scanID]
			ctx := builder.ctxByNode[scanID]
			match := DeepCopyExpr(builder.qry.Nodes[projectID].ProjectList[0])
			match.Typ = planpb.Type{Id: int32(types.T_float32)}
			scan.FilterList = []*planpb.Expr{booleanFullTextBind(t, "or", DeepCopyExpr(match), scan.FilterList[0])}
			child := scanID
			switch consumer {
			case "aggregate":
				agg := &planpb.Node{NodeType: planpb.Node_AGG, Children: []int32{scanID}, BindingTags: []int32{builder.genNewBindTag()},
					GroupBy: []*planpb.Expr{DeepCopyExpr(match)}, AggList: []*planpb.Expr{ftAggFn("max", DeepCopyExpr(match))}}
				child = builder.appendNode(agg, ctx)
				builder.qry.Nodes[projectID].ProjectList = []*planpb.Expr{{Typ: match.Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: agg.BindingTags[0], ColPos: 0}}}}
			case "window partition":
				part := &planpb.Node{NodeType: planpb.Node_PARTITION, Children: []int32{scanID}, OrderBy: []*planpb.OrderBySpec{{Expr: DeepCopyExpr(match), Flag: planpb.OrderBySpec_INTERNAL}}}
				partID := builder.appendNode(part, ctx)
				window := &planpb.Node{NodeType: planpb.Node_WINDOW, Children: []int32{partID}, BindingTags: []int32{builder.genNewBindTag()},
					WinSpecList: []*planpb.Expr{ftWindowSpecWithMatch(DeepCopyExpr(match))}}
				window.WinSpecList[0].GetW().PartitionBy = []*planpb.Expr{DeepCopyExpr(match)}
				child = builder.appendNode(window, ctx)
			case "outer join":
				baseDef := makeFullTextJoinTestTableDef("base", false)
				baseID := builder.appendNode(makeFullTextJoinTestScan(baseDef, builder.genNewBindTag(), nil), ctx)
				join := &planpb.Node{NodeType: planpb.Node_JOIN, JoinType: planpb.Node_LEFT, Children: []int32{baseID, scanID},
					OnList: []*planpb.Expr{ftjMakeEqExpr(t, ftjColExpr(baseDef, builder.qry.Nodes[baseID].BindingTags[0], 0), ftjColExpr(scan.TableDef, scan.BindingTags[0], 0)), DeepCopyExpr(match)}}
				child = builder.appendNode(join, ctx)
			}
			builder.qry.Nodes[projectID].Children[0] = child
			root := booleanFullTextRewrite(t, builder, projectID)
			normalized := booleanFullTextScoreProject(t, builder, root)
			scorePos := int32(len(scan.TableDef.Cols))
			// Capture the actual published consumer slots after the traversal.
			var consumers []*planpb.Expr
			n := builder.qry.Nodes[child]
			switch consumer {
			case "aggregate":
				consumers = []*planpb.Expr{n.GroupBy[0], n.AggList[0].GetF().Args[0]}
			case "window partition":
				w := n.WinSpecList[0].GetW()
				consumers = []*planpb.Expr{w.OrderBy[0].Expr, w.PartitionBy[0], builder.qry.Nodes[n.Children[0]].OrderBy[0].Expr, builder.qry.Nodes[projectID].ProjectList[0]}
			case "outer join":
				require.Equal(t, planpb.Node_LEFT, n.JoinType)
				require.Equal(t, int32(types.T_bool), n.OnList[1].Typ.Id, "bare ON consumer gets numeric truth coercion")
				// A bare MATCH in ON is whether the index returned the row.
				require.Equal(t, "isnotnull", n.OnList[1].GetF().Func.ObjName)
				found := n.OnList[1].GetF().Args[0].GetCol()
				require.Equal(t, [2]int32{normalized.BindingTags[0], scorePos + 1}, [2]int32{found.RelPos, found.ColPos})
				consumers = []*planpb.Expr{builder.qry.Nodes[projectID].ProjectList[0]}
				// A user-join missing child must remain NULL above this boundary.
				require.False(t, exprCallsFunc(builder.qry.Nodes[projectID].ProjectList[0], "coalesce"))
				require.True(t, planHasReachableNodeType(builder.qry, n.Children[1], planpb.Node_PROJECT))
			}
			for _, expr := range consumers {
				require.NotNil(t, expr.GetCol())
				require.Equal(t, normalized.BindingTags[0], expr.GetCol().RelPos)
				require.Equal(t, scorePos, expr.GetCol().ColPos)
				require.Equal(t, int32(types.T_float32), expr.Typ.Id)
			}
			counts := map[[2]int32]int{}
			builder.countColRefs(root, counts)
			root, _ = builder.removeSimpleProjections(root, planpb.Node_UNKNOWN, false, counts)
			builder.qry.Steps = []int32{root}
			require.Same(t, normalized, booleanFullTextScoreProject(t, builder, root), "final simplification retains normalization at the scan owner")
			require.Zero(t, countReachableFullTextMatches(builder.qry))
			requireBooleanFullTextCompleteStreams(t, builder.qry, root, 1)
		})
	}
}

func buildBooleanFullTextPublicPlan(t *testing.T, sql string, ft2, prepare bool, vectorAlgo ...string) *planpb.Query {
	t.Helper()
	query, _ := buildBooleanFullTextPublicPlanWithContext(t, sql, ft2, prepare, vectorAlgo...)
	return query
}

func buildBooleanFullTextPublicPlanWithContext(t *testing.T, sql string, ft2, prepare bool, vectorAlgo ...string) (*planpb.Query, *fullTextJoinMockCompilerContext) {
	t.Helper()
	cc := newFullTextJoinMockCompilerContext(t)
	builder := NewQueryBuilder(planpb.Query_SELECT, cc, false, true)
	def := makeFullTextJoinTestTableDef("ft", true)
	scan := makeFullTextJoinTestScan(def, 0, nil)
	registerFullTextJoinRegularIndexTable(builder, def.Indexes[0].IndexTableName)
	if ft2 {
		convertFullTextJoinTestToFulltext2(builder, scan)
		for _, idx := range def.Indexes {
			registerFullTextJoinRegularIndexTable(builder, idx.IndexTableName)
		}
	}
	if len(vectorAlgo) != 0 {
		def.TblId = 901
		def.Cols[0].Typ = planpb.Type{Id: int32(types.T_int64), NotNullable: true}
		def.Name2ColIndex["v"] = int32(len(def.Cols))
		def.Cols = append(def.Cols, &planpb.ColDef{Name: "v", Typ: planpb.Type{Id: int32(types.T_array_float32), Width: 3, NotNullable: true}})
		index := newVectorJoinIvfIndex()
		if vectorAlgo[0] == catalog.MoIndexHnswAlgo.ToString() {
			index = newVectorJoinHnswIndex()
		}
		for _, idx := range index.IndexDefs {
			idx.TableExist = true
			def.Indexes = append(def.Indexes, idx)
			cc.tables[idx.IndexTableName] = &planpb.TableDef{Name: idx.IndexTableName, TblId: uint64(910 + len(def.Indexes))}
			cc.objects[idx.IndexTableName] = &planpb.ObjectRef{SchemaName: "test", ObjName: idx.IndexTableName, Obj: int64(910 + len(def.Indexes))}
		}
	}
	def.Indexes = append(def.Indexes, &planpb.IndexDef{IndexName: "idx_base_id", IndexTableName: "__mo_idx_base_id", Parts: []string{"base_id", "id"}, TableExist: true})
	registerFullTextJoinRegularIndexTable(builder, "__mo_idx_base_id")
	cc.tables["ft"], cc.objects["ft"] = def, &planpb.ObjectRef{SchemaName: "test", ObjName: "ft"}
	cc.tables["base"], cc.objects["base"] = makeFullTextJoinTestTableDef("base", false), &planpb.ObjectRef{SchemaName: "test", ObjName: "base"}
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	p, err := BuildPlan(cc, stmt, prepare)
	require.NoError(t, err)
	require.NotNil(t, p.GetQuery())
	require.Zero(t, countReachableFullTextMatches(p.GetQuery()))
	return p.GetQuery(), cc
}

func TestFullTextBooleanPublicConsumersAndLegacyControls(t *testing.T) {
	for _, ft2 := range []bool{false, true} {
		for _, tc := range []struct {
			name, sql string
			streams   int
			preserve  bool
			// scores counts the leaves whose score, not only whether the index returned
			// the row, is consumed.
			scores int
		}{
			{"dedup projected and ordered score", "select id, match(title, body) against('hello') sc from ft where match(title, body) against('hello') or base_id='b1' order by sc desc, id limit 2 offset 1", 1, true, 1},
			{"independent terms and bare sibling", "select id from ft where (match(title, body) against('hello') or base_id='b1') and match(title, body) against('world')", 2, true, 0},
			{"distinct served projected scores", "select id, match(title, body) against('hello'), match(title, body) against('world') from ft where match(title, body) against('hello') or match(title, body) against('world')", 2, true, 2},
			{"zero comparison and scalar NULL", "select id from ft where match(title, body) against('hello')=0 or base_id is null", 1, true, 1},
			{"aggregate score and HAVING", "select base_id, max(match(title, body) against('hello')) sc from ft where match(title, body) against('hello') or base_id='b1' group by base_id having sc >= 0", 1, true, 1},
			{"window score order and partition", "select id, row_number() over(partition by base_id order by match(title, body) against('hello') desc) rn from ft where match(title, body) against('hello') or base_id='b1'", 1, true, 1},
			{"inner join child", "select f.id, match(f.title, f.body) against('hello') from ft f join base b on f.base_id=b.id where match(f.title, f.body) against('hello') or f.base_id='b1'", 1, true, 1},
			{"outer join nullable child", "select b.id, d.sc from base b left join (select id, match(title, body) against('hello') sc from ft where match(title, body) against('hello') or base_id='b1') d on b.id=d.id", 1, true, 1},
			{"positive driver control", "select id from ft where match(title, body) against('hello')", 1, false, 0},
			{"projection only control", "select id, match(title, body) against('hello') from ft", 1, false, 0},
		} {
			t.Run(tc.name+map[bool]string{false: "/classic", true: "/fulltext2"}[ft2], func(t *testing.T) {
				query := buildBooleanFullTextPublicPlan(t, tc.sql, ft2, false)
				root := query.Steps[0]
				if tc.preserve {
					requireBooleanFullTextCompleteStreams(t, query, root, tc.streams)
				} else {
					require.Equal(t, tc.streams, countReachableFullTextScans(query)+countReachableFullText2Scans(query))
				}
				attachments, normalization := 0, 0
				for _, n := range booleanFullTextNodes(query, root) {
					if n.NodeType == planpb.Node_JOIN && len(n.Children) == 2 && (isFullTextSearchScan(query.Nodes[n.Children[1]]) || isFulltext2SearchScan(query.Nodes[n.Children[1]])) {
						attachments++
						want := planpb.Node_INNER
						if tc.preserve {
							want = planpb.Node_LEFT
							require.Empty(t, n.RuntimeFilterBuildList)
						}
						require.Equal(t, want, n.JoinType)
					}
					if n.NodeType == planpb.Node_PROJECT {
						for _, expr := range n.ProjectList {
							if expr.GetF() != nil && expr.GetF().Func.ObjName == "coalesce" && expr.Typ.Id == int32(types.T_float32) {
								normalization++
							}
						}
					}
				}
				require.Equal(t, tc.streams, attachments)
				require.Equal(t, tc.scores, normalization, "exactly one retained normalized score slot per scored leaf")
			})
		}
	}
}

func TestFullTextBooleanPublicVectorTopKDeclinesCandidates(t *testing.T) {
	for _, algo := range []string{catalog.MoIndexHnswAlgo.ToString(), catalog.MoIndexIvfFlatAlgo.ToString()} {
		for _, ft2 := range []bool{false, true} {
			t.Run(algo+map[bool]string{false: "/classic", true: "/fulltext2"}[ft2], func(t *testing.T) {
				// The no-MATCH control proves this table/index/distance actually
				// reaches ANN. A Boolean query must keep the exact sort instead.
				control := buildBooleanFullTextPublicPlan(t, "select id from ft order by l2_distance(v,'[1,2,3]') limit 2", ft2, false, algo)
				vectorScans := func(q *planpb.Query) int {
					count := 0
					for _, n := range booleanFullTextNodes(q, q.Steps[0]) {
						if n.NodeType == planpb.Node_INDEX_SEARCH_SCAN && n.IndexSearchScan.Index.IndexAlgo == algo {
							count++
						}
					}
					return count
				}
				require.Equal(t, 1, vectorScans(control), "nearest control must exercise the eligible ANN path")
				query := buildBooleanFullTextPublicPlan(t, "select id from ft where match(title,body) against('hello') or base_id='b1' order by l2_distance(v,'[1,2,3]') limit 2", ft2, false, algo)
				require.Zero(t, vectorScans(query), "early ANN must not truncate scalar-only OR rows")
				requireBooleanFullTextCompleteStreams(t, query, query.Steps[0], 1)
				exactSort := false
				for _, n := range booleanFullTextNodes(query, query.Steps[0]) {
					if n.NodeType == planpb.Node_SORT {
						for _, order := range n.OrderBy {
							if exprCallsFunc(order.Expr, "l2_distance") {
								exactSort = true
							}
						}
					}
				}
				require.True(t, exactSort, "requested distance ordering stays above the complete Boolean input")
			})
		}
	}
}

func TestFullTextBooleanPublicPreparedReuseKeepsPatternIdentity(t *testing.T) {
	for _, ft2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "classic", true: "fulltext2"}[ft2], func(t *testing.T) {
			query, cc := buildBooleanFullTextPublicPlanWithContext(t, "select id from ft where match(title,body) against(?) > ? or base_id=? order by id limit ? offset ?", ft2, true)
			template := &planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}
			find := func(q *planpb.Query) *planpb.Node {
				var stream *planpb.Node
				for _, n := range booleanFullTextNodes(q, q.Steps[0]) {
					if isFullTextSearchScan(n) || isFulltext2SearchScan(n) {
						require.Nil(t, stream)
						stream = n
					}
				}
				require.NotNil(t, stream)
				return stream
			}
			pattern := find(query).IndexSearchScan.QueryPayload
			// baseBindParam publishes TEXT; bindFullTextMatchExpr preserves it
			// under the ordinary VARCHAR cast required by AGAINST.
			require.NotNil(t, pattern.GetF())
			require.Equal(t, "cast", pattern.GetF().Func.ObjName)
			require.Equal(t, int32(types.T_varchar), pattern.Typ.Id)
			var markers []*planpb.Expr
			require.NoError(t, planpb.VisitExprTree(pattern, func(e *planpb.Expr) error {
				if e.GetP() != nil {
					markers = append(markers, e)
				}
				return nil
			}))
			require.Len(t, markers, 1)
			require.Equal(t, int32(1), markers[0].GetP().Pos, "bind/index rewrites preserve the parser's raw one-based ordinal")
			require.Equal(t, int32(types.T_text), markers[0].Typ.Id)
			// PREPARE calls ResetPreparePlan after BuildPlan. Its source count
			// preserves all five original markers even if optimization elides one.
			schemas, paramTypes, err := ResetPreparePlan(cc, template, 5)
			require.NoError(t, err)
			require.NotEmpty(t, schemas)
			require.Len(t, paramTypes, 5)
			require.Equal(t, int32(0), markers[0].GetP().Pos, "execution binding is zero-based after the real PREPARE reset")
			originalPattern := DeepCopyExpr(pattern)
			proc := newPlanTestProcess(t)
			evaluatePattern := func(e *planpb.Expr) string {
				executor, err := colexec.NewExpressionExecutor(proc, e)
				require.NoError(t, err)
				defer executor.Free()
				result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
				require.NoError(t, err)
				require.False(t, result.IsNull(0))
				return result.GetStringAt(0)
			}
			for _, term := range []string{"hello", "world", "hello"} {
				filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), template, []any{term, float64(0), "b1", uint64(2), uint64(1)})
				require.NoError(t, err)
				requireBooleanFullTextCompleteStreams(t, filled.GetQuery(), filled.GetQuery().Steps[0], 1)
				require.Equal(t, term, evaluatePattern(find(filled.GetQuery()).IndexSearchScan.QueryPayload))
				require.Equal(t, originalPattern, find(query).IndexSearchScan.QueryPayload, "binding a reuse must preserve the complete template expression")
			}
		})
	}
}

func TestFullTextBooleanPublicExplainPlansPreserveComposition(t *testing.T) {
	for _, ft2 := range []bool{false, true} {
		for _, prefix := range []string{"explain ", "explain analyze "} {
			t.Run(prefix+map[bool]string{false: "/classic", true: "/fulltext2"}[ft2], func(t *testing.T) {
				query := buildBooleanFullTextPublicPlan(t, prefix+"select id from ft where not match(title,body) against('hello') and base_id is not null order by id limit 2 offset 1", ft2, false)
				root := query.Steps[0]
				requireBooleanFullTextCompleteStreams(t, query, root, 1)
				require.True(t, planHasReachableNodeType(query, root, planpb.Node_FILTER), "EXPLAIN follows the real executable Boolean FILTER route")
				attachments := 0
				for _, n := range booleanFullTextNodes(query, root) {
					if n.NodeType == planpb.Node_JOIN && len(n.Children) == 2 && (isFullTextSearchScan(query.Nodes[n.Children[1]]) || isFulltext2SearchScan(query.Nodes[n.Children[1]])) {
						attachments++
						require.Equal(t, planpb.Node_LEFT, n.JoinType)
					}
				}
				require.Equal(t, 1, attachments)
			})
		}
	}
}

func TestFullTextBooleanRefusesUnservedProjectionLeaf(t *testing.T) {
	for _, ft2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "classic", true: "fulltext2"}[ft2], func(t *testing.T) {
			builder, scanID, projectID := booleanFullTextFixture(t, ft2)
			scan := builder.qry.Nodes[scanID]
			driving := DeepCopyExpr(builder.qry.Nodes[projectID].ProjectList[0])
			scan.FilterList = []*planpb.Expr{booleanFullTextBind(t, "or", driving, scan.FilterList[0])}
			unserved := builder.qry.Nodes[projectID].ProjectList[0]
			unserved.GetF().Args[0] = makePlan2StringConstExprWithType("world", false)
			builder.prepareSpecialIndexGuards(projectID)
			root, err := builder.applyIndices(projectID, map[[2]int32]int{}, map[[2]int32]*planpb.Expr{})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
			require.Equal(t, int32(-1), root, "a partially served query must not publish an executable root")
			streams := collectFullTextFunctionScans(builder, projectID)
			require.Len(t, streams, 1, "the unserved projection must not add a new preserving or legacy driver")
			require.Equal(t, "hello", streams[0].IndexSearchScan.QueryPayload.GetLit().GetSval())
			for _, n := range booleanFullTextNodes(builder.qry, projectID) {
				if n.NodeType == planpb.Node_JOIN {
					require.Equal(t, planpb.Node_LEFT, n.JoinType, "no legacy INNER restriction is added for the distinct projection")
				}
			}
			require.True(t, exprCallsFunc(builder.qry.Nodes[projectID].ProjectList[0], "fulltext_match"), "an unserved leaf must not borrow another term's score")
		})
	}
}

func TestFullTextBooleanForcedJoinHintsDoNotSkipComposition(t *testing.T) {
	for _, ft2 := range []bool{false, true} {
		kind := "classic"
		if ft2 {
			kind = "fulltext2"
		}
		for _, hint := range []string{"", " force index for join(primary)", " force index for join(idx_base_id)"} {
			for _, predicate := range []string{
				"match(f.title,f.body) against('hello') or f.base_id='b1'",
				"not match(f.title,f.body) against('hello')",
			} {
				t.Run(kind+"/"+hint+"/"+predicate, func(t *testing.T) {
					sql := "select f.id from ft f" + hint + " join base b on f.base_id=b.id where " + predicate
					query := buildBooleanFullTextPublicPlan(t, sql, ft2, false)
					requireBooleanFullTextCompleteStreams(t, query, query.Steps[0], 1)
				})
			}
		}
	}
}

// A MATCH used as a Boolean is whether the index returned the row, whatever its score;
// compared or projected it is the score (#31411).
func TestFullTextBooleanMatchTruthIsFound(t *testing.T) {
	for _, ft2 := range []bool{false, true} {
		for _, tc := range []struct {
			name, sql     string
			found, scored bool
		}{
			{"or", "select id from ft where match(title, body) against('hello') or base_id='b1'", true, false},
			{"not", "select id from ft where not match(title, body) against('hello')", true, false},
			{"and inside or", "select id from ft where (match(title, body) against('hello') and base_id='b2') or base_id='b1'", true, false},
			{"case when", "select id, case when match(title, body) against('hello') then 1 else 0 end from ft where match(title, body) against('hello')", true, false},
			{"if", "select id, if(match(title, body) against('hello'), 'y', 'n') from ft where match(title, body) against('hello')", true, false},
			{"if under or", "select id, if(match(title, body) against('hello'), 'y', 'n') from ft where match(title, body) against('hello') or base_id='b1'", true, false},
			{"if in aggregate under or", "select base_id, count(if(match(title, body) against('hello'), 1, null)) from ft where match(title, body) against('hello') or base_id='b1' group by base_id", true, false},
			{"xor", "select id from ft where match(title, body) against('hello') xor base_id='b1'", true, false},
			{"bang", "select id from ft where !match(title, body) against('hello')", true, false},
			{"comparison", "select id from ft where match(title, body) against('hello') > 0 or base_id='b1'", false, true},
			{"projected score", "select id, match(title, body) against('hello') from ft where not match(title, body) against('hello')", true, true},
		} {
			t.Run(tc.name+map[bool]string{false: "/classic", true: "/fulltext2"}[ft2], func(t *testing.T) {
				query := buildBooleanFullTextPublicPlan(t, tc.sql, ft2, false)
				var exprs []*planpb.Expr
				for _, n := range booleanFullTextNodes(query, query.Steps[0]) {
					exprs = append(exprs, n.FilterList...)
					exprs = append(exprs, n.ProjectList...)
					exprs = append(exprs, n.AggList...)
				}
				found, castScore := 0, 0
				for _, expr := range exprs {
					found += countExprFunctionCalls([]*planpb.Expr{expr}, "isnotnull")
					castScore += countBoolCastOfFloat(expr)
					requireIfConditionsFound(t, expr)
				}
				require.Equal(t, tc.found, found > 0, "found test")
				require.Zero(t, castScore, "a score is never used as a truth value")
				if tc.scored {
					scored := false
					for _, n := range booleanFullTextNodes(query, query.Steps[0]) {
						for _, expr := range n.ProjectList {
							scored = scored || exprCallsFunc(expr, "coalesce") || exprCallsFunc(expr, ">")
						}
						for _, expr := range n.FilterList {
							scored = scored || exprCallsFunc(expr, ">")
						}
					}
					require.True(t, scored)
				}
			})
		}
	}
}

// requireIfConditionsFound requires every IF condition in expr to be a found test.
func requireIfConditionsFound(t *testing.T, expr *planpb.Expr) {
	fn := expr.GetF()
	if fn == nil {
		return
	}
	if fn.Func.ObjName == "if" {
		cond := fn.Args[0].GetF()
		require.NotNil(t, cond)
		require.Equal(t, "isnotnull", cond.Func.ObjName)
	}
	for _, arg := range fn.Args {
		requireIfConditionsFound(t, arg)
	}
}

func countBoolCastOfFloat(expr *planpb.Expr) int {
	fn := expr.GetF()
	if fn == nil {
		return 0
	}
	n := 0
	if fn.Func.ObjName == "cast" && len(fn.Args) == 2 && fn.Args[1].Typ.Id == int32(types.T_bool) && fn.Args[0].Typ.Id == int32(types.T_float32) {
		n++
	}
	for _, arg := range fn.Args {
		n += countBoolCastOfFloat(arg)
	}
	return n
}

// A BOOL value of MATCH, an explicit CAST included, is whether the index returned the row;
// a cast through a numeric type converts the score.
func TestFullTextBooleanExplicitCastIsFound(t *testing.T) {
	for _, ft2 := range []bool{false, true} {
		for _, sql := range []string{
			"select id, cast(match(title, body) against('hello') as bool), cast(cast(match(title, body) against('hello') as double) as bool) from ft where match(title, body) against('hello')",
			"select id, cast(match(title, body) against('hello') as bool), cast(cast(match(title, body) against('hello') as double) as bool) from ft where match(title, body) against('hello') or base_id='b1'",
		} {
			query := buildBooleanFullTextPublicPlan(t, sql, ft2, false)
			projected := query.Nodes[query.Steps[0]].ProjectList
			require.Len(t, projected, 3, sql)
			require.Equal(t, 1, countExprFunctionCalls(projected[1:2], "isnotnull"), sql)
			require.Zero(t, countExprFunctionCalls(projected[2:], "isnotnull"), sql)
		}
	}
}

func TestFullTextBooleanProjectedScoreAsBoolIsScore(t *testing.T) {
	planFuncCalls := func(query *planpb.Query, name string) int {
		count := 0
		var visit func(int32)
		visit = func(id int32) {
			node := query.Nodes[id]
			count += countExprFunctionCalls(node.FilterList, name)
			count += countExprFunctionCalls(node.ProjectList, name)
			for _, child := range node.Children {
				visit(child)
			}
		}
		visit(query.Steps[0])
		return count
	}
	const inner = "select id, match(title, body) against('hello') sc from ft where match(title, body) against('hello')"
	for _, ft2 := range []bool{false, true} {
		for _, where := range []string{"%s", "%s or id < 0", "%s or id", "not %s", "if(%s, true, false)", "case when %s then true else false end"} {
			composed := inner + " or base_id = 'b1'"
			for _, from := range []string{inner, composed,
				inner + " union all " + composed, composed + " union " + inner,
				inner + " intersect " + composed, composed + " except " + inner,
				"select id, match(title, body) against('hello') sc from ft where match(title, body) against('hello') group by id, sc",
			} {
				sql := fmt.Sprintf("select id from (%s) d where %s", from, fmt.Sprintf(where, "sc"))
				control := fmt.Sprintf("select id from (%s) d where %s", from, fmt.Sprintf(where, "sc <> 0"))
				query := buildBooleanFullTextPublicPlan(t, sql, ft2, false)
				expected := buildBooleanFullTextPublicPlan(t, control, ft2, false)
				require.Equal(t, planFuncCalls(expected, "isnotnull"), planFuncCalls(query, "isnotnull"), sql)
				require.Equal(t, planFuncCalls(expected, "<>"), planFuncCalls(query, "<>"), sql)
				require.Positive(t, planFuncCalls(query, "<>"), sql)
			}
		}
	}
}
