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
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func temporalOriginalCacheFixture(t *testing.T) (*QueryBuilder, *Node, *Node) {
	builder, _, _ := temporalCBOTraversalFixture(t)
	scope := builder.temporalPlanningScope
	require.True(t, scope.EnableOrdinaryCache())
	_, claimed := scope.claim(scope.root.Load())
	require.True(t, claimed)
	scan := builder.qry.Nodes[0]
	filter := &Node{NodeId: 1, NodeType: plan.Node_FILTER, Children: []int32{0}, FilterList: scan.FilterList, NotCacheable: true}
	scan.FilterList = nil
	project := &Node{NodeId: 2, NodeType: plan.Node_PROJECT, Children: []int32{1}, ProjectList: []*Expr{GetColExpr(scan.TableDef.Cols[0].Typ, scan.BindingTags[0], 0)}, NotCacheable: true}
	builder.qry.Nodes = append(builder.qry.Nodes, filter, project)
	builder.qry.TemporalIndexNeedsReplan = true
	builder.temporalCacheFilter, builder.temporalCacheProject = filter, project
	return builder, filter, project
}

func TestTemporalOriginalCacheAuditClearsOnlyCertifiedTimeFlags(t *testing.T) {
	for _, scenario := range []string{"eligible", "query bit absent", "dynamic projection", "independent scan flag", "wrong filter identity", "missing inherited flag", "unclaimed root", "native read changed"} {
		t.Run(scenario, func(t *testing.T) {
			builder, filter, project := temporalOriginalCacheFixture(t)
			scope := builder.temporalPlanningScope
			switch scenario {
			case "query bit absent":
				builder.qry.TemporalIndexNeedsReplan = false
			case "dynamic projection":
				project.ProjectList = filter.FilterList[0].GetF().Args[1:]
			case "independent scan flag":
				builder.qry.Nodes[0].NotCacheable = true
			case "wrong filter identity":
				copy := *filter
				builder.temporalCacheFilter = &copy
			case "missing inherited flag":
				project.NotCacheable = false
			case "native read changed":
				scope.sources[0].relation.(*temporalCBORelationFixture).invalidRead = true
			case "unclaimed root":
				scope.root.Store(&tree.Select{})
			}
			priorFilter, priorProject := filter.NotCacheable, project.NotCacheable
			builder.auditTemporalOriginalCacheOwner(2)
			scope.SelectQuery(builder.qry)
			proof := scope.cacheProof
			ok := proof.valid
			require.Equal(t, scenario == "eligible", ok)
			if ok {
				require.True(t, proof.valid)
				require.False(t, filter.NotCacheable)
				require.False(t, project.NotCacheable)
				require.True(t, builder.qry.TemporalIndexNeedsReplan)
			} else {
				require.Equal(t, priorFilter, filter.NotCacheable)
				require.Equal(t, priorProject, project.NotCacheable)
			}
		})
	}
}

func TestTemporalCacheCompletePreflightBoundsMetadata(t *testing.T) {
	for _, scenario := range []string{"small", "huge metadata string", "huge map", "unknown metadata", "opaque metadata bytes", "expression cycle", "typed nil oneof", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			q := &Query{StmtType: plan.Query_SELECT, Nodes: []*Node{{NodeType: plan.Node_TABLE_SCAN, TableDef: &TableDef{Name: "t"}}}}
			p := &Plan{Plan: &plan.Plan_Query{Query: q}}
			switch scenario {
			case "huge metadata string":
				q.Nodes[0].TableDef.Name = strings.Repeat("x", 128*1024+1)
			case "huge map":
				q.Nodes[0].TableDef.Name2ColIndex = make(map[string]int32)
				for i := range 1025 {
					q.Nodes[0].TableDef.Name2ColIndex[strconv.Itoa(i)] = int32(i)
				}
			case "unknown metadata":
				q.Nodes[0].TableDef.XXX_unrecognized = []byte{8, 1}
			case "opaque metadata bytes":
				q.Nodes[0].TableDef.Cols = []*plan.ColDef{{Default: &plan.Default{Expr: &Expr{Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Data: []byte{1}}}}}}}
			case "expression cycle":
				e := &Expr{Expr: &plan.Expr_List{List: &plan.ExprList{}}}
				e.GetList().List = []*Expr{e}
				q.Nodes[0].FilterList = []*Expr{e}
			case "typed nil oneof":
				p.Plan = (*plan.Plan_Query)(nil)
			case "cancelled":
				cancel()
			}
			require.Equal(t, scenario == "small", temporalCachePlanFits(ctx, p))
		})
	}
}

func TestTemporalCacheAuditBindsBaseAndVerifiedHiddenPlan(t *testing.T) {
	for _, hidden := range []bool{false, true} {
		t.Run(map[bool]string{false: "base", true: "hidden"}[hidden], func(t *testing.T) {
			builder, filter, project := temporalOriginalCacheFixture(t)
			builder.auditTemporalOriginalCacheOwner(2)
			scope := builder.temporalPlanningScope
			scope.SelectQuery(builder.qry)
			_, ok := scope.TemporalCacheProof(builder.qry)
			require.False(t, ok, "an original BASE certificate is not cache publication authority")
			scan := builder.qry.Nodes[0]
			scan.FilterList = filter.FilterList
			builder.qry.CatalogDependencies = []*ObjectRef{prepareSchemaRef(scan.ObjRef, scan.TableDef)}
			if hidden {
				builder.ctxByNode = append(builder.ctxByNode, nil, nil)
				mapping := make(map[[2]int32]*Expr)
				id, err := builder.applyIndicesWithOrder(0, map[[2]int32]int{{scan.BindingTags[0], 0}: 1}, mapping, false)
				require.NoError(t, err)
				require.NotZero(t, id)
				project.Children = []int32{id}
				project.BindingTags = []int32{builder.genNewBindTag()}
				project.ProjectList = []*Expr{replaceColumnsForExpr(DeepCopyExpr(project.ProjectList[0]), mapping)}
				builder.qry.Steps = []int32{2}
				_, err = builder.remapAllColRefs(2, 0, map[[2]int32]int{{project.BindingTags[0], 0}: 1}, map[[2]int32]bool{}, map[[2]int32]int{})
				require.NoError(t, err)
			} else {
				builder.ctxByNode = append(builder.ctxByNode, nil, nil)
				builder.qry.Steps = []int32{0}
				_, err := builder.remapAllColRefs(0, 0, map[[2]int32]int{{scan.BindingTags[0], 0}: 1}, map[[2]int32]bool{}, map[[2]int32]int{})
				require.NoError(t, err)
			}
			proof, ok := scope.TemporalCacheProof(builder.qry)
			require.Equal(t, hidden, ok)
			p := &Plan{Plan: &plan.Plan_Query{Query: builder.qry}}
			require.False(t, AuditTemporalCacheRaw(builder.GetContext(), p, proof, nil))
			if !hidden {
				require.False(t, AuditTemporalCacheRaw(builder.GetContext(), p, proof, builder.compCtx.GetProcess()))
				_, cloned := CloneTemporalCachePlan(builder.GetContext(), p)
				require.False(t, cloned, "BASE must be costed afresh as the clock changes")
				return
			}
			require.True(t, AuditTemporalCacheRaw(builder.GetContext(), p, proof, builder.compCtx.GetProcess()), "final optimizer/remap mapping must agree with original ownership")
			if hidden {
				selected := builder.qry.Nodes[builder.qry.Nodes[builder.qry.Steps[0]].Children[0]]
				rule.NewConstantFold(false).Apply(selected, builder.qry, builder.compCtx.GetProcess())
				require.True(t, AuditTemporalCacheRaw(builder.GetContext(), p, proof, builder.compCtx.GetProcess()), "ordinary static-key folding preserves the proof and raw NOW")
				bad := proto.Clone(p).(*plan.Plan)
				var corrupt func(*Expr) bool
				corrupt = func(e *Expr) bool {
					if lit := e.GetLit(); lit != nil && lit.IsSerialized {
						lit.Value = &plan.Literal_Sval{Sval: lit.GetSval() + "bad"}
						return true
					}
					if f := e.GetF(); f != nil {
						for _, arg := range f.Args {
							if corrupt(arg) {
								return true
							}
						}
					}
					return false
				}
				require.True(t, corrupt(bad.GetQuery().Nodes[selected.NodeId].BlockFilterList[0]))
				require.False(t, AuditTemporalCacheRaw(builder.GetContext(), bad, proof, builder.compCtx.GetProcess()), "folded bytes must remain exact")
			}
			raw, ok := CloneTemporalCachePlan(builder.GetContext(), p)
			require.True(t, ok)
			require.NotSame(t, p, raw)
			originalTable := p.GetQuery().Nodes[0].TableDef
			rawTable := raw.GetQuery().Nodes[0].TableDef
			require.NotSame(t, originalTable, rawTable)
			require.NotSame(t, originalTable.Cols[0], rawTable.Cols[0])
			require.NotSame(t, originalTable.Indexes[0], rawTable.Indexes[0])
			originalTable.Cols[0].Name = "changed_after_clone"
			require.NotEqual(t, originalTable.Cols[0].Name, rawTable.Cols[0].Name)
			require.True(t, AuditTemporalCacheRaw(builder.GetContext(), raw, proof, builder.compCtx.GetProcess()))
			root := raw.GetQuery().Nodes[raw.GetQuery().Steps[0]]
			root.NotCacheable = true
			require.False(t, AuditTemporalCacheRaw(builder.GetContext(), raw, proof, builder.compCtx.GetProcess()), "a later independent no-cache reason cannot be cleared")
		})
	}
}

func TestTemporalCacheNativeReadRevalidatedAndCloseRevokes(t *testing.T) {
	builder, _, _ := temporalOriginalCacheFixture(t)
	builder.auditTemporalOriginalCacheOwner(2)
	scope := builder.temporalPlanningScope
	scope.SelectQuery(builder.qry)
	require.True(t, scope.ValidateTemporalCacheRead(t.Context(), builder.qry))
	source := scope.sources[0].relation.(*temporalCBORelationFixture)
	source.invalidRead = true
	require.False(t, scope.ValidateTemporalCacheRead(t.Context(), builder.qry))
	source.invalidRead = false
	require.True(t, scope.ValidateTemporalCacheRead(t.Context(), builder.qry))
	scope.Close()
	require.False(t, scope.ValidateTemporalCacheRead(t.Context(), builder.qry))
}

func TestTemporalCacheChecksHiddenAndCallbackInvalidation(t *testing.T) {
	for _, scenario := range []string{"hidden changed", "close in callback"} {
		t.Run(scenario, func(t *testing.T) {
			builder, _, _ := temporalOriginalCacheFixture(t)
			builder.auditTemporalOriginalCacheOwner(2)
			scope := builder.temporalPlanningScope
			scope.SelectQuery(builder.qry)
			base := scope.sources[0]
			hidden := &temporalCBORelationFixture{}
			require.True(t, scope.AddNativeRelation(&ObjectRef{Obj: 999, SchemaName: "db", ObjName: "hidden"}, &TableDef{TblId: 999, Version: 2}, hidden, base.read))
			scope.cacheProof.hiddenID, scope.cacheProof.hiddenVersion = 999, 2
			require.True(t, scope.ValidateTemporalCacheRead(t.Context(), builder.qry))
			if scenario == "hidden changed" {
				hidden.invalidRead = true
			} else {
				base.relation.(*temporalCBORelationFixture).onValidate = scope.Close
			}
			require.False(t, scope.ValidateTemporalCacheRead(t.Context(), builder.qry))
		})
	}
}
