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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// potentialTemporalIndexAccess detects a possible execution-dependent choice
// before admission and costing. This is a cache-lifetime fact, never permission
// to create an index candidate. Unknown parameter provenance stays conservative;
// parameterized owners cannot enter this optional access path.
func potentialTemporalIndexAccess(ctx context.Context, query *plan.Query) bool {
	if query == nil || query.StmtType != plan.Query_SELECT || len(query.Steps) != 1 || len(query.Params) != 0 || len(query.Nodes) > 64 {
		return false
	}
	var nodes []*plan.Node
	seen := make(map[int32]bool)
	pending := append([]int32(nil), query.Steps...)
	for len(pending) != 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id < 0 || int(id) >= len(query.Nodes) || seen[id] || query.Nodes[id] == nil {
			continue
		}
		seen[id] = true
		node := query.Nodes[id]
		nodes = append(nodes, node)
		pending = append(pending, node.Children...)
	}
	indexed := make(map[[2]int32]types.T)
	for id := range seen {
		node := query.Nodes[id]
		if !temporalCandidateSimplePathShape(query, id) || node.NodeType != plan.Node_TABLE_SCAN || node.TableDef == nil || len(node.BindingTags) == 0 {
			continue
		}
		for _, index := range node.TableDef.Indexes {
			if index == nil || !index.TableExist || !catalog.IsRegularIndexAlgo(index.IndexAlgo) {
				continue
			}
			for _, part := range index.Parts {
				pos, ok := node.TableDef.Name2ColIndex[catalog.ResolveAlias(part)]
				if !ok || pos < 0 || int(pos) >= len(node.TableDef.Cols) || node.TableDef.Cols[pos] == nil {
					continue
				}
				oid := types.T(node.TableDef.Cols[pos].Typ.Id)
				if oid == types.T_datetime || oid == types.T_timestamp {
					indexed[[2]int32{node.BindingTags[0], pos}] = oid
				}
			}
		}
	}
	if len(indexed) == 0 {
		return false
	}
	found := false
	for _, node := range nodes {
		for _, list := range [][]*plan.Expr{node.FilterList, node.OnList} {
			for _, root := range list {
				_ = plan.VisitExprTree(root, func(expr *plan.Expr) error {
					fn := expr.GetF()
					if found || fn == nil || fn.Func == nil || len(fn.Args) < 2 {
						return nil
					}
					id, _ := function.DecodeOverloadID(fn.Func.Obj)
					switch id {
					case function.EQUAL, function.LESS_THAN, function.LESS_EQUAL, function.GREAT_THAN, function.GREAT_EQUAL:
						if len(fn.Args) != 2 {
							return nil
						}
						found = temporalPotentialPair(indexed, fn.Args[0], fn.Args[1]) || temporalPotentialPair(indexed, fn.Args[1], fn.Args[0])
					case function.BETWEEN, function.IN_RANGE:
						if len(fn.Args) >= 3 {
							found = temporalPotentialPair(indexed, fn.Args[0], fn.Args[1]) || temporalPotentialPair(indexed, fn.Args[0], fn.Args[2])
						}
					case function.IN:
						if len(fn.Args) == 2 && fn.Args[1] != nil {
							found = temporalPotentialPair(indexed, fn.Args[0], fn.Args[1])
							if list := fn.Args[1].GetList(); list != nil {
								for _, bound := range list.List {
									found = found || temporalPotentialPair(indexed, fn.Args[0], bound)
								}
							}
						}
					default:
						return nil
					}
					return nil
				})
				if found {
					// A prepared rebind can materialize source values. Preserve
					// their existing value-dependent lifetime conservatively.
					if ctx != nil && preparedBindingState(ctx) != nil {
						return true
					}
					return !temporalOriginalOwnerExcludesIndex(ctx, query)
				}
			}
		}
	}
	return false
}

// temporalOriginalOwnerExcludesIndex proves that a bound owner cannot reach
// temporal index admission. Incomplete evidence leaves the lifetime marked.
func temporalOriginalOwnerExcludesIndex(ctx context.Context, query *plan.Query) bool {
	for id, scan := range query.Nodes {
		if scan == nil || scan.NodeType != plan.Node_TABLE_SCAN || !temporalCandidateSimplePathShape(query, int32(id)) {
			continue
		}
		budget := 4096
		if ctx == nil || !temporalOriginalOwnerFits(ctx, query, int32(id), &budget) {
			return false
		}
		atom, ok := temporalOriginalSingleMixedLeaf(query, int32(id))
		if !ok {
			return ctx.Err() == nil
		}
		if scan.TableDef == nil || len(scan.TableDef.Indexes) > engine.MaxTemporalIndexDefinitions {
			return false
		}
		for _, index := range scan.TableDef.Indexes {
			if index == nil || !spendTemporalCostWork(ctx, &budget, 1) {
				return false
			}
			if !catalog.IsRegularIndexAlgo(index.IndexAlgo) {
				continue
			}
			if !index.TableExist || len(index.Parts) == 0 || len(index.Parts[0]) == 0 || len(index.Parts[0]) > engine.MaxTemporalNameBytes || !spendTemporalCostWork(ctx, &budget, len(index.Parts[0])) {
				return false
			}
			pos, exists := scan.TableDef.Name2ColIndex[catalog.ResolveAlias(index.Parts[0])]
			if !exists || pos < 0 || int(pos) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[pos] == nil {
				return false
			}
			if pos == atom.column.GetCol().ColPos {
				return false
			}
		}
		return ctx.Err() == nil
	}
	return false
}

// markTemporalAccessLifetime must run on the bound original owner, before
// diagnostic and access-path decisions. It does not enable index selection.
func (builder *QueryBuilder) markTemporalAccessLifetime(rootID int32) {
	if builder == nil || builder.compCtx == nil || builder.qry == nil {
		return
	}
	// Initial PREPARE and executions with original parameters cannot enter
	// this path, even if rebinding materializes parameter values as literals.
	// A parameter-free prepared rebind may still need a fresh cost decision.
	if builder.isPrepareStatement || builder.hasTemporalPlanningIntent && !builder.temporalPlanningIntent.ParameterFree {
		return
	}
	// The newly bound root is normally not yet appended to Steps.
	owner := *builder.qry
	owner.Steps = append([]int32(nil), builder.qry.Steps...)
	if len(owner.Steps) == 0 || owner.Steps[len(owner.Steps)-1] != rootID {
		owner.Steps = append(owner.Steps, rootID)
	}
	if !potentialTemporalIndexAccess(builder.GetContext(), &owner) {
		return
	}
	builder.qry.TemporalIndexNeedsReplan = true
	if state := preparedBindingState(builder.GetContext()); state != nil {
		state.valueDependent = true
	}
}

func temporalPotentialPair(indexed map[[2]int32]types.T, candidate, bound *plan.Expr) bool {
	if candidate == nil || bound == nil || candidate.GetCol() == nil {
		return false
	}
	col := candidate.GetCol()
	oid, ok := indexed[[2]int32{col.RelPos, col.ColPos}]
	// A parameter nested inside a known same-domain cast cannot change the
	// comparison domain. Keep unknown provenance conservative until it binds.
	boundType := types.T(bound.Typ.Id)
	return ok && (isDatetimeTimestampTypePair(oid, boundType) || boundType == types.T_any && temporalCandidateHasUnprovedSource(bound))
}
